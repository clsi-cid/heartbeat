/******************************************************************************
 * heartbeat.c
 *-----------------------------------------------------------------------------
 * EPICS Heartbeat Extension
 *
 * This code is written to be fast. It makes extensive use of several
 * global buffers for string formatting etc.  Care must be taken to ensure
 * the global buffers are used properly.
 *-----------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <malloc.h>
#include <dirent.h>
#include <assert.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/* EPICS includes */
#include "dbDefs.h"
#include "dbBase.h"
#include "link.h"
#include "dbStaticLib.h"
#include "dbCommon.h"
#include "dbAddr.h"
#include "dbLock.h"
#include "dbAccessDefs.h"
#include "ellLib.h"

#include "heartbeat.h"

/******************************************************************************
 * Defines
 *-----------------------------------------------------------------------------
 */

#define MAX_PVS             10
#define HEART               "Heartbeat"
#define ERR_BUF_OVERFLOW    "ERROR: Buffer Overflow!"

#define TCP_FILE            "/proc/net/tcp"
#define UDP_FILE            "/proc/net/udp"
#define PROC_DIR            "/proc"
#define MAX_BUF             2048

/* Make BUF_SIZE a little smaller than actual buffer in case of error in size checks */
#define BUF_SIZE            MAX_BUF - 128

#define CMD_LIST_PVS        0x4DA3B921
#define CMD_NETSTAT         0x7F201C3B
#define CMD_LOGGING_ENABLE  0xD58E309A
#define CMD_LOGGING_DISABLE 0x73DFE830

#define HEARTBEAT_PORT      5066
#define HEARTBEAT_VERSION   "2.0"
#define RX_TIMEOUT_SEC      2

/******************************************************************************
 * External variables
 *-----------------------------------------------------------------------------
 */

extern unsigned short ca_server_port;
extern ELLLIST beaconAddrList;

/******************************************************************************
 * Thread arguments
 *-----------------------------------------------------------------------------
 */

typedef struct ThreadArgs_s
{
    Intu_t      mode;
    Char_p      arg0_p;
    Char_p      arg1_p;

} ThreadArgs_t, *ThreadArgs_p;

/******************************************************************************
 * RX Message header
 *-----------------------------------------------------------------------------
 */

typedef struct RxMessage_s
{
    Int32u_t    command;
    Int32u_t    heartbeat;
    Int32u_t    offset;
    Int32u_t    handle;
} RxMessage_t, *RxMessage_p;

/******************************************************************************
 * Singly linked list of strings for storing and returning data
 *-----------------------------------------------------------------------------
 */

typedef struct StringList_s
{
    Char_p                  string_p;
    struct StringList_s    *next_p;
} StringList_t, *StringList_p;

/******************************************************************************
 * Global Variables
 *
 * To make this code as fast as possible this extension makes heavy use of
 * global variables. Care must be taken to ensure proper usage.
 *-----------------------------------------------------------------------------
 */

pthread_t       gHeartbeatThread = 0;
ThreadArgs_p    gThreadArgs_p = NIL;
StringList_p    gPvListBase_p = NIL;
StringList_p    gStringListBase_p = NIL;
StringList_p    gStringListCurrent_p = NIL;
Char_t          gBuf1[MAX_BUF];
Char_t          gBuf2[MAX_BUF];
Boolean_t       gLogging = FALSE;
Intu_t          gMallocCount = 0;
Intu_t          gNetstatBuild = 0;

/******************************************************************************
 * Frees memory allocated for singly linked list of strings
 * (PV List and "netstat" results)
 *-----------------------------------------------------------------------------
 */

Void_t free_string_list(StringList_p base_p)
{
    StringList_p    current_p;
    StringList_p    next_p;

    if( base_p == NIL ) return;

    current_p = base_p;

    while( current_p )
    {
        next_p = current_p->next_p;
        if( current_p->string_p )
        {
            free( current_p->string_p );
            gMallocCount--;
        }
        free( current_p );
        gMallocCount--;
        current_p = next_p;
    }
    return;
}

/*-----------------------------------------------------------------------------
 * Build a linked list of PV names
 *-----------------------------------------------------------------------------
 */

Void_t refresh_pv_list()
{
    StringList_p        entry_p = NIL;
    StringList_p        prev_p = NIL;

    DBENTRY             dbentry;
    DBENTRY            *dbentry_p = &dbentry;
    Ints_t              status;
    Intu_t              pv_count = 0;
    Ints_t              byte_count;

    free_string_list( gPvListBase_p );
    gPvListBase_p = NIL;

    if( !pdbbase )
    {
        fprintf(stderr, "%s: No database loaded\n", HEART);
        return;
    }

    dbInitEntry( pdbbase, dbentry_p );
    status = dbFirstRecordType( dbentry_p );

    if( status )
    {
        fprintf(stderr, "%s: No record description\n", HEART);
    }

    while( !status )
    {
        status = dbFirstRecord( dbentry_p );
        while( !status )
        {
            if( gLogging )
            {
                printf("refresh_pv_list(): GOT A PV: %s\n", dbGetRecordName( dbentry_p ));
            }

            entry_p = (StringList_p)calloc( 1, sizeof(StringList_t) );
            gMallocCount++;

            byte_count = snprintf(gBuf2, BUF_SIZE, "%s", dbGetRecordName( dbentry_p ));
            if( byte_count < 0 || byte_count >= BUF_SIZE )
            {
                fprintf(stderr, "%s: %s: PV list\n", HEART, ERR_BUF_OVERFLOW );
                break;
            }
            entry_p->string_p = (char *)malloc( strlen(gBuf2) + 1 );
            gMallocCount++;

            strcpy( entry_p->string_p, gBuf2 );

            /* Point global list pointer to first element in list */
            if( gPvListBase_p == NIL ) gPvListBase_p = entry_p;

            /* Link element to previous element */
            if( prev_p != NIL ) prev_p->next_p = entry_p;

            prev_p = entry_p;

            pv_count++;
            status = dbNextRecord( dbentry_p );
        }
        status = dbNextRecordType( dbentry_p );
    }
    dbFinishEntry( dbentry_p );
    return;
}

/*-----------------------------------------------------------------------------
 * Send a list of PVs to the caller MAX_PVS at a time, starting from offset
 *-----------------------------------------------------------------------------
 */

Boolean_t send_pvs(
    Ints_t                   sock,
    struct sockaddr_storage *addr_storage_p,
    Int32u_t                 offset,
    Int32u_t                 handle
)
{
    struct sockaddr_in *addr_p;
    StringList_p        current_p;
    Ints_t              buf_len;
    Ints_t              temp_len;
    Ints_t              status;
    Ints_t              i;
    Ints_t              have_count = 0;
    Boolean_t           result = FALSE;

    if( gLogging )
    {
        printf("send_pvs called with offset %u handle %u\n", offset, handle);
    }

    /* NOTE: It should be safe if multiple clients are requesting PVs       */
    /* simultaneously, as all the code in this extension is executed by a   */
    /* single thread.  In fact there is probably no need to rebuild the     */
    /* list of PVs each time a new request for PVs at offset 0 is made, as  */
    /* the PVs list is not dynamic. However rebuilding the list every time  */
    /* will support dynamic PV lists                                        */

    if( offset == 0 ) refresh_pv_list();

    /* The sockaddr_storage struct can store IPv4 or IPv6 addresses.  */
    /* cast to desired format */
    addr_p = (struct sockaddr_in *)addr_storage_p;

    /* Format first part of JSON response */
    buf_len = sprintf( gBuf1, "{\"handle\":\"%u\",\"offset\":\"%u\",\"pvs\":[",
        handle, offset );

    /* Loop through linked list of PV names */
    for( current_p = gPvListBase_p, i = 0 ; current_p ; current_p = current_p->next_p, i++ )
    {
        /*
        printf("PV: %d name '%s'\n", i, current_p->name_p);
        printf("i: %d. offset: %u, have_count: %d\n", i, offset, have_count );
        */

        if( i >= offset && have_count < MAX_PVS )
        {
            if( have_count > 0 )
            {
                strcat( gBuf1, "," );
                buf_len++;
            }

            temp_len = snprintf( gBuf2, BUF_SIZE, "\"%s\"", current_p->string_p );

            if( temp_len < 0 || temp_len >= BUF_SIZE )
            {
                fprintf(stderr, "%s: buffer overflow: skipping PV %s\n",
                    HEART, current_p->string_p);
                continue;
            }

            if( temp_len + buf_len >= BUF_SIZE )
            {
                fprintf(stderr, "%s: buffer overflow: skipping PV %s\n",
                    HEART, current_p->string_p);
                continue;
            }
            strcat(gBuf1, gBuf2);

            buf_len += temp_len;
            have_count++;
        }
    }

    temp_len = snprintf( gBuf2, BUF_SIZE, "],\"total\":\"%u\"}", i );

    if( ( temp_len + buf_len ) >= BUF_SIZE )
    {
        fprintf(stderr, "%s: buffer overflow terminating pv response\n", HEART);
        goto exit;
    }

    strcat( gBuf1, gBuf2 );
    buf_len = strlen( gBuf1 );

    if( gLogging )
    {
        printf("send_pvs(): TX Buffer: %s\n", gBuf1);
        /*
        printf("send_pvs(): TX Port: %d\n", addr_p->sin_port);
        printf("send_pvs(): TX IP: 0x%X\n", addr_p->sin_addr.s_addr);
        */
    }

    status = sendto( sock, gBuf1, buf_len, 0, (struct sockaddr *)addr_p,
        sizeof(struct sockaddr_in));

    if( status < 0 )
    {
        fprintf( stderr, "%s: send_pvs() failed\n", HEART);
    }
    else
    {
        result = TRUE;
    }

exit:
    return result;
}

/******************************************************************************
 * Add line to the singly linked list of lines
 *-----------------------------------------------------------------------------
 */

void add_line(Char_p start_p)
{
    StringList_p    entry_p;

    entry_p = (StringList_p)calloc( 1, sizeof(StringList_t) );

    if( entry_p == NIL )
    {
        fprintf( stderr, "ERROR: %s: add_line(): calloc failed\n", HEART);
        return;
    }

    entry_p->string_p = (Char_p)malloc( strlen(start_p) + 1 );

    if( entry_p->string_p == NIL )
    {
        free( entry_p );
        fprintf( stderr, "ERROR: %s: add_line(): malloc failed\n", HEART);
        return;
    }

    gMallocCount += 2;

    strcpy( entry_p->string_p, start_p );

    /* Point global list pointer to first element in list */
    if( gStringListBase_p == 0 ) gStringListBase_p = entry_p;

    /* Link element to previous element */
    if( gStringListCurrent_p ) gStringListCurrent_p->next_p = entry_p;

    gStringListCurrent_p = entry_p;

    return;
}

/******************************************************************************
 * Return the line specified by the index
 *-----------------------------------------------------------------------------
 */

Char_p get_line( Ints_t index )
{
    StringList_p    entry_p;
    Ints_t          i;

    /* printf( "Want to get line index: %d\n", index); */

    if( gStringListBase_p == NIL ) return NIL;

    entry_p = gStringListBase_p;

    /* Loop until desired entry found.  This is a little "brute force but */
    /* should be relatively fast */

    for( i = 0 ; i < index ; i++ )
    {
        if( entry_p == NIL )
        {
            break;
        }

        if( entry_p->next_p == NIL )
        {
            entry_p = NIL;
            break;
        }
        entry_p = entry_p->next_p;
    }

    if( entry_p == NIL ) return NIL;

    return entry_p->string_p;
}

/******************************************************************************
 * Check to see if this is a "pid" directory.
 *
 * Returns TRUE if there are only digits in the string name_p; FALSE otherwise
 *-----------------------------------------------------------------------------
 */

Ints_t is_pid_dir( Char_p name_p )
{
    Char_t      c;
    Ints_t      i;

    if( name_p == NIL ) return FALSE;

    for( i = 0 ; ; i++ )
    {
        c = *( name_p + i );
        if( c == 0 ) break;

        if( c < '0' || c > '9' ) return FALSE;
    }
    return TRUE;
}

/******************************************************************************
 * Read the CWD file for the specified pid
 *-----------------------------------------------------------------------------
 */

Void_t read_pid_cwd( Char_p proc_dir_p, Char_p pid_p )
{
    Ints_t  bytes;

    bytes = snprintf( gBuf2, BUF_SIZE, "%s/%s/cwd", proc_dir_p, pid_p );
    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        add_line( ERR_BUF_OVERFLOW );
        fprintf( stderr, "%s: %s: read_pid_cwd()\n", HEART, ERR_BUF_OVERFLOW );
        return;
    }

    /* printf( "want to read CWD: %s\n", gBuf2 ); */

    /* Read value of symlink into gBuf1 */
    bytes = readlink( gBuf2, gBuf1, BUF_SIZE );

    if( bytes < 0 )
    {
        fprintf( stderr, "%s: readlink failed for: %s\n", HEART, gBuf2 );
        return;
    }

    if( bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: readlink error for: %s\n", HEART, gBuf2 );
        fprintf( stderr, "%s: %s: pid readlink: %d\n", HEART, ERR_BUF_OVERFLOW, bytes );
        add_line( ERR_BUF_OVERFLOW );
        return;
    }

    /* readlink DOES NOT null terminate result!! */
    gBuf1[bytes] = 0;

    /* printf( "THIS IS THE CWD: %s\n", gBuf1 ); */

    if( strlen( gBuf1 ) <= 1 ) return;

    bytes = snprintf( gBuf2, BUF_SIZE, "pid: %s cwd: %s", pid_p, gBuf1 );
    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: cwd\n", HEART, ERR_BUF_OVERFLOW );
        add_line( ERR_BUF_OVERFLOW );
        return;
    }

    add_line( gBuf2 );
    return;
}

/******************************************************************************
 *
 *-----------------------------------------------------------------------------
 */

Void_t read_pid_cmdline( Char_p proc_dir_p, Char_p pid_p )
{
    FILE           *fp = NIL;
    Ints_t          bytes;
    Ints_t          i, j;

    bytes = snprintf( gBuf1, BUF_SIZE, "%s/%s/cmdline", proc_dir_p, pid_p );
    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: cmdline\n", HEART, ERR_BUF_OVERFLOW );
        add_line( ERR_BUF_OVERFLOW );
        return;
    }

    /* printf("Want to read CMDLINE: %s\n", gBuf1 ); */
    if( ( fp = fopen( gBuf1, "rb" ) ) == NIL )
    {
        bytes = snprintf( gBuf2, BUF_SIZE, "ERROR: failed to open: %s", gBuf1 );
        if( bytes < 0 || bytes >= BUF_SIZE )
        {
            fprintf( stderr, "%s: %s: fopen\n", HEART, ERR_BUF_OVERFLOW );
            add_line( ERR_BUF_OVERFLOW );
        }
        else
        {
            add_line( gBuf2 );
        }
        fprintf( stderr, "%s: Failed to open file: %s\n", HEART, gBuf1 );
        return;
    }

    /* Just do one read... if it fills buffer then we are in trouble! */
    bytes = fread( gBuf2, 1, BUF_SIZE, fp );

    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        bytes = snprintf( gBuf1, BUF_SIZE, "ERROR: cannot read: %s/%s/cmdline", proc_dir_p, pid_p );
        if( bytes < 0 || bytes >= BUF_SIZE )
        {
            fprintf( stderr, "%s: %s: read cmdline\n", HEART, ERR_BUF_OVERFLOW );
            add_line( ERR_BUF_OVERFLOW );
        }
        else
        {
            add_line( gBuf1 );
        }
        goto exit;
    }

    for( i = 0, j = 0 ; i < bytes ; i++ )
    {
        /* Check for potential buffer overflow due to injected escape char */
        if( j >= ( BUF_SIZE - 3 ) )
        {
            fprintf( stderr, "%s: %s: format 1\n", HEART, ERR_BUF_OVERFLOW );
            add_line( ERR_BUF_OVERFLOW );
            goto exit;
        }

        if( gBuf2[i] == 0 )
        {
            /* The cmdline file seems to be putting zeros in for spaces */
            gBuf1[j++] = ' ';
        }
        else if( gBuf2[i] < ' ')
        {
            gBuf1[j++] = '.';
        }
        else if( gBuf2[i] > '~')
        {
            gBuf1[j++] = '.';
        }
        else if( gBuf2[i] == '\"' )
        {
            gBuf1[j++] = '\\';
            gBuf1[j++] = '\"';
        }
        else if( gBuf2[i] == '\\' )
        {
            gBuf1[j++] = '\\';
            gBuf1[j++] = '\\';
        }
        else
        {
            gBuf1[j++] = gBuf2[i];
        }
        /* printf( "%c %d\n", gBuf2[i], (Int8u_t)gBuf2[i] ); */
    }
    gBuf1[j] = 0;

    /* printf("got CMDLINE: '%s'\n", gBuf1); */

    if( strlen( gBuf1 ) == 0 )
    {
        /* printf( "Skipping 0 length command line\n"); */
        goto exit;
    }

    bytes = snprintf( gBuf2, BUF_SIZE, "pid: %s cmdline: %s", pid_p, gBuf1 );
    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: format 2\n", HEART, ERR_BUF_OVERFLOW );
        add_line( ERR_BUF_OVERFLOW );
        goto exit;
    }
    add_line( gBuf2 );

exit:
    if( fp ) fclose( fp );
    return;
}

/******************************************************************************
 *  Get details for the specified PID
 *-----------------------------------------------------------------------------
 */

Void_t read_pid( Char_p proc_dir_p, Char_p pid_p )
{
    Ints_t           bytes;
    DIR             *dir_p;
    struct dirent   *dir_entry_p;

    /* printf("Want to read pid: %s\n", pid_p); */

    bytes = snprintf( gBuf1, BUF_SIZE, "%s/%s/fd", proc_dir_p, pid_p );
    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: pid file\n", HEART, ERR_BUF_OVERFLOW );
        add_line( ERR_BUF_OVERFLOW );
        return;
    }

    dir_p = opendir( gBuf1 );

    if( !dir_p )
    {
        bytes = snprintf( gBuf1, BUF_SIZE, "ERROR: Failed to open: %s/%s/fd", proc_dir_p, pid_p );
        if( bytes < 0 || bytes >= BUF_SIZE )
        {
            fprintf( stderr, "%s: %s: pid opendir\n", HEART, ERR_BUF_OVERFLOW );
            add_line( ERR_BUF_OVERFLOW );
        }
        else
        {
            add_line( gBuf1 );
        }
        return;
    }

    while( ( dir_entry_p = readdir( dir_p ) ) != NULL )
    {
        /* printf("Got : %s\n", dir_entry_p->d_name); */
        if( dir_entry_p->d_type != DT_LNK ) continue;

        bytes = snprintf( gBuf2, BUF_SIZE, "%s/%s/fd/%s", proc_dir_p, pid_p, dir_entry_p->d_name );

        if( bytes < 0 || bytes >= BUF_SIZE )
        {
            add_line( ERR_BUF_OVERFLOW );
            fprintf( stderr, "%s: %s: fd\n", HEART, ERR_BUF_OVERFLOW );
            continue;
        }
        /* printf("GOT A SYMLINK: '%s'\n", gBuf2 ); */

        /* Read value of symlink into gBuf1 */
        bytes = readlink( gBuf2, gBuf1, BUF_SIZE );

        if( bytes < 0 )
        {
            fprintf( stderr, "%s: readlink failed for: %s\n", HEART, gBuf2 );
            return;
        }

        if( bytes >= BUF_SIZE )
        {
            add_line( ERR_BUF_OVERFLOW  );
            fprintf( stderr, "%s: %s: readlink\n", HEART, ERR_BUF_OVERFLOW );
            continue;
        }

        /* readlink DOES NOT null terminate result!! */
        gBuf1[bytes] = 0;

        /* printf("written: %ld\n", written ); */
        if( strstr( gBuf1, "socket" ) == NULL )
        {
            /* printf( "SKIPPING non socket fd %s\n", gBuf1); */
            continue;
        }
        /* printf("GOT SOCKET: %s\n", gBuf1); */

        /* Write result into a buffer preceeded by the PID */
        bytes = snprintf( gBuf2, BUF_SIZE, "pid: %s %s", pid_p, gBuf1 );

        if( bytes < 0 || bytes >= BUF_SIZE )
        {
            add_line( ERR_BUF_OVERFLOW );
            fprintf( stderr, "%s: %s: pid\n", HEART, ERR_BUF_OVERFLOW );
            continue;
        }

        /* printf("GOT SOCKET: %s\n", gBuf2); */

        /* Add this to the set of data to return */
        add_line( gBuf2 );
    }
    closedir( dir_p );

    /* Read the cmdline and cwd for this pid */
    read_pid_cmdline( proc_dir_p, pid_p );
    read_pid_cwd( proc_dir_p, pid_p );

    return;
}

/******************************************************************************
 * Scan the specified directory (/proc) for directories that look like PID
 * directories.
 *-----------------------------------------------------------------------------
 */

Void_t scan_pid_dir( Char_p dirname_p )
{
    DIR             *dir_p;
    struct dirent   *dir_entry_p;

    dir_p = opendir( dirname_p );

    if( dir_p )
    {
        while( ( dir_entry_p = readdir( dir_p ) ) != NULL )
        {
            if( is_pid_dir( dir_entry_p->d_name ) )
            {
                read_pid( dirname_p, dir_entry_p->d_name );
            }
        }
        closedir( dir_p );
    }
    return;
}

/******************************************************************************
 * Read the passed in file and add lines to the singly linked list
 *-----------------------------------------------------------------------------
 */

Ints_t read_net_file( Char_p filename_p, Char_p type_p )
{
    FILE           *fp = NIL;
    size_t          bytes;
    size_t          index;
    Intu_t          size = 0;
    Intu_t          line_length;
    Char_p          next_char_p;
    size_t          rx_buf_size = BUF_SIZE;
    Ints_t          i;
    Intu_t          space_count = 0;

    if( ( fp = fopen( filename_p, "rb" ) ) == NIL )
    {
        snprintf( gBuf1, (size_t)BUF_SIZE, "Failed to open file: %s", filename_p );
        add_line( gBuf1 );
        goto exit;
    }

    /* Put the received characters into global gBuf2 */
    index = snprintf(gBuf2, (size_t)BUF_SIZE, "%s: ", type_p);
    next_char_p = &gBuf2[index];

    for( ; ; )
    {
        bytes = fread( gBuf1, 1, rx_buf_size, fp );

        if( bytes < 0 || bytes > rx_buf_size )
        {
            snprintf( gBuf1, (size_t)BUF_SIZE, "Failed to read file: %s", filename_p );
            add_line( gBuf1 );
            goto exit;
        }

        size += bytes;

        /* printf( "Read %d bytes\n", size ); */

        /* Loop through the received bytes looking for newlines and linefeeds */
        for( i = 0 ; i < bytes ; i++ )
        {
            /* printf( "%c", *(buf1_p + i)); */

            if( gBuf1[i] == '\n' || gBuf1[i] == '\r' )
            {
                /* printf( "Found end of line\n" ); */
                *next_char_p = 0;
                add_line( gBuf2 );
                index = snprintf( gBuf2, (size_t)BUF_SIZE, "%s: ", type_p );
                next_char_p = &gBuf2[index];
                space_count = 0;
                continue;
            }

            if( gBuf1[i] == ' ' )
            {
                space_count += 1;
            }
            else
            {
                space_count = 0;
            }

            /* get rid of repeating spaces to keep response size to a minimum */
            if( space_count > 1 ) continue;

            /* Add the found character to the line buffer (IF THERE IS ROOM!!) */
            line_length = (Intu_t)(next_char_p - gBuf2);

            if( line_length < ( BUF_SIZE - 1 ) )
            {
                /* printf( "This is the length: %d\n", line_length); */
                *next_char_p++ = gBuf1[i];
            }
            else
            {
                fprintf( stderr, "ERROR: %s: Buffer overflow\n", HEART);
            }
        }

        if( bytes < rx_buf_size )
        {
            /* printf("Must be done, got less than a full buffer\n"); */
            break;
        }
    }

exit:

    if( fp != NIL ) fclose( fp );
    return 0;
}

/******************************************************************************
 * Read the passed in file and add lines to the singly liked list
 *-----------------------------------------------------------------------------
 */

Boolean_t send_netstat(
    Ints_t                   sock,
    struct sockaddr_storage *addr_storage_p,
    Int32u_t                 offset,
    Int32u_t                 handle
)
{
    struct sockaddr_in      *addr_p;
    Ints_t                   bytes;
    Ints_t                   status;
    Char_p                   line_p;
    Boolean_t                result = FALSE;

    /* printf("send_netstat called with offset %u handle %u\n", offset, handle); */

    if( offset == 0 )
    {
        /* Rebuild the "netstat" data whenever offset zero is requested     */
        /* This should be safe even if multiple clients are requesting this */
        /* data, as this code is executed in a single thread.  HOWEVER, it  */
        /* is possible that if the list is rebuilt while another client is  */
        /* halfway through returning the results, the other client could    */
        /* miss lines or get duplicate lines etc.  Do not want to start     */
        /* keeping multiple lists for multiple clients as this would be     */
        /* complicated and error prone.                                     */
        free_string_list( gStringListBase_p );
        gStringListCurrent_p = gStringListBase_p = NIL;

        gNetstatBuild++;

        /* By adding a bogus line at the start of the result, the client    */
        /* can detect that the result has been rebuilt on the fly and can   */
        /* redownload the result starting at offset 1 if it so desires      */
        /* (i.e., without rebuilding)                                       */
        snprintf( gBuf1, BUF_SIZE, "build: %u", gNetstatBuild );
        add_line( gBuf1 );

        read_net_file( TCP_FILE, "tcp" );
        read_net_file( UDP_FILE, "udp" );
        scan_pid_dir( PROC_DIR );
    }

    line_p = get_line( offset );

    if( line_p == NIL ) line_p = "__NONE__";

#if 1
    /* Including "build" count allows client to detect a rebuild */
    /* Keep the keys really short just to minimize message size */
    bytes = snprintf( gBuf1, BUF_SIZE, "{\"h\":\"%u\",\"o\":\"%u\",\"b\":\"%u\",\"l\":\"%s\"}",
        handle, offset, gNetstatBuild, line_p );

    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: send_netstat\n", HEART, ERR_BUF_OVERFLOW );
        bytes = snprintf( gBuf1, BUF_SIZE, "{\"h\":\"%u\",\"o\":\"%u\",\"b\":\"%u\",\"ERROR\":\"%s\"}",
            handle, offset, gNetstatBuild, ERR_BUF_OVERFLOW );
    }
#else
    /* Including "build" count allows client to detect a rebuild */
    /* Keep the keys really short just to minimize message size */
    bytes = snprintf( gBuf1, BUF_SIZE, "{\"handle\":\"%u\",\"offset\":\"%u\",\"build\":\"%u\",\"line\":\"%s\"}",
        handle, offset, gNetstatBuild, line_p );

    if( bytes < 0 || bytes >= BUF_SIZE )
    {
        fprintf( stderr, "%s: %s: send_netstat\n", HEART, ERR_BUF_OVERFLOW );
        bytes = snprintf( gBuf1, BUF_SIZE, "{\"handle\":\"%u\",\"offset\":\"%u\",\"build\":\"%u\",\"ERROR\":\"%s\"}",
            handle, offset, gNetstatBuild, ERR_BUF_OVERFLOW );
    }
#endif

    addr_p = (struct sockaddr_in *)addr_storage_p;

    if( gLogging )
    {
        printf( "%s: self_nsetstat(): '%s'\n", HEART, gBuf1 );
    }

    status = sendto( sock, gBuf1, bytes, 0, (struct sockaddr *)addr_p,
        sizeof(struct sockaddr_in) );

    if( status < 0 )
    {
        fprintf( stderr, "%s: send_netstat() sendto failed\n", HEART );
    }
    else
    {
        result = TRUE;
    }

    return result;
}

/******************************************************************************
 * Looks at received command. It must have:
 *
 * - the expected number of bytes
 * - a valid command
 * - a heartbeat value within 1 of the current heartbeat
 *-----------------------------------------------------------------------------
 */

int process_rx_command(
    Ints_t                   sock,
    struct sockaddr_storage *addr_storage_p,
    Ints_t                   rx_count,
    Char_p                   buf_p,
    Int32u_t                 heartbeat
)
{
    RxMessage_p             message_p;
    Int32u_t                handle;
    Int32u_t                offset;
    Int32u_t                command;
    Boolean_t               result = FALSE;

    /* Check the received number of bytes against expected */
    if( rx_count < 0 ) goto exit;

    if( rx_count != (int)sizeof(RxMessage_t))
    {
        fprintf( stderr, "%s: Expect %d bytes, got %d\n",
            HEART, (int)sizeof(RxMessage_t), rx_count );
        goto exit;
    }

    /* The message is in the passed in buffer */
    message_p = (RxMessage_p)buf_p;

    if( heartbeat - ntohl( message_p->heartbeat ) > 1 )
    {
        fprintf( stderr, "%s: Invalid count %u (expect %u)\n", HEART,
            ntohl( message_p->heartbeat ), heartbeat );
         goto exit;
    }

    handle  = ntohl( message_p->handle );
    offset  = ntohl( message_p->offset );
    command = ntohl( message_p->command );

    if( command == CMD_LIST_PVS )
    {
        result = send_pvs( sock, addr_storage_p, offset, handle );
    }
    else if( command == CMD_NETSTAT )
    {
        result = send_netstat( sock, addr_storage_p, offset, handle );
    }
    else if( command == CMD_LOGGING_ENABLE )
    {
        gLogging = TRUE;
    }
    else if( command == CMD_LOGGING_DISABLE )
    {
        gLogging = FALSE;
    }
    else
    {
        fprintf( stderr, "%s: Invalid command: %u\n", HEART, command );
        goto exit;
    }

exit:

    /* Throttle this thread if experiencing problems */
    if( result != TRUE ) sleep( 0.1 );

    return result;
}


/******************************************************************************
 * threadEntry()
 *
 * Main loop of this extension
 *-----------------------------------------------------------------------------
 */
void *threadEntry( void *args_p )
{
    ThreadArgs_p            threadArgs_p;
    struct sockaddr_in      sock_in;
    struct timeval          tv;
    struct sockaddr_storage rx_addr;
    socklen_t               rx_addr_len = sizeof(rx_addr);
    Char_p                  cwd_p;
    Ints_t                  sock;
    Ints_t                  status;
    Ints_t                  yes = 1;
    Ints_t                  bytes;
    Intu_t                  starttime;
    Intu_t                  curtime;
    Intu_t                  uptime;
    Intu_t                  pid;
    Intu_t                  beacon_interval = 30;
    Intu_t                  next_beacon_time;
    Int32u_t                counter = 0;

    /* Give app time to initialize before entering main loop */
    sleep( 5 );

    memset( &sock_in, 0, sizeof(struct sockaddr_in) );

    threadArgs_p = (ThreadArgs_p)args_p;

    sock = socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );

    sock_in.sin_addr.s_addr = htonl( INADDR_ANY );
    sock_in.sin_port = htons( 0 );
    sock_in.sin_family = PF_INET;

    status = bind( sock, (struct sockaddr *)&sock_in, sizeof(struct sockaddr_in) );
    if( status < 0 ) fprintf( stderr, "%s: bind() error: %d\n", HEART, status );

    status = setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(int) );
    if( status < 0 ) fprintf( stderr, "%s: setsockopt SO_BROADCAST failed\n", HEART );

    tv.tv_sec = RX_TIMEOUT_SEC;
    tv.tv_usec = 0;

    status = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if( status < 0 ) fprintf( stderr, "%s: setsockopt SO_RCVTIMEO failed\n", HEART );

    getcwd( gBuf1, BUF_SIZE );
    cwd_p = (Char_p)malloc( strlen( gBuf1 ) + 1 );
    strcpy( cwd_p, gBuf1 );

    pid = (Intu_t)getpid();
    starttime = (Intu_t)time( 0 );

    next_beacon_time = starttime + beacon_interval;

    /* Use just a single socket and thread to send heartbeats */
    /* and handle incoming requests */
    for( ; ; )
    {
        /* Look for received commands */
        bytes = recvfrom( sock, gBuf1, BUF_SIZE, 0, (struct sockaddr *)&rx_addr, &rx_addr_len);

        /* Process any received commands */
        process_rx_command( sock, &rx_addr, bytes, gBuf1, counter );

        curtime = (Intu_t)time(0);

        /* Send the next heartbeat if needed.  Due to the timeout */
        /* in the recvfrom() call above, we could be up to 'timeout' late */
        if( curtime > next_beacon_time )
        {
            counter++;
            next_beacon_time += beacon_interval;
            uptime = curtime - starttime;

            bytes = snprintf( gBuf1, (size_t)BUF_SIZE,
                "{\"seq\":\"%u\",\"sp\":\"%u\",\"up\":\"%u\",\"cur\":\"%u\",\"pid\":\"%u\",\"cwd\":\"%s\",\"ver\":\"%s\",\"img\":\"%s\",\"stcmd\":\"%s\"}",
                counter, ca_server_port, (Intu_t)uptime, (Intu_t)curtime, (Intu_t)pid, cwd_p, HEARTBEAT_VERSION,
                threadArgs_p->arg0_p == (Char_p)0 ? "Unknown" : threadArgs_p->arg0_p,
                threadArgs_p->arg1_p == (Char_p)0 ? "Unknown" : threadArgs_p->arg1_p
            );

            if( bytes < 0 || bytes >= BUF_SIZE )
            {
                fprintf( stderr, "%s: Buffer overflow composing heartbeat\n", HEART );
                continue;
            }

            /* printf("ca_server_port: %u\n", ca_server_port); */

            sock_in.sin_addr.s_addr = htonl( -1 ); /* send message to 255.255.255.255 */
            sock_in.sin_port = htons( HEARTBEAT_PORT ); /* port number */

            if( gLogging )
            {
                printf( "%s: %s (gMallocCount: %u)\n", HEART, gBuf1, gMallocCount );
            }

            status = sendto( sock, gBuf1, bytes, 0,
                (struct sockaddr *)&sock_in, sizeof(struct sockaddr_in));

            if( status < 0 ) fprintf( stderr, "%s: sendto() failed\n", HEART );
        }
#if 0
        else
        {
            printf("curtime: %u next_time: %u\n", curtime, next_beacon_time);
        }
#endif

    }
    return 0;
}

/******************************************************************************
 * heartbeat()
 *
 * Entry point of this extension
 *-----------------------------------------------------------------------------
 */
void heartbeat( int argc, char *argv[] )
{
    ThreadArgs_p threadArgs_p;

    printf( "sizeof( Int8u_t )    %u\n", (Intu_t)sizeof( Int8u_t ) );
    printf( "sizeof( Int16u_t )   %u\n", (Intu_t)sizeof( Int16u_t ) );
    printf( "sizeof( Int32u_t )   %u\n", (Intu_t)sizeof( Int32u_t ) );
    printf( "sizeof( Int64u_t )   %u\n", (Intu_t)sizeof( Int64u_t ) );
    printf( "sizeof( Intu_t )     %u\n", (Intu_t)sizeof( Intu_t ) );

    assert( sizeof( Int8u_t  ) == 1 );
    assert( sizeof( Int16u_t ) == 2 );
    assert( sizeof( Int32u_t ) == 4 );
    assert( sizeof( Int32s_t ) == 4 );
    assert( sizeof( Int64u_t ) == 8 );

    threadArgs_p = (ThreadArgs_p)malloc( sizeof(ThreadArgs_t) );

    if( argc > 0 )
    {
        threadArgs_p->arg0_p = (Char_p)malloc( strlen(argv[0]) + 1 );
        strcpy( threadArgs_p->arg0_p, argv[0] );
    }
    else
    {
        threadArgs_p->arg0_p = NIL;
    }

    if( argc > 1 )
    {
        threadArgs_p->arg1_p = (Char_p)malloc(strlen(argv[1]) + 1);
        strcpy(threadArgs_p->arg1_p, argv[1]);
    }
    else
    {
        threadArgs_p->arg1_p = NIL;
    }

    {
        Ints_t i;
        for( i = 0 ; i < argc ; i++ )
        {
            printf( "%s: argument: %d val: '%s'\n", HEART, i, argv[i] );
        }
    }

    threadArgs_p->mode = 0;
    pthread_create( &gHeartbeatThread, 0, (void *)threadEntry, (void *)threadArgs_p );

    return;
}
