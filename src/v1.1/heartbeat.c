/*-----------------------------------------------------------------------------
heartbeat.c
-------------------------------------------------------------------------------
EPICS Heartbeat Extension
-----------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <malloc.h>

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

#define HEART               "Heartbeat"
#define BUF_LEN             1024
#define CMD_LIST_PVS        0x4DA3B921
#define HEARTBEAT_PORT      5066
#define HEARTBEAT_VERSION   "1.1"
#define RX_TIMEOUT_SEC      2
#ifndef FALSE
#define FALSE               0
#endif

#ifndef TRUE
#define TRUE                1
#endif

extern unsigned short ca_server_port;
extern ELLLIST beaconAddrList;

/*---------------------------------------------------------------------------*/
typedef struct ThreadArgs_s
{
    unsigned int mode;
    char *arg0_p;
    char *arg1_p;

} ThreadArgs_t, *ThreadArgs_p;

typedef struct RxMessage_s
{
    unsigned command;
    unsigned heartbeat;
    unsigned offset;
    unsigned handle;
} RxMessage_t, *RxMessage_p;


typedef struct PvList_s
{
    char             *name_p;
    struct PvList_s  *next_p;
} PvList_t, *PvList_p;

/*-----------------------------------------------------------------------------
Global Variables
-----------------------------------------------------------------------------*/
pthread_t       gHeartbeatThread = 0;
ThreadArgs_p    gThreadArgs_p = 0;
PvList_p        gPvList_p = 0;
int             gMallocCount = 0;

/*-----------------------------------------------------------------------------
Frees memory allocated for the singly linked list of PV names
-----------------------------------------------------------------------------*/
void free_pv_list()
{
    PvList_p    current_p;
    PvList_p    next_p;

    current_p = gPvList_p;

    while( current_p )
    {
        next_p = current_p->next_p;
        if(current_p->name_p)
        {
            gMallocCount--;
            free(current_p->name_p);
        }
        free(current_p);
        gMallocCount--;
        current_p = next_p;
    }

    if( gMallocCount != 0 )
    {
        fprintf(stderr, "%s: non-zero malloc count: %d\n", HEART, gMallocCount);
    }
    gPvList_p = 0;

    return;
}

/*-----------------------------------------------------------------------------
Builds a linked list of PV names
-----------------------------------------------------------------------------*/

void refresh_pv_list()
{
    PvList_p            entry_p = 0;
    PvList_p            prev_p = 0;
    DBENTRY             dbentry;
    DBENTRY            *dbentry_p = &dbentry;
    int                 status;
    int                 pv_count = 0;
    int                 byte_count;
    char                buf2[2 * BUF_LEN];

    free_pv_list();

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
            /* printf("GOT A PV: %s\n", dbGetRecordName( dbentry_p )); */

            entry_p = (PvList_p)calloc( 1, sizeof(PvList_t) );
            gMallocCount++;

            byte_count = snprintf(buf2, BUF_LEN, "%s", dbGetRecordName( dbentry_p ));
            if( byte_count >= BUF_LEN )
            {
                fprintf(stderr, "%s: buffer overflow generating PV list\n", HEART);
                break;
            }
            entry_p->name_p = (char *)malloc( strlen(buf2) + 1 );
            gMallocCount++;

            strcpy( entry_p->name_p, buf2 );

            /* Point global list pointer to first element in list */
            if( gPvList_p == 0 ) gPvList_p = entry_p;

            /* Link element to previous element */
            if( prev_p ) prev_p->next_p = entry_p;

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
Sends a list of PVs to the caller 10 at a time starting from offset
-----------------------------------------------------------------------------*/

void send_pvs(
    int                      sock,
    struct sockaddr_storage *addr_storage_p,
    char                    *buf_p,
    unsigned                 offset,
    unsigned                 handle
)
{
    struct sockaddr_in *addr_p;
    PvList_p            current_p;
    int                 buf_len;
    int                 temp_len;
    int                 status;
    int                 addr_len;
    int                 i;
    int                 have_count = 0;
    char                buf2[2 * BUF_LEN];

    /* printf("send_pvs called with offset %u handle %u\n", offset, handle); */

    if( offset == 0 ) refresh_pv_list();

    /* The sockaddr_storage struct can store IPv4 or IPv6 addresses.  */
    /* cast to desired format */
    addr_p = (struct sockaddr_in *)addr_storage_p;

    addr_len = sizeof(struct sockaddr_in);

    /* Format first part of JSON response */
    buf_len = sprintf(buf_p, "{\"handle\":\"%u\",\"offset\":\"%u\",\"pvs\":[",
        handle, offset);

    /* Loop through linked list of PV names */
    for( current_p = gPvList_p, i = 0 ; current_p ; current_p = current_p->next_p, i++ )
    {
        /* printf("PV: %d name '%s'\n", i, current_p->name_p); */
        /* printf("i: %d. offset: %u, have_count: %d\n", i, offset, have_count ); */
        if( i >= offset && have_count < 10 )
        {
            if( have_count > 0 )
            {
                strcat(buf_p, ",");
                buf_len++;
            }

            temp_len = snprintf(buf2, BUF_LEN, "\"%s\"", current_p->name_p);

            if( temp_len + buf_len > BUF_LEN )
            {
                fprintf(stderr, "%s: buffer overflow: skipping PV %s\n",
                    HEART, current_p->name_p);
                continue;
            }
            strcat(buf_p, buf2);

            buf_len += temp_len;
            have_count++;
        }
    }

    temp_len = snprintf(buf2, BUF_LEN, "],\"total\":\"%u\"}", i);
    if( temp_len >= BUF_LEN || (temp_len + buf_len) > BUF_LEN )
    {
        fprintf(stderr, "%s: buffer overflow terminating pv response\n", HEART);
    }
    strcat(buf_p, buf2);
    buf_len = strlen(buf_p);

    /*
    printf("This is the buffer to send: %s\n", buf_p);
    printf("send to PORT: %d\n", addr_p->sin_port);
    printf("send to IP: 0x%X\n", addr_p->sin_addr.s_addr);
    */


    status = sendto(sock, buf_p, buf_len, 0, (struct sockaddr *)addr_p, addr_len);

    if( status < 0 ) fprintf( stderr, "%s: send_pvs() failed\n", HEART);

    return;
}

/*-----------------------------------------------------------------------------
Looks at received command. It must have:
- the expected number of bytes
- a valid command
- a heartbeat value within 1 of the current heartbeat
-----------------------------------------------------------------------------*/
int process_rx_command(
    int                      sock,
    struct sockaddr_storage *addr_storage_p,
    int                      rx_count,
    char                    *buf_p,
    unsigned                 heartbeat
)
{
    RxMessage_p             message_p;
    int                     result = FALSE;
    unsigned                handle;
    unsigned                offset;
    unsigned                command;

    /* Check the received number of bytes against expected */
    if( rx_count < 0 ) goto exit;

    if( rx_count != (int)sizeof(RxMessage_t))
    {
        fprintf( stderr, "%s: Expect %d bytes, got %d\n",
            HEART, (int)sizeof(RxMessage_t), rx_count);
        goto exit;
    }

    /* The message is in the passed in buffer */
    message_p = (RxMessage_p)buf_p;

    if( heartbeat - ntohl(message_p->heartbeat) > 1 )
    {
        fprintf(stderr, "%s: Invalid count %u (expect %u)\n", HEART,
            ntohl(message_p->heartbeat), heartbeat);
         goto exit;
    }

    handle  = ntohl(message_p->handle);
    offset  = ntohl(message_p->offset);
    command = ntohl(message_p->command);

    if( command == CMD_LIST_PVS )
    {
        send_pvs( sock, addr_storage_p, buf_p, offset, handle);
    }
    else
    {
        fprintf(stderr, "%s: Invalid command: %u\n", HEART, command);
        goto exit;
    }

    result = TRUE;
exit:
    return result;
}

/*---------------------------------------------------------------------------*/
void *threadEntry( void *args_p )
{
    ThreadArgs_p            threadArgs_p;
    int                     sock;
    int                     status;
    int                     yes = 1;
    int                     byte_count;
    unsigned                counter = 0;
    unsigned long           starttime;
    unsigned long           curtime;
    unsigned long           uptime;
    unsigned long           pid;
    unsigned long           beacon_interval = 30;
    unsigned long           next_beacon_time;
    struct sockaddr_in      sock_in;
    struct sockaddr_storage rx_addr;
    socklen_t               rx_addr_len = sizeof(rx_addr);
    struct timeval          tv;
    char                   *buf_p;
    char                   *cwd_p;

    /* Allocate buffers.  Make bigger than MAX size fore safety */
    buf_p = (char *)malloc((size_t)(2 * BUF_LEN));

    cwd_p = (char *)malloc((size_t)(2 * BUF_LEN));

    memset(&sock_in, 0, sizeof(struct sockaddr_in));

    threadArgs_p = (ThreadArgs_p)args_p;

    sock = socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );

    sock_in.sin_addr.s_addr = htonl(INADDR_ANY);
    sock_in.sin_port = htons(0);
    sock_in.sin_family = PF_INET;

    status = bind(sock, (struct sockaddr *)&sock_in, sizeof(struct sockaddr_in));
    if( status < 0 ) fprintf(stderr, "%s: bind() error: %d\n", HEART, status);

    status = setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(int) );
    if( status < 0 ) fprintf( stderr, "%s: setsockopt SO_BROADCAST failed\n", HEART);

    tv.tv_sec = RX_TIMEOUT_SEC;
    tv.tv_usec = 0;

    status = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if( status < 0 ) fprintf( stderr, "%s: setsockopt SO_RCVTIMEO failed\n", HEART);

    getcwd(cwd_p, (size_t)(2 * BUF_LEN));

    pid = (unsigned long)getpid();
    starttime = (unsigned long)time(0);

    next_beacon_time = starttime + beacon_interval;

    /* Use just a single socket and thread to send heartbeats */
    /* and handle incoming requests */
    for( ; ; )
    {
        /* Look for received commands */
        byte_count = (int)recvfrom(sock, buf_p, BUF_LEN, 0,
            (struct sockaddr *)&rx_addr, &rx_addr_len);

        /* Process any received commands */
        process_rx_command(sock, &rx_addr, byte_count, buf_p, counter);

        curtime = (unsigned long)(time(0));

        /* Send the next heartbeat if needed.  Due to the timeout */
        /* in the recvfrom() call above, we could be up to 'timeout' late */
        if( curtime > next_beacon_time )
        {
            counter++;
            next_beacon_time += beacon_interval;
            uptime = curtime - starttime;

            byte_count = snprintf(buf_p, (size_t)BUF_LEN,
                "{\"seq\":\"%u\",\"sp\":\"%u\",\"up\":\"%lu\",\"cur\":\"%lu\",\"pid\":\"%lu\",\"cwd\":\"%s\",\"ver\":\"%s\",\"img\":\"%s\",\"stcmd\":\"%s\"}",
                counter, ca_server_port, uptime, curtime, pid, cwd_p, HEARTBEAT_VERSION,
                threadArgs_p->arg0_p == (char *)0 ? "Unknown" : threadArgs_p->arg0_p,
                threadArgs_p->arg1_p == (char *)0 ? "Unknown" : threadArgs_p->arg1_p
            );

            if( byte_count >= BUF_LEN )
            {
                fprintf(stderr, "%s: Buffer overflow composing heartbeat\n", HEART);
                continue;
            }

            /* printf("ca_server_port: %u\n", ca_server_port); */
            sock_in.sin_addr.s_addr = htonl(-1); /* send message to 255.255.255.255 */
            sock_in.sin_port = htons(HEARTBEAT_PORT); /* port number */

            status = sendto(sock, buf_p, byte_count, 0,
                (struct sockaddr *)&sock_in, sizeof(struct sockaddr_in));

            if( status < 0 ) fprintf(stderr, "%s: sendto() failed\n", HEART);
        }
    }
    free(buf_p);
    free(cwd_p);
    return 0;
}

/*---------------------------------------------------------------------------*/
void heartbeat(int argc, char *argv[])
{
    ThreadArgs_p threadArgs_p;

    threadArgs_p = (ThreadArgs_p)malloc(sizeof(ThreadArgs_t));

    if( argc > 0 )
    {
        threadArgs_p->arg0_p = (char *)malloc(strlen(argv[0]) + 1);
        strcpy(threadArgs_p->arg0_p, argv[0]);
    }
    else
    {
        threadArgs_p->arg0_p = (char *)0;
    }

    if( argc > 1 )
    {
        threadArgs_p->arg1_p = (char *)malloc(strlen(argv[1]) + 1);
        strcpy(threadArgs_p->arg1_p, argv[1]);
    }
    else
    {
        threadArgs_p->arg1_p = (char *)0;
    }

#if 0
    {
        int i;
        for( i = 0 ; i < argc ; i++ )
        {
            printf("%s: argument: %d val: '%s'\n", HEART, i, argv[i]);
        }
    }
#endif

    threadArgs_p->mode = 0;
    pthread_create( &gHeartbeatThread, 0, (void *)threadEntry, (void *)threadArgs_p );

    return;
}
