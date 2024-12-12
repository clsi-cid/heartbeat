/*-----------------------------------------------------------------------------
heartbeat.h
-------------------------------------------------------------------------------
EPICS Heartbeat Extension Header File
-----------------------------------------------------------------------------*/

#ifndef h_heartbeat
#define h_heartbeat

#ifdef  External
#undef  External
#endif

#ifdef __cplusplus
#define External extern "C"
#else
#define External extern
#endif

/* Function prototypes */
External void heartbeat(int argc, char *argv[]);

#endif /* h_heartbeat */
