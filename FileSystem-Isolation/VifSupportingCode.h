/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#ifndef VIFSUPPORTINGCODE_H
#define VIFSUPPORTINGCODE_H

#include <sys/types.h>
#include <sys/kauth.h>
#include <sys/wait.h>
#include "Common.h"

#define VIF_INVALID_EVENT_VALUE ((UInt32)(-1))

inline
void
VifInitNotificationEvent(
    __in UInt32* event
    )
{
    *event = 0x0;
}

//
// sets the event in a signal state
//
void
VifSetNotificationEvent(
    __in UInt32* event
    );

//
// wait for event, the event is not reseted after the wait is completed
//
wait_result_t
VifWaitForNotificationEventWithTimeout(
    __in UInt32* event,
    __in uint32_t  uSecTimeout // mili seconds, if ( -1) the infinite timeout
    );

inline
void
VifWaitForNotificationEvent(
    __in UInt32* event
    )
{
    VifWaitForNotificationEventWithTimeout( event, (-1) );
}


inline
void
VifInitSynchronizationEvent(
    __in UInt32* event
    )
{
    VifInitNotificationEvent( event );
}

//
// sets the event in a signal state
//
inline
void
VifSetSynchronizationEvent(
    __in UInt32* event
    )
{
    VifSetNotificationEvent( event );
}

//
// wait for event, the event is reseted if the wait is completed not because of a timeout 
//
inline
wait_result_t
VifWaitForSynchronizationEventWithTimeout(
    __in UInt32* event,
    __in uint32_t  uSecTimeout // mili seconds, if ( -1) the infinite timeout
    )
{
    wait_result_t waitResult;
    
    waitResult = VifWaitForNotificationEventWithTimeout( event, uSecTimeout );
    if( THREAD_AWAKENED == waitResult ){
        
        //
        // reset the event, notice that you can lost the event set after
        // VifWaitForNotificationEvent was woken up and before the event
        // is reset
        //
        OSCompareAndSwap( 0x3, 0x0, event );
    }
    
    return waitResult;
}

inline
void
VifWaitForSynchronizationEvent(
    __in UInt32* event
    )
{
    
    VifWaitForNotificationEvent( event );
    
    //
    // reset the event, notice that you can lost the event set after
    // VifWaitForNotificationEvent was woken up and before the event
    // is reset
    //
    OSCompareAndSwap( 0x3, 0x0, event );
}

void
VifMemoryBarrier();

errno_t
VifVnodeSetsize(vnode_t vp, off_t size, int ioflag, vfs_context_t ctx);

#endif//VIFSUPPORTINGCODE_H

