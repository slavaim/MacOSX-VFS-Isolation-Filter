/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#include "VifUndocumentedQuirks.h"
#include <sys/lock.h>
#include <sys/proc.h>

//--------------------------------------------------------------------

//
// it is hardly that we will need more than CPUs in the system,
// but we need a reasonable prime value for a good dustribution
//
static IOSimpleLock*    gPreemptionLocks[ 19 ];

//--------------------------------------------------------------------

bool
VifAllocateInitPreemtionLocks()
{
    
    for( int i = 0x0; i < VIF_STATIC_ARRAY_SIZE( gPreemptionLocks ); ++i ){
        
        gPreemptionLocks[ i ] = IOSimpleLockAlloc();
        assert( gPreemptionLocks[ i ] );
        if( !gPreemptionLocks[ i ] )
            return false;
        
    }
    
    return true;
}

//--------------------------------------------------------------------

void
VifFreePreemtionLocks()
{
    for( int i = 0x0; i < VIF_STATIC_ARRAY_SIZE( gPreemptionLocks ); ++i ){
        
        if( gPreemptionLocks[ i ] )
            IOSimpleLockFree( gPreemptionLocks[ i ] );
        
    }
}

//--------------------------------------------------------------------

#define VifTaskToPreemptionLockIndx( _t_ ) (int)( ((vm_offset_t)(_t_)>>5)%VIF_STATIC_ARRAY_SIZE( gPreemptionLocks ) )

//
// MAC OS X kernel doesn't export disable_preemtion() and does not
// allow a spin lock allocaton on the stack! Marvelous, first time
// I saw such a reckless design! All of the above are allowed by
// Windows kernel as IRQL rising to DISPATCH_LEVEL and KSPIN_LOCK.
//

//
// returns a cookie for VifEnablePreemption
//
int
VifDisablePreemption()
{
    int indx;
    
    //
    // check for a recursion or already disabled preemption,
    // we support a limited recursive functionality - the
    // premption must not be enabled by calling enable_preemption()
    // while was disabled by this function
    //
    if( !preemption_enabled() )
        return (int)(-1);
    
    indx = VifTaskToPreemptionLockIndx( current_task() );
    assert( indx < VIF_STATIC_ARRAY_SIZE( gPreemptionLocks ) );
    
    //
    // acquiring a spin lock have a side effect of preemption disabling,
    // so try to find any free slot for this thread
    //
    while( !IOSimpleLockTryLock( gPreemptionLocks[ indx ] ) ){
        
        indx = (indx+1)%VIF_STATIC_ARRAY_SIZE( gPreemptionLocks );
        
    }// end while
    
    assert( !preemption_enabled() );
    
    return indx;
}

//--------------------------------------------------------------------

//
// accepts a value returned by VifDisablePreemption
//
void
VifEnablePreemption( __in int cookie )
{
    assert( !preemption_enabled() );
    
    //
    // check whether a call to VifDisablePreemption was a void one
    //
    if( (int)(-1) == cookie )
        return;
    
    assert( cookie < VIF_STATIC_ARRAY_SIZE( gPreemptionLocks ) );
    
    //
    // release the lock thus enabling preemption
    //
    IOSimpleLockUnlock( gPreemptionLocks[ cookie ] );
    
    assert( preemption_enabled() );
}

//--------------------------------------------------------------------

bool
VifInitUndocumentedQuirks()
{
    
    assert( preemption_enabled() );
    assert( current_task() == kernel_task );
    
    if( !VifAllocateInitPreemtionLocks() ){
        
        DBG_PRINT_ERROR(("VifAllocateInitPreemtionLocks() failed\n"));
        return false;
    }
    
    return true;
}

//--------------------------------------------------------------------

void
VifFreeUndocumentedQuirks()
{
    VifFreePreemtionLocks();
}

//--------------------------------------------------------------------
