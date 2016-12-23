/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#include "VifUndocumentedQuirks.h"
#include "VmPmap.h"
#include <sys/lock.h>
#include <sys/proc.h>

//--------------------------------------------------------------------

//
// it is hardly that we will need more than CPUs in the system,
// but we need a reasonable prime value for a good dustribution
//
static IOSimpleLock*    gPreemptionLocks[ 19 ];

//--------------------------------------------------------------------

//
// an offset to mach task pointer in the BSD proc structure,
// used for a conversion from BSD process to mach task
//
static vm_offset_t  gTaskOffset = (vm_offset_t)(-1);

//
// an offset to the task's bsd_info field, used to convert the
// mach task to a bsd proc if the last exists( there might
// be a mach task without a corresponding BSD process ), the bsd_info
// field is zeroed on process exit
//
static vm_offset_t  gProcOffset = (vm_offset_t)(-1);

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

task_t VifBsdProcToTask( __in proc_t proc )
{
    assert( (vm_offset_t)(-1) != gTaskOffset );
    
    return *(task_t*)( (vm_offset_t)proc + gTaskOffset );
}

//--------------------------------------------------------------------

// use get_bsdtask_info() instead!
proc_t VifTaskToBsdProc( __in task_t task )
{
    assert( (vm_offset_t)(-1) != gProcOffset );
    
    proc_t  proc;
    
    proc = *(proc_t*)( (vm_offset_t)task + gProcOffset );
    
    // like get_bsdtask_info() does
    if( !proc )
        proc = kernproc;
    
    return proc;
}

//--------------------------------------------------------------------

vm_offset_t
VifGetTaskOffsetInBsdProc(
                          __in proc_t  bsdProc,
                          __in task_t  machTask
                          )
{
    task_t*  task_p = (task_t*)bsdProc;
    
    //
    // it is unlikely that the proc structure size will be greater than a page size
    //
    while( task_p < (task_t*)( (vm_offset_t)bsdProc + PAGE_SIZE ) ){
        
        if( (vm_offset_t)task_p == page_aligned( (vm_offset_t)task_p ) ){
            
            //
            // page boundary crossing, check for validity
            //
            if( 0x0 == FltVirtToPhys( (vm_offset_t)task_p ) )
                break;
            
        }// end if
        
        if( *task_p == machTask )
            return ( (vm_offset_t)task_p - (vm_offset_t)bsdProc );
        
        ++task_p;
        
    }// end while
    
    //
    // failed to find an offset
    //
    return (vm_offset_t)(-1);
}

//--------------------------------------------------------------------

vm_offset_t
VifGetBsdProcOffsetInTask(
                          __in proc_t  bsdProc,
                          __in task_t  machTask
                          )
/*
 returns the task_t's bsd_info field offset,
 the kernel has an internal function
 void  *get_bsdtask_info(task_t t)
 {
 return(t->bsd_info);
 }
 which is unavailable for third party kernel extensions
 */
{
    proc_t*  proc_p = (proc_t*)machTask;
    
    //
    // it is unlikely that the task structure size will be greater than a page size
    //
    while( proc_p < (proc_t*)( (vm_offset_t)machTask + PAGE_SIZE ) ){
        
        if( (vm_offset_t)proc_p == page_aligned( (vm_offset_t)proc_p ) ){
            
            //
            // page boundary crossing, check for validity
            //
            if( 0x0 == FltVirtToPhys( (vm_offset_t)proc_p ) )
                break;
            
        }// end if
        
        if( *proc_p == bsdProc )
            return ( (vm_offset_t)proc_p - (vm_offset_t)machTask );
        
        ++proc_p;
        
    }// end while
    
    //
    // failed to find an offset
    //
    return (vm_offset_t)(-1);
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
    
    //
    // get a mach task field offset in BSD proc
    //
    gTaskOffset = VifGetTaskOffsetInBsdProc( current_proc(), current_task() );
    assert( ((vm_offset_t)(-1)) != gTaskOffset );
    if( ((vm_offset_t)(-1)) == gTaskOffset ){
        
        DBG_PRINT_ERROR(("VifGetTaskOffsetInBsdProc() failed\n"));
        return false;
    }
    
    gProcOffset = VifGetBsdProcOffsetInTask( current_proc(), current_task() );
    assert( ((vm_offset_t)(-1)) != gProcOffset );
    if( ((vm_offset_t)(-1)) == gProcOffset ){
        
        DBG_PRINT_ERROR(("VifGetBsdProcOffsetInTask() failed\n"));
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
