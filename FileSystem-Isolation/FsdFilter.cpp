/*
 * Copyright (c) 2015 Slava Imameev. All rights reserved.
 */

#include <IOKit/IOLib.h>
#include <IOKit/IODeviceTreeSupport.h>
#include <libkern/c++/OSContainers.h>
#include <IOKit/assert.h>
#include <IOKit/IOCatalogue.h>
#include "Common.h"
#include "FsdFilter.h"
#include "VFSHooks.h"
#include "Kauth.h"
#include "VNodeHook.h"
#include "VifSparseFile.h"
#include "VifVnodeHashTable.h"
#include "VifVfsMntHook.h"
#include "FltFakeFSD.h"
#include "VifCoveringVnode.h"
#include "VifUndocumentedQuirks.h"

//--------------------------------------------------------------------

VfsIsolationFilter* VfsIsolationFilter::Instance;

//--------------------------------------------------------------------

//
// the standard IOKit declarations
//
#undef super
#define super IOService

OSDefineMetaClassAndStructors(VfsIsolationFilter, IOService)

//--------------------------------------------------------------------

bool
VfsIsolationFilter::start(
    __in IOService *provider
    )
{
    
    Instance = this;
    
    if( ! VifInitUndocumentedQuirks() ){
        
        DBG_PRINT_ERROR( ( "VifInitUndocumentedQuirks() failed\n" ) );
        goto __exit_on_error;
    }
    
    if( kIOReturnSuccess != FltGetVnodeLayout() ){
        
        DBG_PRINT_ERROR( ( "FltGetVnodeLayout() failed\n" ) );
        goto __exit_on_error;
    }
    
    if( kIOReturnSuccess != VifSparseFile::sInitSparseFileSubsystem() ){
        
        DBG_PRINT_ERROR( ( "VifSparseFile::sInitSparseFileSubsystem() failed\n" ) );
        goto __exit_on_error;
    }
    
    //
    // init the vnode subsystem, as no any kauth callback has been set the subsystem won't be called,
    // the subsystem can't be called before the vnode hooks initializtion as it calls the vnode hooks
    // subsystem when being called from the kauth callback
    //
    if( !VifVnodeHashTable::CreateStaticTable() ){
        
        DBG_PRINT_ERROR( ( "VifVnodeHashTable::CreateStaticTable() failed\n" ) );
        goto __exit_on_error;
    }
    
    
    if( ! FltVnodeHooksHashTable::CreateStaticTableWithSize( 8, true ) ){
        
        DBG_PRINT_ERROR( ( "FltVnodeHooksHashTable::CreateStaticTableWithSize() failed\n" ) );
        goto __exit_on_error;
    }
    
    if( !VifIOVnode::InitVnodeSubsystem() ){
        
        DBG_PRINT_ERROR( ( "DldIOVnode::InitVnodeSubsystem() failed\n" ) );
        goto __exit_on_error;
    }
    
    //
    // init the mount's VFS hooks
    //
    if( kIOReturnSuccess != VifVfsMntHook::CreateVfsMntHook() ){

        DBG_PRINT_ERROR( ( "VifVfsMntHook::CreateVfsMntHook() failed\n" ) );
        goto __exit_on_error;
    }
    
    //
    // init a sparse file hash table, the hash table is required for the Isolation subsystem
    //
    if( !VifSparseFilesHashTable::CreateStaticTableWithSize( 100, true ) ){
        
        assert( !"VifSparseFilesHashTable::CreateStaticTableWithSize( 100, true ) failed" );
        DBG_PRINT_ERROR( ( "VifSparseFilesHashTable::CreateStaticTableWithSize( 100, true ) failed\n" ) );
        
        goto __exit_on_error;
    }
    
    if( !VifCoveringFsd::InitCoveringFsd() ){
        
        DBG_PRINT_ERROR( ( "DldCoveringFsd::InitCoveringFsd() failed\n" ) );
       goto __exit_on_error;
    }
    
    if( kIOReturnSuccess != VFSHookInit() ){
        
        DBG_PRINT_ERROR( ( "VFSHookInit() failed\n" ) );
        goto __exit_on_error;
    }
    
    //
    // create an object for the vnodes KAuth callback and register the callback,
    // the callback might be called immediatelly just after registration!
    //
    gVnodeGate = FltIOKitKAuthVnodeGate::withCallbackRegistration( this );
    assert( NULL != gVnodeGate );
    if( NULL == gVnodeGate ){
        
        DBG_PRINT_ERROR( ( "FltIOKitKAuthVnodeGate::withDefaultSettings() failed\n" ) );
        goto __exit_on_error;
    }
    
    //
    // register with IOKit to allow the class matching
    //
    registerService();
    
    //
    // make the driver non-unloadable
    //
    this->retain();

    return true;
    
__exit_on_error:
    
    assert( !"Error in VfsIsolationFilter::start" );
    //
    // all cleanup will be done in stop() and free()
    //
    this->release();
    return false;
}

//--------------------------------------------------------------------

void
VfsIsolationFilter::stop(
    __in IOService * provider
    )
{
    super::stop( provider );
}

//--------------------------------------------------------------------

bool VfsIsolationFilter::init()
{
    if(! super::init() )
        return false;
    
    return true;
}

//--------------------------------------------------------------------

//
// actually this will not be called as the module should be unloadable in release build
//
void VfsIsolationFilter::free()
{
    if( gVnodeGate ){
        
        gVnodeGate->release();
        gVnodeGate = NULL;
    }
    
    VFSHookRelease();
    
    VifVfsMntHook::DestructVfsMntHook();
    
    VifIOVnode::InitVnodeSubsystem();
    
    VifSparseFilesHashTable::DeleteStaticTable();
    
    VifVnodeHashTable::DeleteStaticTable();
    
    FltVnodeHooksHashTable::DeleteStaticTable();
    
    VifSparseFile::sFreeSparseFileSubsystem();
    
    VifFreeUndocumentedQuirks();
    
    super::free();
}

//--------------------------------------------------------------------


