//
//  Kauth.cpp
//
//  Copyright (c) 2015 Slava Imameev. All rights reserved.
//

#include "Common.h"
#include "Kauth.h"
#include "VFSHooks.h"
#include "FsdFilter.h"
#include "VNodeHook.h"
#include "FltFakeFSD.h"

//--------------------------------------------------------------------

FltIOKitKAuthVnodeGate*     gVnodeGate;

//--------------------------------------------------------------------

#define super OSObject
OSDefineMetaClassAndStructors( FltIOKitKAuthVnodeGate, OSObject)

//--------------------------------------------------------------------

IOReturn
FltIOKitKAuthVnodeGate::RegisterVnodeScopeCallback(void)
{    
    //
    // register our listener
    //
    this->VnodeListener = kauth_listen_scope( KAUTH_SCOPE_VNODE,                              // for the vnode scope
                                              FltIOKitKAuthVnodeGate::VnodeAuthorizeCallback, // using this callback
                                              this );                                         // give a cookie to callback
    
    if( NULL == this->VnodeListener ){
        
        DBG_PRINT_ERROR( ( "kauth_listen_scope failed\n" ) );
        return kIOReturnInternalError;
        
    }
    
    return kIOReturnSuccess;
}

//--------------------------------------------------------------------

int
FltIOKitKAuthVnodeGate::VnodeAuthorizeCallback(
                                                  kauth_cred_t    credential, // reference to the actor's credentials
                                                  void           *idata,      // cookie supplied when listener is registered
                                                  kauth_action_t  action,     // requested action
                                                  uintptr_t       arg0,       // the VFS context
                                                  uintptr_t       arg1,       // the vnode in question
                                                  uintptr_t       arg2,       // parent vnode, or NULL
                                                  uintptr_t       arg3)       // pointer to an errno value
{
    FltIOKitKAuthVnodeGate*    _this;
    vnode_t                    vnode = (vnode_t)arg1;
    
    assert( preemption_enabled() );
    
    //
    // if this is a dead vnode then skip it
    //
    if( vnode_isrecycled( vnode ) )
        return KAUTH_RESULT_DEFER;
    
    _this = (FltIOKitKAuthVnodeGate*)idata;
    
    //
    // VNON vnode is created by devfs_devfd_lookup() for /dev/fd/X vnodes that
    // are not of any interest for us
    // VSOCK is created for UNIX sockets
    // etc.
    //
    enum vtype   vnodeType = vnode_vtype( vnode );    
    if( VREG != vnodeType &&
        VDIR != vnodeType )
        return KAUTH_RESULT_DEFER;
    
    //
    // hook this vnode, this starts filtering for this vnode
    //
    bool  isHooked = false;
    FltHookVnodeVop( vnode, &isHooked );
    
    return KAUTH_RESULT_DEFER;
}

//--------------------------------------------------------------------

FltIOKitKAuthVnodeGate* FltIOKitKAuthVnodeGate::withCallbackRegistration(
    __in VfsIsolationFilter* _provider
    )
/*
 the caller must call the release() function for the returned object when it is not longer needed
 */
{
    IOReturn                   RC;
    FltIOKitKAuthVnodeGate*    pKAuthVnodeGate;
    
    pKAuthVnodeGate = new FltIOKitKAuthVnodeGate();
    assert( pKAuthVnodeGate );
    if( !pKAuthVnodeGate ){
        
        DBG_PRINT_ERROR( ( "FltIOKitKAuthVnodeGate::withCallbackRegistration FltIOKitKAuthVnodeGate allocation failed\n" ) );
        return NULL;
    }
    
    //
    // IOKit base classes initialization
    //
    if( !pKAuthVnodeGate->init() ){
        
        DBG_PRINT_ERROR( ( "FltIOKitKAuthVnodeGate::withCallbackRegistration init() failed\n" ) );
        pKAuthVnodeGate->release();
        return NULL;
    }
    
    pKAuthVnodeGate->provider = _provider;
    
    //
    // register the callback, it will be active immediatelly after registration, i.e. before control leaves the function
    //
    RC = pKAuthVnodeGate->RegisterVnodeScopeCallback();
    assert( kIOReturnSuccess == RC );
    if( kIOReturnSuccess != RC ){
        
        DBG_PRINT_ERROR( ( "pKAuthVnodeGate->RegisterVnodeScopeCallback() failed with the 0x%X error\n", RC ) );
        pKAuthVnodeGate->release();
        return NULL;
    }
    
    return pKAuthVnodeGate;
}

//--------------------------------------------------------------------

bool FltIOKitKAuthVnodeGate::init()
{
    if(! super::init() )
        return false;
    
    //
    // add code here
    //
    
    return true;
}

//--------------------------------------------------------------------

void FltIOKitKAuthVnodeGate::free()
{
    //
    // add code here
    //
    
    super::free();
}

//--------------------------------------------------------------------

