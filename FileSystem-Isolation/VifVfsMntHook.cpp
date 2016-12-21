/* 
 * Copyright (c) 2011 Slava Imameev. All rights reserved.
 */

#include "VifVfsMntHook.h"
#include "VmPmap.h"

//--------------------------------------------------------------------

OSArray*  VifVfsMntHook::sMntsArray = NULL;
IORWLock* VifVfsMntHook::sRWLock = NULL;

//--------------------------------------------------------------------

#define super OSObject

OSDefineMetaClassAndStructors( VifVfsMntHook, OSObject )

//--------------------------------------------------------------------

IOReturn VifVfsMntHook::CreateVfsMntHook()
{
    VifVfsMntHook::sRWLock = IORWLockAlloc();
    assert( VifVfsMntHook::sRWLock );
    if( !VifVfsMntHook::sRWLock ){
        
        DBG_PRINT_ERROR(( "IORWLockAlloc() failed\n" ));
        return kIOReturnNoMemory;
    }
    
    VifVfsMntHook::sMntsArray = OSArray::withCapacity( 4 );
    assert( VifVfsMntHook::sMntsArray );
    if( !VifVfsMntHook::sMntsArray ){
        
        DBG_PRINT_ERROR(( "OSArray::withCapacity() failed\n" ));
        return kIOReturnNoMemory;
    }
    
    return kIOReturnSuccess;
}

//--------------------------------------------------------------------

void VifVfsMntHook::DestructVfsMntHook()
{
    if( VifVfsMntHook::sRWLock )
        IORWLockFree( VifVfsMntHook::sRWLock );
    
    if( VifVfsMntHook::sMntsArray )
        VifVfsMntHook::sMntsArray->release();
}

//--------------------------------------------------------------------

VFSFUNC* VifVfsMntHook::getVfsOperationEntryForVfsVector( __in VFSFUNC* vfsOpVector, __in FltVfsOperation op )
{
    
    assert( op < kFltVfsOpMax );
    
    FltVfsOperationDesc*  dsc;
    
    dsc = FltGetVfsOperationDesc( op );
    if( !dsc )
        return NULL;
    
    assert( VIF_VOP_UNKNOWN_OFFSET != dsc->offset );
    
    return (VFSFUNC*)((vm_address_t)vfsOpVector + dsc->offset);
}

//--------------------------------------------------------------------

VFSFUNC* VifVfsMntHook::getVfsOperationEntryForMount( __in mount_t mnt, __in FltVfsOperation op )
{
    VFSFUNC* vfsOpVector;
    
    assert( op < kFltVfsOpMax );
    
    vfsOpVector = FltGetVfsOperations( mnt );
    assert( vfsOpVector );
    if( !vfsOpVector )
        return NULL;
    
    return VifVfsMntHook::getVfsOperationEntryForVfsVector( vfsOpVector, op );
}

//--------------------------------------------------------------------

//
// the returned object is not referenced, a caller must hold the lock
//
VifVfsMntHook*  VifVfsMntHook::findInArrayByMount( __in mount_t mnt )
{
    int count = VifVfsMntHook::sMntsArray->getCount();
    
    //
    // it is unlikely that there will be so many mounted FS
    //
    assert( count < 0xFF );
    
    for( int i = 0x0; i < count; ++i ){
        
        VifVfsMntHook*  currentHookObj = OSDynamicCast( VifVfsMntHook, VifVfsMntHook::sMntsArray->getObject( i ) );
        assert( currentHookObj );
        
        if( currentHookObj->mnt != mnt )
            continue;
        
        return currentHookObj;
    } // end for
    
    return NULL;
}

//--------------------------------------------------------------------

//
// the returned object is not referenced, a caller must hold the lock
//
VifVfsMntHook::VifVfsHook*  VifVfsMntHook::findInArrayByVfsOpVector( __in VFSFUNC* vfsVector )
{
    int count = VifVfsMntHook::sMntsArray->getCount();
    
    //
    // it is unlikely that there will be so many mounted FS
    //
    assert( count < 0xFF );
    
    for( int i = 0x0; i < count; ++i ){
        
        VifVfsMntHook*  currentHookObj = OSDynamicCast( VifVfsMntHook, VifVfsMntHook::sMntsArray->getObject( i ) );
        assert( currentHookObj );
        
        assert( currentHookObj->vfsHook && currentHookObj->vfsHook->getVfsOpsVector() );
        
        if( currentHookObj->vfsHook->getVfsOpsVector() != vfsVector )
            continue;
        
        return currentHookObj->vfsHook;
    } // end for
    
    return NULL;
}

//--------------------------------------------------------------------

void VifVfsMntHook::VifVfsHook::retain()
{
    assert( this->referenceCount > 0x0 );
    OSIncrementAtomic( &this->referenceCount );
}

//--------------------------------------------------------------------

void VifVfsMntHook::VifVfsHook::release()
{
    assert( preemption_enabled() );
    assert( this->referenceCount > 0x0 );
    
    if( 0x1 != OSDecrementAtomic( &this->referenceCount ) )
        return;
    
    //
    // the last reference has gone,
    // unhook all VFS operations
    //
    for( int i = 0x0; i < VIF_STATIC_ARRAY_SIZE( this->originalVfsOps ); ++i ){
        
        if( NULL == this->originalVfsOps[ i ] ){
            
            //
            // not hooked
            //
            continue;
        }
        
        assert( i != kFltVfsOpUnknown );
        
        VFSFUNC*  funcToUnhookEntry;
        
        //
        // hook and add in the array, currently only unmount is hooked to support ISOALTION
        //
        funcToUnhookEntry = VifVfsMntHook::getVfsOperationEntryForVfsVector( (VFSFUNC*)this->vfsOpsVector, (FltVfsOperation)i );
        assert( funcToUnhookEntry );
        if( funcToUnhookEntry ){
            
            unsigned int bytes;
            
            //
            // replace the function address with the original one
            //
            
            bytes = FltWriteWiredSrcToWiredDst( (vm_offset_t)&this->originalVfsOps[ i ],
                                                (vm_offset_t)funcToUnhookEntry,
                                                sizeof( VFSFUNC ) );
            
            assert( sizeof( VFSFUNC ) == bytes );
            
            this->originalVfsOps[ i ] = NULL;
            
        } else {
            
            DBG_PRINT_ERROR(( "getVfsOperationEntryForVfsVector(%u) failed\n", (unsigned int)i ));
        }
        
    } // end for
    
    delete this;
}

//--------------------------------------------------------------------

typedef struct _VifVfsHookDscr{
    FltVfsOperation  operation;
    VFSFUNC          hookingFunction;
} VifVfsHookDscr;

VifVfsHookDscr gVifVfsHookDscrs[] = 
{
    { kFltVfsOpUnmount, (VFSFUNC)VifVfsMntHook::VifVfsUnmountHook },
    
    //
    // the last termintaing entry
    //
    { kFltVfsOpUnknown, (VFSFUNC)NULL }
};

//--------------------------------------------------------------------

VifVfsMntHook::VifVfsHook*  VifVfsMntHook::VifVfsHook::withVfsOpsVector( __in VFSFUNC*  vfsOpsVector )
{
    //
    // THE CALLER MUST HOLD THE LOCK EXCLUSIVELY
    //
    
    VifVfsMntHook::VifVfsHook*  vfsObj = NULL;
    
    assert( vfsOpsVector );
    if( !vfsOpsVector )
        return NULL;
    
    //
    // first try to search in the array
    //
    vfsObj = VifVfsMntHook::findInArrayByVfsOpVector( vfsOpsVector );
    if( vfsObj ){
        
        vfsObj->retain();
        return vfsObj;
        
    }
    
    vfsObj = new VifVfsMntHook::VifVfsHook();
    assert( vfsObj );
    if( !vfsObj ){
        
        DBG_PRINT_ERROR(( "new VifVfsMntHook::VifVfsHook() faile\n" ));
        return NULL;
    }
    
    vfsObj->vfsOpsVector = vfsOpsVector;
    vfsObj->referenceCount = 0x1;
    
    for( int i = 0x0; kFltVfsOpUnknown != gVifVfsHookDscrs[ i ].operation; ++i ){
        
        VFSFUNC*  funcToHookEntry;
        
        assert( gVifVfsHookDscrs[ i ].hookingFunction );
        
        //
        // hook and add in the array, currently only unmount is hooked to support ISOALTION
        //
        funcToHookEntry = VifVfsMntHook::getVfsOperationEntryForVfsVector( (VFSFUNC*)vfsOpsVector, gVifVfsHookDscrs[ i ].operation );
        assert( funcToHookEntry );
        if( funcToHookEntry ){
            
            unsigned int bytes;
            
            assert( gVifVfsHookDscrs[ i ].operation < VIF_STATIC_ARRAY_SIZE( vfsObj->originalVfsOps ) );
            assert( gVifVfsHookDscrs[ i ].hookingFunction != *funcToHookEntry );
            
            //
            // exchange the functions
            //
            vfsObj->originalVfsOps[ gVifVfsHookDscrs[ i ].operation ] = *funcToHookEntry;
            
            bytes = FltWriteWiredSrcToWiredDst( (vm_offset_t)&gVifVfsHookDscrs[ i ].hookingFunction,
                                                (vm_offset_t)funcToHookEntry,
                                                sizeof( VFSFUNC ) );
            
            assert( sizeof( VFSFUNC ) == bytes );
            
        } else {
            
            DBG_PRINT_ERROR(( "getVfsOperationEntryForVfsVector(%u) failed\n", (unsigned int)gVifVfsHookDscrs[ i ].operation  ));
        }
        
    } // end for
    
    return vfsObj;
}

//--------------------------------------------------------------------

VifVfsMntHook*  VifVfsMntHook::withVfsMnt( __in mount_t mnt )
{
    assert( preemption_enabled() );
    assert( VifVfsMntHook::sRWLock );
    assert( VifVfsMntHook::sMntsArray );
    assert( mnt );
    
    VifVfsMntHook*  mntHookObj = NULL;
    VifVfsMntHook*  objToRelease = NULL;
    
    if( !mnt )
        return NULL;
    
    //
    // first try to search in the array
    //
    IORWLockRead( VifVfsMntHook::sRWLock );
    { // start of the lock
        
        mntHookObj = VifVfsMntHook::findInArrayByMount( mnt );
        if( mntHookObj )
            mntHookObj->retain();
        
    } // end of the lock
    IORWLockUnlock( VifVfsMntHook::sRWLock );
    
    if( mntHookObj )
        return mntHookObj;
    
    //
    // create a new object
    //
    mntHookObj = new VifVfsMntHook();
    assert( mntHookObj );
    if( !mntHookObj ){
        
        DBG_PRINT_ERROR(( "new VifVfsMntHook() fauled\n" ));
        return NULL;
    }
    
    if( !mntHookObj->init() ){
        
        DBG_PRINT_ERROR(( "mntHookObj->init() failed\n" ));
        mntHookObj->release();
        return NULL;
    }
    
    mntHookObj->mnt = mnt;
    
    //
    // hook the VFS operations, there should not be two hooking thread simultaneously
    //
    IORWLockWrite( VifVfsMntHook::sRWLock );
    { // start of the lock
        
        //
        // first check thet it has not been already hooked
        //
        if( VifVfsMntHook::findInArrayByMount( mnt ) ){
            
            //
            // releasing under the lock is not allowed as results in the deadlock
            //
            objToRelease = mntHookObj;
            objToRelease->duplicateObject = true;
            mntHookObj = VifVfsMntHook::findInArrayByMount( mnt );
            
            assert( mntHookObj && mntHookObj->vfsHook );
            mntHookObj->retain();
            
        } else {
            
            mntHookObj->vfsHook = VifVfsMntHook::VifVfsHook::withVfsOpsVector( ::FltGetVfsOperations( mnt ) );
            assert( mntHookObj->vfsHook );
            if( mntHookObj->vfsHook ){
                
                //
                // insert in the array, the array retains the object!
                //
                if( !VifVfsMntHook::sMntsArray->setObject( mntHookObj ) ){
                                       
                    DBG_PRINT_ERROR(( "VifVfsMntHook::sMntsArray->setObject( mntHookObj ) failed\n" ));
                    assert( NULL == objToRelease );
                    objToRelease = mntHookObj; // do not release under the lock!
                    mntHookObj = NULL;
                }
                
            } else {
                
                
                DBG_PRINT_ERROR(( "VifVfsMntHook::VifVfsHook::withVfsOpsVector failed\n" ));
                assert( NULL == objToRelease );
                objToRelease = mntHookObj; // do not release under the lock!
                mntHookObj = NULL;
            }
            
        }
        
    } // end of the lock
    IORWLockUnlock( VifVfsMntHook::sRWLock );
    
    if( objToRelease )
        objToRelease->release();
    
    return mntHookObj;
}

//--------------------------------------------------------------------

void VifVfsMntHook::free()
{
    //
    // it is okay for the duplicate object to be removed
    //
    assert(!( false == this->duplicateObject  && NULL != VifVfsMntHook::findInArrayByMount( this->mnt ) ) );
    assert( this != VifVfsMntHook::findInArrayByMount( this->mnt ) );
    
    if( this->vfsHook )
        this->vfsHook->release();
    
    super::free();
}

//--------------------------------------------------------------------

int  VifVfsMntHook::VifVfsUnmountHook(struct mount *mp, int mntflags, vfs_context_t context)
{
    VifVfsMntHook*  mntHookObj = NULL;
    typedef int  (*VfsUnmount)(struct mount *mp, int mntflags, vfs_context_t context);
    VfsUnmount      vfsUnmountOriginal;
    bool            allowUnmount = true;
    bool            forcedUnmount;
    
    forcedUnmount = ( 0x0 != (mntflags & MNT_FORCE) );
    
    //
    // search in the array and remove if found
    //
    IORWLockWrite( VifVfsMntHook::sRWLock );
    { // start of the lock
        
        int count = VifVfsMntHook::sMntsArray->getCount();
        
        //
        // it is unlikely that there will be so many mounted FS
        //
        assert( count < 0xFF );
        
        for( int i = 0x0; i < count; ++i ){
            
            assert( !mntHookObj );
            
            VifVfsMntHook*  currentHookObj = OSDynamicCast( VifVfsMntHook, VifVfsMntHook::sMntsArray->getObject( i ) );
            assert( currentHookObj );
            
            if( currentHookObj->mnt != mp )
                continue;
            
            mntHookObj = currentHookObj;
            
            //
            // if there no dirty files it is okay to remove the object
            // and allow the unmount, also the forced unmount is always
            // obeyed
            //
            allowUnmount = ( forcedUnmount || 0x0 == mntHookObj->dirtyIsolationFilesCnt );
            if( allowUnmount ){
                
                //
                // remove from the array, this also removes a reference, so retain before removing
                //
                mntHookObj->retain();
                VifVfsMntHook::sMntsArray->removeObject( i );
            }
            
            break;
        } // end for
        
        
    } // end of the lock
    IORWLockUnlock( VifVfsMntHook::sRWLock );
    
    assert( mntHookObj );
    if( !mntHookObj || !allowUnmount ){
        
        DBG_PRINT_ERROR(( "mntHookObj is %p, allowUnmount = %u\n", mntHookObj, (int)allowUnmount ));
        
        //
        // disable unmount to not damage the system
        //
        return EBUSY;
    }
    
    assert( mntHookObj->mnt == mp );
    assert( mntHookObj->vfsHook );
    
    vfsUnmountOriginal = (VfsUnmount)mntHookObj->vfsHook->getOriginalVfsOps( kFltVfsOpUnmount );
    assert( vfsUnmountOriginal );
    if( !vfsUnmountOriginal ){
        
        mntHookObj->release();
        
        DBG_PRINT_ERROR(( "vfsUnmountOriginal is NULL\n" ));
        return EBUSY;
        
    } // end if( !vfsUnmountOriginal )
    
    mntHookObj->vfsHook->release();
    mntHookObj->vfsHook = NULL;
    mntHookObj->release();
    
    return vfsUnmountOriginal( mp, mntflags, context );
}


//--------------------------------------------------------------------

IOReturn VifVfsMntHook::HookVfsMnt( __in mount_t mp )
{
    VifVfsMntHook*  mntHookObj;
    
    mntHookObj = VifVfsMntHook::withVfsMnt( mp );
    assert( mntHookObj );
    if( !mntHookObj ){
        
        DBG_PRINT_ERROR(( "withVfsMnt() failed to return an object\n" ));
        return kIOReturnError;
    }
    
    //
    // the object is retained by the array
    //
    mntHookObj->release();
    
    return kIOReturnSuccess;
}

//--------------------------------------------------------------------
