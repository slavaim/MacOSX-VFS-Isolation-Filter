/* 
 * Copyright (c) 2011 Slava Imameev. All rights reserved.
 */
    
#include <IOKit/assert.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <libkern/c++/OSArray.h>
#include "FltFakeFSD.h"

//--------------------------------------------------------------------

class VifVfsMntHook: public OSObject{
    
    OSDeclareDefaultStructors( VifVfsMntHook )

private:
    
    class VifVfsHook{
        
        friend class VifVfsMntHook;
        
    private:
        
        //
        // the structure is recycled when the reference count drops to zero
        //
        SInt32          referenceCount;
        
        //
        // addresses of original functions
        //
        VFSFUNC         originalVfsOps[ kFltVfsOpMax ];
        
        //
        // mnt->mnt_op value
        //
        VFSFUNC*        vfsOpsVector;
        
    protected:
        
        static VifVfsHook*   withVfsOpsVector( __in VFSFUNC*  vfsOpsVector );
        
        virtual void retain();
        virtual void release();
        
        virtual VFSFUNC*  getVfsOpsVector() { return vfsOpsVector; }
        
        virtual VFSFUNC  getOriginalVfsOps( __in FltVfsOperation op )
        { 
            assert( op < kFltVfsOpMax );
            return this->originalVfsOps[ op ];
        }
        
    };
    
private:
    
    //
    // a hooked mount structure
    //
    mount_t       mnt;
    
    //
    // several mount structures can share the same vfs operation structure,
    // the object is referenced
    //
    VifVfsHook*   vfsHook;
    
    //
    // number of "dirty" ISOALTION files, i.e. the number of ISOALTION backed files with unflushed data
    //
    SInt32        dirtyIsolationFilesCnt;
    
    //
    // true if the volume is under the ISOALTION control
    //
    bool          isolationOn;
    
    //
    // if true the object is a second duplicate as a concurrent thread managed to create
    // an object for the mounted volume structure before this object was created
    //
    bool         duplicateObject;
    
    //
    // a static array of VifVfsMntHook objects
    //
    static OSArray*  sMntsArray;
    
    //
    // a lock for sMntsArray protection
    //
    static IORWLock* sRWLock;
    
    //
    // the returned object is not referenced, a caller must hold the lock
    //
    static VifVfsMntHook* findInArrayByMount( __in mount_t mnt );
    
    //
    // the returned object is not referenced, a caller must hold the lock
    //
    static VifVfsHook*  findInArrayByVfsOpVector( __in VFSFUNC* vfsVector );
    
    //
    // returns an address for a VFS operation entry in a mount structure's VFS vector
    //
    static VFSFUNC* getVfsOperationEntryForMount( __in mount_t mnt, __in FltVfsOperation op );
    static VFSFUNC* getVfsOperationEntryForVfsVector( __in VFSFUNC* vfsOpVector, __in FltVfsOperation op );
    
protected:
    
    virtual void free();
    
public:
    
    virtual void incrementDirtyIsolationFilesCounter(){ OSIncrementAtomic( &this->dirtyIsolationFilesCnt ); }
    virtual void decrementDirtyIsolationFilesCounter(){ assert( this->dirtyIsolationFilesCnt > 0x0 ); OSDecrementAtomic( &this->dirtyIsolationFilesCnt ); }
    virtual SInt32 getDirtyIsolationFilesCounter(){ return this->dirtyIsolationFilesCnt; }
    
    virtual void setIsolationOn() { this->isolationOn = true; }
    virtual bool isIsolationOn() { return this->isolationOn; }
    
    static int  VifVfsUnmountHook(struct mount *mp, int mntflags, vfs_context_t context);
    
    //
    // returns a referenced object, if there is no object for
    // the mnt structure it will be created, the caller must
    // call release() when the object is no longer needed, the
    // function is idempotent in behaviour
    //
    static VifVfsMntHook*  withVfsMnt( __in mount_t mnt );
    
    //
    // a hook function, actually a wrapper for withVfsMnt,
    // there is no unhook function as unhook is performed
    // in the VifVfsUnmountHook routine
    //
    static IOReturn HookVfsMnt( __in mount_t mnt );
    
    //
    // static members initialization and destruction
    //
    static IOReturn CreateVfsMntHook();
    static void DestructVfsMntHook();

};

//--------------------------------------------------------------------
