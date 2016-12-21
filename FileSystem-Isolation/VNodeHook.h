//
//  VNodeHook.h
//
//  Copyright (c) 2015 Slava Imameev. All rights reserved.
//

#ifndef __VFSFilter0__VNodeHook__
#define __VFSFilter0__VNodeHook__

#include <IOKit/assert.h>
#include <libkern/c++/OSObject.h>
#include <IOKit/assert.h>

#ifdef __cplusplus
extern "C" {
#endif
    
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
    
#ifdef __cplusplus
}
#endif

#include "Common.h"
#include "CommonHashTable.h"

//--------------------------------------------------------------------

#define FLT_VOP_UNKNOWN_OFFSET ((vm_offset_t)(-1))

//--------------------------------------------------------------------

typedef enum _FltVopEnum{
    FltVopEnum_Unknown = 0x0,
    
    FltVopEnum_access,
    FltVopEnum_advlock,
    FltVopEnum_allocate,
    FltVopEnum_blktooff,
    FltVopEnum_blockmap,
    FltVopEnum_bwrite,
    FltVopEnum_close,
    FltVopEnum_copyfile,
    FltVopEnum_create,
    FltVopEnum_default,
    FltVopEnum_exchange,
    FltVopEnum_fsync,
    FltVopEnum_getattr,
    FltVopEnum_getxattr,
    FltVopEnum_inactive,
    FltVopEnum_ioctl,
    FltVopEnum_link,
    FltVopEnum_listxattr,
    FltVopEnum_lookup,
    FltVopEnum_kqfilt_add,
    FltVopEnum_kqfilt_remove,
    FltVopEnum_mkdir,
    FltVopEnum_mknod,
    FltVopEnum_mmap,
    FltVopEnum_mnomap,
    FltVopEnum_offtoblock,
    FltVopEnum_open,
    FltVopEnum_pagein,
    FltVopEnum_pageout,
    FltVopEnum_pathconf,
    FltVopEnum_read,
    FltVopEnum_readdir,
    FltVopEnum_readdirattr,
    FltVopEnum_readlink,
    FltVopEnum_reclaim,
    FltVopEnum_remove,
    FltVopEnum_removexattr,
    FltVopEnum_rename,
    FltVopEnum_revoke,
    FltVopEnum_rmdir,
    FltVopEnum_searchfs,
    FltVopEnum_select,
    FltVopEnum_setattr,
    FltVopEnum_setxattr,
    FltVopEnum_strategy,
    FltVopEnum_symlink,
    FltVopEnum_whiteout,
    FltVopEnum_write,
    FltVopEnum_getnamedstreamHook,
    FltVopEnum_makenamedstreamHook,
    FltVopEnum_removenamedstreamHook,
    
    FltVopEnum_Max
} FltVopEnum;

//--------------------------------------------------------------------

class FltVnodeHookEntry: public OSObject
{
    
    OSDeclareDefaultStructors( FltVnodeHookEntry )
    
#if defined( DBG )
    friend class FltVnodeHooksHashTable;
#endif//DBG
    
private:
    
    //
    // the number of vnodes which we are aware of for this v_op vector
    //
    SInt32 vNodeCounter;
    
    //
    // the value is used to mark the origVop's entry as
    // corresponding to not hooked function ( i.e. skipped deliberately )
    //
    static VOPFUNC  vopNotHooked;
    
    //
    // an original functions array,
    // for not present functons the values are set to NULL( notional assertion,
    // it was niether checked no taken into account by the code ),
    // for functions which hooking was skipped deliberately the
    // value is set to vopNotHooked
    //
    VOPFUNC  origVop[ FltVopEnum_Max ];
    
#if defined( DBG )
    bool   inHash;
#endif
    
protected:
    
    virtual bool init();
    virtual void free();
    
public:
    
    //
    // allocates the new entry
    //
    static FltVnodeHookEntry* newEntry()
    {
        FltVnodeHookEntry* entry;
        
        assert( preemption_enabled() );
        
        entry = new FltVnodeHookEntry();
        assert( entry ) ;
        if( !entry )
            return NULL;
        
        //
        // the init is very simple and must alvays succeed
        //
        entry->init();
        
        return entry;
    }
    
    
    VOPFUNC
    getOrignalVop( __in FltVopEnum   indx ){
        
        assert( indx < FltVopEnum_Max );
        return this->origVop[ (int)indx ];
    }
    
    void
    setOriginalVop( __in FltVopEnum   indx, __in VOPFUNC orig ){
        
        assert( indx < FltVopEnum_Max );
        assert( NULL == this->origVop[ (int)indx ] );
        
        this->origVop[ (int)indx ] = orig;
    }
    
    void
    setOriginalVopAsNotHooked( __in FltVopEnum   indx ){
        
        this->setOriginalVop( indx, this->vopNotHooked );
    }
    
    bool
    isHooked( __in FltVopEnum indx ){
        
        //
        // NULL is invalid, vopNotHooked means not hooked deliberately
        //
        return ( this->vopNotHooked != this->origVop[ (int)indx ] );
    }
    
    //
    // returns te value before the increment
    //
    SInt32
    incrementVnodeCounter(){
        
        assert( this->vNodeCounter < 0x80000000 );
        return OSIncrementAtomic( &this->vNodeCounter );
    }
    
    //
    // returns te value before the decrement
    //
    SInt32
    decrementVnodeCounter(){
        
        assert( this->vNodeCounter > 0x0 && this->vNodeCounter < 0x80000000);
        return OSDecrementAtomic( &this->vNodeCounter );
    }
    
    SInt32
    getVnodeCounter(){
        
        return this->vNodeCounter;
    }
    
};

//--------------------------------------------------------------------

class FltVnodeHooksHashTable
{
    
private:
    
    ght_hash_table_t*  HashTable;
    IORWLock*          RWLock;
    
#if defined(DBG)
    thread_t           ExclusiveThread;
#endif//DBG
    
    //
    // returns an allocated hash table object
    //
    static FltVnodeHooksHashTable* withSize( int size, bool non_block );
    
    //
    // free must be called before the hash table object is deleted
    //
    void free();
    
    //
    // as usual for IOKit the desctructor and constructor do nothing
    // as it is impossible to return an error from the constructor
    // in the kernel mode
    //
    FltVnodeHooksHashTable()
    {
        
        this->HashTable = NULL;
        this->RWLock = NULL;
#if defined(DBG)
        this->ExclusiveThread = NULL;
#endif//DBG
        
    }
    
    //
    // the destructor checks that the free() has been called
    //
    ~FltVnodeHooksHashTable()
    {
        
        assert( !this->HashTable && !this->RWLock );
    };
    
public:
    
    static bool CreateStaticTableWithSize( int size, bool non_block );
    static void DeleteStaticTable();
    
    //
    // adds an entry to the hash table, the entry is referenced so the caller must
    // dereference the entry if it has been referenced
    //
    bool   AddEntry( __in VOPFUNC* v_op, __in FltVnodeHookEntry* entry );
    
    //
    // removes the entry from the hash and returns the removed entry, NULL if there
    // is no entry for an object, the returned entry is referenced
    //
    FltVnodeHookEntry*   RemoveEntry( __in VOPFUNC* v_op );
    
    //
    // returns an entry from the hash table, the returned entry is referenced
    // if the refrence's value is "true"
    //
    FltVnodeHookEntry*   RetrieveEntry( __in VOPFUNC* v_op, __in bool reference = true );
    
    
    void
    LockShared()
    {   assert( this->RWLock );
        assert( preemption_enabled() );
        
        IORWLockRead( this->RWLock );
    };
    
    
    void
    UnLockShared()
    {   assert( this->RWLock );
        assert( preemption_enabled() );
        
        IORWLockUnlock( this->RWLock );
    };
    
    
    void
    LockExclusive()
    {
        assert( this->RWLock );
        assert( preemption_enabled() );
        
#if defined(DBG)
        assert( current_thread() != this->ExclusiveThread );
#endif//DBG
        
        IORWLockWrite( this->RWLock );
        
#if defined(DBG)
        assert( NULL == this->ExclusiveThread );
        this->ExclusiveThread = current_thread();
#endif//DBG
        
    };
    
    
    void
    UnLockExclusive()
    {
        assert( this->RWLock );
        assert( preemption_enabled() );
        
#if defined(DBG)
        assert( current_thread() == this->ExclusiveThread );
        this->ExclusiveThread = NULL;
#endif//DBG
        
        IORWLockUnlock( this->RWLock );
    };
    
    static FltVnodeHooksHashTable* sVnodeHooksHashTable;
};

//--------------------------------------------------------------------

//
// success doen't mean that a vnode's operations table has
// been hooked, it can be skipped as ia not interested for us
//
extern
IOReturn
FltHookVnodeVop(
                __inout vnode_t vnode,
                __inout bool* isVopHooked
                );

extern
VOPFUNC
VifGetOriginalVnodeOp(
                      __in vnode_t      vnode,
                      __in FltVopEnum   indx
                      );

extern
void
FltUnHookVnodeVop(
                  __inout vnode_t vnode
                  );

//--------------------------------------------------------------------

#endif /* defined(__VFSFilter0__VNodeHook__) */
