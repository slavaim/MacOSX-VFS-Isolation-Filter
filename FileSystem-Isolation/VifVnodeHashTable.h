/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#ifndef _VIFVNODEHASH_H
#define _VIFVNODEHASH_H

#include <libkern/c++/OSObject.h>
#include <IOKit/assert.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include "Common.h"
#include "VifIOVnode.h"


class VifVnodeHashTable;

//--------------------------------------------------------------------

class VifVnodeHashTableListHead{
    
    friend class VifVnodeHashTable;
    
private:
    
    VifVnodeHashTableListHead();
    ~VifVnodeHashTableListHead();
    
    void LockShared();
    void UnlockShared();
    
    void LockExclusive();
    void UnlockExclusive();
    
    //
    // returns a referenced object
    //
    VifIOVnode* RetrieveReferencedIOVnodByBSDVnode( __in vnode_t vnode );
    
    //
    // remove an object from the table if it exists and returns
    // a retained entry for the removed object
    //
    VifIOVnode* RemoveIOVnodeByBSDVnode( __in vnode_t vnode );
    
    //
    // adds an object to the table and references the object
    //
    void AddIOVnode( __in VifIOVnode* ioVnode );
    
    //
    // a head of the IOVnodes list
    //
    CIRCLEQ_HEAD( ListHead, VifIOVnode ) listHead;
    
    //
    // a lock to protect the list access
    //
    IORWLock*    rwLock;
    
#if defined( DBG )
    thread_t exclusiveThread;
#endif//DBG
    
};

//--------------------------------------------------------------------


class VifVnodeHashTable : public OSObject
{    
    OSDeclareDefaultStructors( VifVnodeHashTable )
    
private:
    
    //
    // it is better to choose the prime number which is not too close
    // to a power of 2
    //
    const static unsigned long VNODE_HASH_SIZE = 97;
    
    VifVnodeHashTableListHead   head[ VNODE_HASH_SIZE ];
    
    static unsigned long BSDVnodeToHashTableLine( __in vnode_t vnode )
    { return (((unsigned long)vnode)>>0x5)%VNODE_HASH_SIZE; }
    
    void LockLineSharedByBSDVnode( __in vnode_t vnode );
    void UnlockLineSharedByBSDVnode( __in vnode_t vnode );
    
    void LockLineExclusiveByBSDVnode( __in vnode_t vnode );
    void UnlockLineExclusiveByBSDVnode( __in vnode_t vnode );
    
    //
    // returns a referenced object
    //
    VifIOVnode* RetrieveReferencedIOVnodByBSDVnodeWOLock( __in vnode_t vnode );
    
    //
    // removes an object from the table if it exists and returns
    // a retained entry for the removed object
    //
    VifIOVnode* RemoveIOVnodeByBSDVnodeWOLock( __in vnode_t vnode );
    
    //
    // adds an object to the table and references the object
    //
    void AddIOVnodeWOLock( __in VifIOVnode* ioVnode );
    
public:
    
    //
    // the returned object is retained, the function is idempotent in its behaviour
    // but this behaviour should not be abused by overusing
    //
    VifIOVnode* CreateAndAddIOVnodByBSDVnode( __in vnode_t vnode,
                                             __in VifIOVnode::VnodeType vnodeType = VifIOVnode::kVnodeType_Native );
    
    //
    // returns a referenced object from the hash table or NULL if
    // the corresponding object doesn't exist
    //
    VifIOVnode* RetrieveReferencedIOVnodByBSDVnode( __in vnode_t vnode );
    
    //
    // removes the corresponding object from the hash table
    //
    void RemoveIOVnodeByBSDVnode( __in vnode_t vnode );
    
    static bool CreateStaticTable();
    static void DeleteStaticTable();
    
    static VifVnodeHashTable*      sVnodesHashTable;
};

//--------------------------------------------------------------------

#endif//_VIFVNODEHASH_H

