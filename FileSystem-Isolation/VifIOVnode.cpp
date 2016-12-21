/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#include <sys/proc.h> // for proc_name
#include "VifIOVnode.h"
//#include "IOBSDSystem.h"
#include "Kauth.h"

//--------------------------------------------------------------------

#define super OSObject

OSDefineMetaClassAndStructors( VifIOVnode, OSObject )

#if defined( DBG )
SInt32     VifIOVnode::VifIOVnodesCount = 0x0;
#endif//DBG

OSString*        VifIOVnode::EmptyName = NULL;
const OSSymbol*  VifIOVnode::UnknownProcessName = NULL;

//--------------------------------------------------------------------

VifIOVnode*
VifIOVnode::withBSDVnode( __in vnode_t vnode )
/*
 the function must not assume that it won't be called 
 multiple times for the same vnode, and each time it must
 return a new IOVnode, this is a caller's responsibility
 to preserve only one and destroy all others
 
 an example of a stack for this function calling
 #3  0x46569dc4 in VifIOVnode::withBSDVnode (vnode=0x8357314) at /work/DeviceLockProject/DeviceLockIOKitDriver/VifIOVnode.cpp:94
 #4  0x4656b220 in VifVnodeHashTable::CreateAndAddIOVnodByBSDVnode (this=0x8375800, vnode=0x8357314) at /work/DeviceLockProject/DeviceLockIOKitDriver/VifVnodeHashTable.cpp:304
 #5  0x4652602d in VifIOKitKAuthVnodeGate::VifVnodeAuthorizeCallback (credential=0x5656ea4, idata=0x6acc520, action=4, arg0=115219732, arg1=137720596, arg2=0, arg3=834583448) at /work/DeviceLockProject/DeviceLockIOKitDriver/VifKAuthVnodeGate.cpp:120
 #6  0x00508a71 in kauth_authorize_action (scope=0x56a2804, credential=0x5656ea4, action=4, arg0=115219732, arg1=137720596, arg2=0, arg3=834583448) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/kern/kern_authorization.c:420
 #7  0x003375e7 in vnode_authorize (vp=0x8357314, dvp=0x0, action=4, ctx=0x6de1d14) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/vfs/vfs_subr.c:4863
 #8  0x0034e092 in vn_open_auth (ndp=0x31bebcf0, fmodep=0x31bebc74, vap=0x31bebe44) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/vfs/vfs_vnops.c:407
 #9  0x0033f4a3 in open1 (ctx=0x6de1d14, ndp=0x31bebcf0, uflags=1537, vap=0x31bebe44, retval=0x6de1c54) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/vfs/vfs_syscalls.c:2717
 #10 0x0033fd31 in open_nocancel (p=0x63a97e0, uap=0x6c669a8, retval=0x6de1c54) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/vfs/vfs_syscalls.c:2921
 #11 0x0033fbf8 in open (p=0x63a97e0, uap=0x6c669a8, retval=0x6de1c54) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/vfs/vfs_syscalls.c:2903
 #12 0x005b19b5 in unix_syscall64 (state=0x6c669a4) at /work/Mac_OS_X_kernel/10_6_4/xnu-1504.7.4/bsd/dev/i386/systemcalls.c:365
 */
{
    VifIOVnode* newIOVnode;
    
    newIOVnode = new VifIOVnode();
    assert( newIOVnode );
    if( !newIOVnode )
        return NULL;
    
    if( !newIOVnode->init() ){
        
        assert( !"newIOVnode->init() failed" );
        
        newIOVnode->release();
        return NULL;
        
    }
    
    //
    // set the data for a new object
    //
    
    newIOVnode->vnode   = vnode;
    newIOVnode->v_type  = vnode_vtype( vnode );
    newIOVnode->vnodeID = (UInt64)newIOVnode;
    
    //
    // check for a direct disk open, there are two cases - the first is when
    // the vnode's type is VBLK or VCHR - this is a case when /dev/(r)diskXX is opened,
    // the second case is not a direct disk open - when a directory where a disk is mounted is opened -
    // i.e. /Volume/SanDisk ( i.e. w/o the last / ) - in that case the vnode->v_name is NULL,
    // but it is not acessibe through KPI, so we make use of a comparision for
    // a directory name where a disk is mounted and a full vnode's path, if they
    // are equal then this is a FS root open, for the second case the type for
    // a vnode is VDIR and requests are processed as requests to a directory ( FS root )
    //
    assert( vnode_mount( vnode ) );
    assert( VNON != newIOVnode->v_type && NULL != newIOVnode->nameStr );
    
    if( VBLK == newIOVnode->v_type || VCHR == newIOVnode->v_type ){
        
        //
        // a direct open through /dev/diskXXX
        //
        newIOVnode->flags.directDiskOpen = 0x1;
        
    }
    
    if( vnode_issystem( vnode ) )
        newIOVnode->flags.system = 0x1;
        
    if( vnode_islnk( vnode ) )
        newIOVnode->flags.link = 0x1;
        
#ifndef VIF_MACOSX_10_5// vnode_isswap() declared but not exported for 10.5
    if( vnode_isswap( vnode ) )
        newIOVnode->flags.swap = 0x1;
#endif//VIF_MACOSX_10_5
    
    if( vnode_isdir( vnode ) )
        newIOVnode->flags.dir = 0x1;
    
#if defined( DBG )
    OSIncrementAtomic( &VifIOVnode::VifIOVnodesCount );
#endif//DBG
    
    return newIOVnode;
}

//--------------------------------------------------------------------

void
VifIOVnode::logVnodeOperation( __in VifIOVnode::VnodeOperation op )
{
#if DBG
    
    assert( kVnodeOp_Unknown != op );
    
    for( int i = 0x0; i < VIF_STATIC_ARRAY_SIZE(this->vnodeOperationsLog); ++i ){
        
        if( kVnodeOp_Unknown != this->vnodeOperationsLog[ i ] )
            continue;
        
        if( OSCompareAndSwap( kVnodeOp_Unknown, op, &this->vnodeOperationsLog[ i ] ) )
           break;
           
    }// end for

#endif//DBG

}

//--------------------------------------------------------------------

OSString*
VifIOVnode::getNameRef()
{
    OSString* nameRef;
    
    assert( preemption_enabled() );
    
    this->LockShared();
    {// start of the lock
        
        assert( this->nameStr );
        
        nameRef = this->nameStr;
        nameRef->retain();
        
    }// end of the lock
    this->UnLockShared();
    
    return nameRef;
}

//--------------------------------------------------------------------

bool
VifIOVnode::isNameVaid()
{
    return ( this->nameStr != VifIOVnode::EmptyName );
}

//--------------------------------------------------------------------

void
VifIOVnode::setName( __in OSString* name )
{
    OSString* oldName;
    
    assert( name );
    assert( preemption_enabled() );
    
    this->LockExclusive();
    {// start of the lock
        
        assert( this->nameStr );
        
        oldName = this->nameStr;
        this->nameStr = name;
        
    }// end of the lock
    this->UnLockExclusive();
    
    assert( oldName );
    oldName->release();
    
    return;
}

//--------------------------------------------------------------------

void
VifIOVnode::updateNameToValid()
{
    
    assert( preemption_enabled() );
    assert( this->nameStr );
    
    if( this->nameStr != VifIOVnode::EmptyName ){
        
        //
        // already has been updated, this provides an idempotent behaviour!
        //
        return;
        
    }// end if( this->nameStr != VifIOVnode::EmptyName )
    
    //
    // get a vnode's path, if called before the KAUTH callback
    // the vnode might not have a valid name and parent,
    // the name amust be reinitialized when called from the KAUTH
    // subsystem
    //
    errno_t     error;
    OSString*   newName;
    
    assert( this->vnode );
    
    int    nameLength = MAXPATHLEN;
    char*  nameBuffer = (char*)IOMalloc( MAXPATHLEN );
    assert( nameBuffer );
    if( ! nameBuffer )
        return;
    
    error = vn_getpath( this->vnode, nameBuffer, &nameLength );
    if( 0x0 != error  ){
        
        DBG_PRINT_ERROR( ("vn_getpath() failed with the 0x%X error\n", error ) );
        
        /*
         
         in some cases the system provides us with a vnode w/o parent, so FQN can not be assembled by vn_getpath
         
         #0  VifIOVnode::updateNameToValid (this=0xffffff80254a7500) at VifIOVnode.cpp:228
         #1  0xffffff7f81fcf0e5 in VifIOKitKAuthVnodeGate::VifVnodeAuthorizeCallback (credential=0xffffff8011d307f0, idata=0xffffff8024fe0c00, action=-2147483548, arg0=18446743524329052896, arg1=18446743524429184152, arg2=0, arg3=18446743527994768164) at VifKAuthVnodeGate.cpp:348
         #2  0xffffff800053fdb1 in kauth_authorize_action (scope=0xffffff8011f73008, credential=0xffffff8011d307f0, action=-2147483548, arg0=18446743524329052896, arg1=18446743524429184152, arg2=0, arg3=18446743527994768164) at /SourceCache/xnu/xnu-2050.22.13/bsd/kern/kern_authorization.c:422
         #3  0xffffff80002f44cb in vnode_authorize (vp=0xffffff801c56bc98, dvp=0x0, action=-2147483548, ctx=0xffffff80165edae0) at /SourceCache/xnu/xnu-2050.22.13/bsd/vfs/vfs_subr.c:5730
         #4  0xffffff80004e626c in hfs_real_user_access [inlined] () at :1031
         #5  0xffffff80004e626c in packcommonattr [inlined] () at :744
         #6  0xffffff80004e626c in hfs_packattrblk (abp=0xffffff80f0dd3da0, hfsmp=0xffffff801539c808, vp=<value temporarily unavailable, due to optimizations>, descp=0xffffff801c570df0, attrp=0xffffff801c570e08, datafork=0xffffff80f0dd3d38, rsrcfork=0xffffff8011d307f0, ctx=<value temporarily unavailable, due to optimizations>) at /SourceCache/xnu/xnu-2050.22.13/bsd/hfs/hfs_attrlist.c:435
         #7  0xffffff80004e582e in hfs_vnop_readdirattr (ap=0xffffff80f0dd3e18) at /SourceCache/xnu/xnu-2050.22.13/bsd/hfs/hfs_attrlist.c:308
         #8  0xffffff80003123d6 in VNOP_READDIRATTR (vp=<value temporarily unavailable, due to optimizations>, alist=<value temporarily unavailable, due to optimizations>, uio=<value temporarily unavailable, due to optimizations>, maxcount=<value temporarily unavailable, due to optimizations>, options=<value temporarily unavailable, due to optimizations>, newstate=<value temporarily unavailable, due to optimizations>, eofflag=0xffffff80f0dd3eb4, actualcount=0xffffff80f0dd3ebc, ctx=0xffffff80165edae0) at /SourceCache/xnu/xnu-2050.22.13/bsd/vfs/kpi_vfs.c:5416
         #9  0xffffff800030122a in getdirentriesattr (p=<value temporarily unavailable, due to optimizations>, uap=0xffffff8015d4ce54, retval=0xffffff80165ed9c8) at /SourceCache/xnu/xnu-2050.22.13/bsd/vfs/vfs_syscalls.c:7269
         #10 0xffffff80005e063a in unix_syscall64 (state=0xffffff8015d4ce50) at /SourceCache/xnu/xnu-2050.22.13/bsd/dev/i386/systemcalls.c:384
         
         (gdb) p *(vnode_t)0xffffff801c56bc98
         $2 = {
         v_lock = {
         opaque = {0, 18446744069414584320}
         }, 
         v_freelist = {
         tqe_next = 0x0, 
         tqe_prev = 0xdeadb
         }, 
         v_mntvnodes = {
         tqe_next = 0xffffff801c56bf80, 
         tqe_prev = 0xffffff801c56bbc0
         }, 
         v_nclinks = {
         lh_first = 0x0
         }, 
         v_ncchildren = {
         lh_first = 0x0
         }, 
         v_defer_reclaimlist = 0x0, 
         v_listflag = 0, 
         v_flag = 542720, 
         v_lflag = 49152, 
         v_iterblkflags = 0 '\0', 
         v_references = 0 '\0', 
         v_kusecount = 0, 
         v_usecount = 0, 
         v_iocount = 1, 
         v_owner = 0x0, 
         v_type = 2, 
         v_tag = 16, 
         v_id = 564813592, 
         v_un = {
         vu_mountedhere = 0x0, 
         vu_socket = 0x0, 
         vu_specinfo = 0x0, 
         vu_fifoinfo = 0x0, 
         vu_ubcinfo = 0x0
         }, 
         v_cleanblkhd = {
         lh_first = 0x0
         }, 
         v_dirtyblkhd = {
         lh_first = 0x0
         }, 
         v_knotes = {
         slh_first = 0x0
         }, 
         v_cred = 0xffffff8011d307f0, 
         v_authorized_actions = -2147483548, 
         v_cred_timestamp = 0, 
         v_nc_generation = 2, 
         v_numoutput = 0, 
         v_writecount = 0, 
         v_name = 0xffffff80124713a8 "Library",  <-- not a FQN path
         v_parent = 0x0,                         <-- a NULL paren
         v_lockf = 0x0, 
         v_reserved1 = 0, 
         v_reserved2 = 0, 
         v_op = 0xffffff8011f70a08, 
         v_mount = 0xffffff8011f78000, 
         v_data = 0xffffff801c570d80, 
         v_label = 0x0, 
         v_resolve = 0x0
         }
         
         */
        
        const char* shortName = vnode_getname( this->vnode );
        if( shortName ){
            
            nameLength = min( strlen( shortName ), /*sizeof( stackBuffer )*/MAXPATHLEN - 1 );
            strncpy( nameBuffer, shortName, nameLength );
            nameBuffer[ nameLength ] = '\0';
            
            vnode_putname( shortName );
            shortName = NULL;
            
        } else {
            
            nameBuffer[ 0 ] = '\0';
            nameLength = 0x1;
        }
        
    }// end if( 0x0 != error  )
    
    newName = OSString::withCString( nameBuffer );
    assert( newName );
    if( !newName ){
        
        DBG_PRINT_ERROR(("OSString::withString( nameBuffer ) failed"));
        goto __exit;
        
    }// end if( !newName )
    
    //
    // check for the roor, as we now have a valid name
    //
    if( VDIR == this->v_type &&
        strlen( newName->getCStringNoCopy() ) == strlen( vfs_statfs( vnode_mount( this->vnode ) )->f_mntonname ) &&
        0x0 == strcasecmp ( newName->getCStringNoCopy(), vfs_statfs( vnode_mount( this->vnode ) )->f_mntonname  ) ){
        
        //
        // so this is a FS root opening through a mounting DIR
        //
        this->flags.fsRoot = 0x1;
    }
    
    this->setName( newName );
    
__exit:
    IOFree( nameBuffer, MAXPATHLEN );
}

//--------------------------------------------------------------------

bool
VifIOVnode::init()
{
#if DBG
    this->listEntry.cqe_next = this->listEntry.cqe_prev = NULL;
#endif//DBG
    
    if( !super::init() )
        return false;
    
    this->RecursiveLock = IORecursiveLockAlloc();
    assert( this->RecursiveLock );
    if( !this->RecursiveLock ){
        
        DBG_PRINT_ERROR(("IORecursiveLockAlloc() failed"));
        return false;
    }// end if( !this->RecursiveLock )
    
    this->spinLock = IOSimpleLockAlloc();
    assert( this->spinLock );
    if( !this->spinLock ){
        
        DBG_PRINT_ERROR(("IOLockAlloc() failed\n"));
        return false;
    }
    
    //
    // set a default invalid name, the correct name will be set later
    // when the first KAUTH callback is invoked - at that time
    // the BSD subsystem initialized the name and the parent vnode
    //
    assert( VifIOVnode::EmptyName );
    this->nameStr = VifIOVnode::EmptyName;
    this->nameStr->retain();
    
    assert( VifIOVnode::UnknownProcessName );
    
    this->auditData.process.processName = VifIOVnode::UnknownProcessName;
    this->auditData.process.processName->retain(); // bump a reference to retain the placeholder
    
    this->auditData.process.pid = (-1);
    this->auditData.userID.uid  = (-1);
    
#ifdef _VIF_MACOSX_VFS_ISOLATION
    InitializeListHead( &this->cawlWaitListHead );
#endif // _VIF_MACOSX_VFS_ISOLATION
    
    return true;
}

//--------------------------------------------------------------------

void
VifIOVnode::free()
{
    assert( (NULL == this->listEntry.cqe_next) && (NULL == this->listEntry.cqe_prev) );
    
    if( this->nameStr )
        this->nameStr->release();
    
    if( this->spinLock )
        IOSimpleLockFree( this->spinLock );
    
    if( this->RecursiveLock )
        IORecursiveLockFree( this->RecursiveLock );
    
    if( this->auditData.process.processName )
        this->auditData.process.processName->release();
    
#ifdef _VIF_MACOSX_VFS_ISOLATION
    if( this->coveredVnode ){
        
        //
        // remove the underlying vnode
        //
        if( kVnodeType_CoveringFSD == this->dldVnodeType ){
            
            //
            // allow the node to be reuses immediately by calling vnode_recycle
            // which triggers 
            // vnode_reclaim_internal()->vgone()->vclean()->VNOP_RECLAIM()
            // when iocounts and userio count drop to zero,
            // the reclaim hook removes vnode from the hash table so there
            // is no need to do this here
            //
            assert( !vnode_isinuse( this->coveredVnode, 0x1 ) );
            vnode_recycle( this->coveredVnode );
            vnode_rele( this->coveredVnode );
            
            this->coveredVnode = NULL;
        }
        
    }// end if( this->coveredVnode )
    
    assert( IsListEmpty( &this->cawlWaitListHead ) );
    
    if( this->sparseFile ){
        
        assert( VifSparseFilesHashTable::sSparseFilesHashTable );
        //
        // emulate the reclaim
        //
        if( 0x0 == this->flags.reclaimed )
            this->sparseFile->decrementUsersCount();
        //VifSparseFilesHashTable::sSparseFilesHashTable->RemoveEntryByObject( this->sparseFile ); done by decrementUsersCount()
        
        this->sparseFile->exchangeIsolationRelatedVnode( NULL );
        this->sparseFile->release();
    }
#endif//#ifdef _VIF_MACOSX_VFS_ISOLATION
    
    super::free();
    
#if defined( DBG )
    assert( 0x0 != VifIOVnode::VifIOVnodesCount );
    OSDecrementAtomic( &VifIOVnode::VifIOVnodesCount );
#endif//DBG
    
}

//--------------------------------------------------------------------

const char*
VifIOVnode::vnodeTypeCStrNoCopy()
{

    switch( this->v_type){
        case VNON:
            return "VNON";
        case VREG:
            return "VREG";
        case VDIR:
            return "VDIR";
        case VBLK:
            return "VBLK";
        case VCHR:
            return "VCHR";
        case VLNK:
            return "VLNK";
        case VSOCK:
            return "VFIFO";
        case VFIFO:
            return "VFIFO";
        case VBAD:
            return "VBAD";
        case VSTR:
            return "VSTR";
        case VCPLX:
            return "VCPLX";
        default:
            return "U";
    }
}

//--------------------------------------------------------------------

void
VifIOVnode::LockShared()
{   assert( this->RecursiveLock );
    assert( preemption_enabled() );
    
    IORecursiveLockLock( this->RecursiveLock );
};


void
VifIOVnode::UnLockShared()
{   assert( this->RecursiveLock );
    assert( preemption_enabled() );
    
    IORecursiveLockUnlock( this->RecursiveLock );
};


void
VifIOVnode::LockExclusive()
{
    assert( this->RecursiveLock );
    assert( preemption_enabled() );
    
    IORecursiveLockLock( this->RecursiveLock );
    
#if defined(DBG)
    this->ExclusiveThread = current_thread();
    this->exclusiveCounter += 0x1;
#endif//DBG
    
};

//--------------------------------------------------------------------

void
VifIOVnode::UnLockExclusive()
{
    assert( this->RecursiveLock );
    assert( preemption_enabled() );
    
#if defined(DBG)
    assert( current_thread() == this->ExclusiveThread );
    this->exclusiveCounter -= 0x1;
    if( 0x0 == this->exclusiveCounter )
        this->ExclusiveThread = NULL;
#endif//DBG
    
    IORecursiveLockUnlock( this->RecursiveLock );
};

//--------------------------------------------------------------------

//
// called once at the driver initialization
//
bool VifIOVnode::InitVnodeSubsystem()
{
    assert( NULL == VifIOVnode::EmptyName );
    
    VifIOVnode::EmptyName = OSString::withCString("empty name");
    assert( VifIOVnode::EmptyName );
    if( !VifIOVnode::EmptyName ){
        
        DBG_PRINT_ERROR(("OSString::withString() failed"));
        return false;
    }
    
    VifIOVnode::UnknownProcessName = OSSymbol::withCString( "Unknown" );
    assert( VifIOVnode::UnknownProcessName );
    if( !VifIOVnode::UnknownProcessName ){
        
        DBG_PRINT_ERROR(("OSSymbol::withString() failed"));
        return false;
    }
    
    return true;
}

//--------------------------------------------------------------------

//
// called at driver unload ( which is never happened )
//
void VifIOVnode::UninitVnodeSubsystem()
{
    if( VifIOVnode::EmptyName )
        VifIOVnode::EmptyName->release();
    
    if( VifIOVnode::UnknownProcessName )
        VifIOVnode::UnknownProcessName->release();
}

//--------------------------------------------------------------------

bool
TestHarness_IsControlledByIsolationFilter( __in const char* pathname )
{
    const static char* TestPathPrefix = "/Volumes/Untitled/";
    
    return( 0 == strncmp( TestPathPrefix, pathname, strlen(TestPathPrefix) ) );
}

void VifIOVnode::defineStatusForIsolation( __in vfs_context_t vfsContext, __in_opt const char* namepath )
{
    assert( preemption_enabled() );
    assert( vfsContext );
    
    //
    // the flag is set once and never removed
    //
    if( 0x1 == this->flags.isolationOn )
        return;
    
    //
    // TO DO infer the vnode isolation status here
    //
    OSString*    nameRef = NULL;
    const char*  fullPath = namepath;
    if( !fullPath ){
        
        nameRef = this->getNameRef();
        if( !nameRef )
            return;
        
        fullPath = nameRef->getCStringNoCopy();
    }
    
    if( TestHarness_IsControlledByIsolationFilter( fullPath ) )
        this->flags.isolationOn = 1;
    
    if( nameRef )
        nameRef->release();
    
}

//--------------------------------------------------------------------

vnode_t
VifIOVnode::getReferencedVnode()
{
    vnode_t    vnode = NULL;
    bool       reclaimed;
    
    assert( preemption_enabled() );
    
    if( 0x1 == this->flags.reclaimed )
        return NULL;
    
    //
    // vnode_getwithref() can block so must not be called with
    // any lock being held
    //
    
    OSIncrementAtomic( &this->delayReclaimCount );
    
    IOSimpleLockLock( this->spinLock );
    { // start of the lock
        
        reclaimed = ( 0x1 == this->flags.reclaimed );
        
    } // end of the lock
    IOSimpleLockUnlock( this->spinLock );
    
    if( !reclaimed && KERN_SUCCESS == vnode_getwithref( this->vnode ) )
        vnode = this->vnode;
    
    OSDecrementAtomic( &this->delayReclaimCount );
    
    //
    // allow the reclaim to continue
    //
    thread_wakeup( &this->delayReclaimCount );
    
    return vnode;
}

//--------------------------------------------------------------------

void
VifIOVnode::prepareForReclaiming()
{
    bool wait = false;
    
    assert( preemption_enabled() );
    
    IOSimpleLockLock( this->spinLock );
    { // start of the lock
        
        assert( 0x0 == this->flags.reclaimed );
        this->flags.reclaimed = 0x1;
        
        if( 0x0 != this->delayReclaimCount )
            wait = ( THREAD_WAITING == assert_wait( &this->delayReclaimCount, THREAD_UNINT ) );
        
    } // end of the lock
    IOSimpleLockUnlock( this->spinLock );
    
    //
    // wait until all callers of getReferencedVnode() returns,
    // this is required as VifIOVnode might outlive the underlying
    // BSD vnode so we need to provide a guarantee that the users
    // of VifIOVnode will not hit a reclaimed or reused vnode
    //
    while( wait ){
        
        thread_block( THREAD_CONTINUE_NULL );
        
        IOSimpleLockLock( this->spinLock );
        { // start of the lock
            
            if( 0x0 != this->delayReclaimCount )
                wait = ( THREAD_WAITING == assert_wait( &this->delayReclaimCount, THREAD_UNINT ) );
            else
                wait = false;
            
        } // end of the lock
        IOSimpleLockUnlock( this->spinLock );
        
    } // end while
}

//--------------------------------------------------------------------

void
VifIOVnode::setUserID( __in kauth_cred_t credential )
{
    if( ! OSCompareAndSwap( (-1), kauth_cred_getuid( credential ), &this->auditData.userID.uid ) ) {
        
        return;
    }
    
    //
    // a user's GUID,
    // kauth_cred_getguid can return an error, disregard it
    //
    kauth_cred_getguid( credential, &this->auditData.userID.guid );
    this->auditData.userID.gid = kauth_cred_getgid( credential );
}

//--------------------------------------------------------------------

void
VifIOVnode::setProcess( __in pid_t _pid )
{    
    assert( (-1) != _pid );
    
    if( ! OSCompareAndSwap( (-1), _pid, &this->auditData.process.pid ) ){
        
        //
        // we were not the first
        //
        return;
    }
       
    char p_comm[MAXCOMLEN + 1];
    
    bzero( p_comm, sizeof(p_comm) );
    proc_name( _pid, p_comm, sizeof( p_comm ) );
    
    const OSSymbol*  newName = OSSymbol::withCString( p_comm );
    assert( newName );
    if( ! newName )
        return;
    
    const OSSymbol* oldName = NULL;
    
    IOSimpleLockLock( this->spinLock );
    { // start of the lock
        
        //
        // set the new name, but do not forget to release the old one
        //
        oldName = this->auditData.process.processName;
        this->auditData.process.processName = newName;
        
    } // end of the lock
    IOSimpleLockUnlock( this->spinLock );
    
    if( oldName )
        oldName->release();
    
    assert( this->auditData.process.processName && (-1) != this->auditData.process.pid );
}

//--------------------------------------------------------------------

const OSSymbol*
VifIOVnode::getProcessAuditNameRef()
{
    const OSSymbol* processName;
    
    IOSimpleLockLock( this->spinLock );
    { // start of the lock
        
        //
        // process.processName must always contain a pointer to an object
        //
        processName = this->auditData.process.processName;
        assert( processName );
        if( NULL == processName )
            processName = VifIOVnode::UnknownProcessName;
        
        processName->retain();
        
    } // end of the lock
    IOSimpleLockUnlock( this->spinLock );
    
    return processName;
}

//--------------------------------------------------------------------

#ifdef _VIF_MACOSX_VFS_ISOLATION

VifSparseFile*
VifIOVnode::getSparseFileRef()
{
    VifSparseFile*    sparseFile;
    
    IOSimpleLockLock( this->spinLock );
    { // start of the lock
        
        sparseFile = this->sparseFile;
        if( sparseFile )
            sparseFile->retain();
            
    } // end of the lock
    IOSimpleLockUnlock( this->spinLock );
    
    return sparseFile;
}

#endif // _VIF_MACOSX_VFS_ISOLATION

//--------------------------------------------------------------------
