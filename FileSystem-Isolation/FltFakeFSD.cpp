/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

// If USE_FAKE_FSD is defined you need to
// add distorm disassembler files in the list of compiled files. Should you
// forget to compile distorm disassembler the driver load fails with the
// following error -
// kxld[com.SlavaImameev.FsdFilter]: The following symbols are unresolved for this kext:
// kxld[com.SlavaImameev.FsdFilter]: 	_distorm_decompose64

#include "Common.h"
#include "FltFakeFSD.h"
#include "VmPmap.h"

extern "C" {
#include "./distorm/distorm3/include/distorm.h"
#include "./distorm/distorm3/include/mnemonics.h"
}


static SInt32   gVnodeCount = 0x0;

//-----------------------------------------------------------------------------
//	FltFakeFsdMount -	This routine is responsible for mounting the filesystem
//					in the desired path
//-----------------------------------------------------------------------------

int
FltFakeFsdVfsMount(
    mount_t					mountPtr,
    vnode_t					blockDeviceVNodePtr,
    user_addr_t				data,
    vfs_context_t		    context )
{
    return EINVAL;
}

//-----------------------------------------------------------------------------
//	FltFakeFsdUnmount -	This routine is called to unmount the disc at the
//					specified mount point
//-----------------------------------------------------------------------------

int
FltFakeFsdVfsUnmount (
    mount_t					mountPtr,
    int						theFlags,
    vfs_context_t		    context )
{
    return EINVAL;
}

//-----------------------------------------------------------------------------
//	FltFakeRoot - This routine is called to get a vnode pointer to the root
//				vnode of the filesystem
//-----------------------------------------------------------------------------

int
FltFakeFsdVfsRoot (
    mount_t			mountPtr,
    vnode_t *		vNodeHandle,
    vfs_context_t	context )
{
    return EINVAL;
}

//-----------------------------------------------------------------------------
//	FltFakeFsdGetAttributes -	This routine is called to get filesystem attributes.
//-----------------------------------------------------------------------------

int
FltFakeFsdVfsGetAttributes (
    mount_t					mountPtr,
    struct vfs_attr *		attrPtr,
    vfs_context_t           context )
{
    return EINVAL;
}

//-----------------------------------------------------------------------------
//	FltFakeFsdVGet - This routine is responsible for getting the desired vnode.
//-----------------------------------------------------------------------------

int
FltFakeFsdVfsVGet(
    mount_t					mountPtr,
    ino64_t					ino,
    vnode_t *				vNodeHandle,
    vfs_context_t           context )
{
    return EINVAL;
}

//--------------------------------------------------------------------

static int
FltFakeFsdLookup(struct vnop_lookup_args *ap)
/*
 struct vnop_lookup_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_dvp;
 struct vnode **a_vpp;
 struct componentname *a_cnp;
 vfs_context_t a_context;
 } *ap)
 */
{
    return ENOENT;
}

//--------------------------------------------------------------------

static int
FltFakeFsdCreate(struct vnop_create_args *ap)
/*
 struct vnop_create_args {
struct vnodeop_desc *a_desc;
 struct vnode *a_dvp;
 struct vnode **a_vpp;
 struct componentname *a_cnp;
 struct vnode_attr *a_vap;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdWhiteout(struct vnop_whiteout_args *ap)
/*
 struct vnop_whiteout_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_dvp;
 struct componentname *a_cnp;
 int a_flags;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdMknod(struct vnop_mknod_args *ap)
/*
 struct vnop_mknod_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_dvp;
 struct vnode **a_vpp;
 struct componentname *a_cnp;
 struct vnode_attr *a_vap;
 vfs_context_t a_context;
 } *ap;
 */
{
	return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdOpen(struct vnop_open_args *ap)
/*
 struct vnop_open_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 int a_mode;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdAccess(struct vnop_access_args *ap)
/*
 struct vnop_access_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 int a_action;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdClose(struct vnop_close_args *ap)
/*
 struct vnop_close_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 int  a_fflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    return 0x0;
}

//--------------------------------------------------------------------

static int
FltFakeFsdInactive(struct vnop_inactive_args *ap)
/*
 struct vnop_inactive_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 vfs_context_t a_context;
 } *ap;
 */
/*
 not called in the current design as there is no user counts
*/
{
    bool reclaimed;
    
    assert( 0x0 != gVnodeCount );
    
    //
    // allow the node to reuse immediately by calling vnode_recycle
    // which calls 
    // vnode_reclaim_internal()->vgone()->vclean()->VNOP_RECLAIM()
    // if the reference count is 0x0 ( which is true for the inactive() call )
    //
    reclaimed = vnode_recycle(ap->a_vp);
    assert( reclaimed );
    
    //
    // the vnode is invalid here if it has been reclaimed
    //
    return 0x0;
}

//--------------------------------------------------------------------

static int
FltFakeFsdReclaim(struct vnop_reclaim_args *ap)
/*
 struct vnop_reclaim_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 vfs_context_t a_context;
 } *ap;
 */
/*
 called from the vnode_reclaim_internal() function
 vnode_reclaim_internal()->vgone()->vclean()->VNOP_RECLAIM()
*/
{
    assert( 0x0 != gVnodeCount );
    OSDecrementAtomic( &gVnodeCount );
    
    return 0x0;
}

//--------------------------------------------------------------------

static int
FltFakeFsdRead(struct vnop_read_args *ap)
/*
 struct vnop_read_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 struct uio *a_uio;
 int  a_ioflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdWrite(struct vnop_write_args *ap)
/*
 struct vnop_write_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 struct uio *a_uio;
 int  a_ioflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdPagein(struct vnop_pagein_args *ap)
/*
 struct vnop_pagein_args {
 struct vnodeop_desc *a_desc;
 struct vnode 	*a_vp,
 upl_t		a_pl,
 vm_offset_t	a_pl_offset,
 off_t		a_f_offset,
 size_t		a_size,
 int		a_flags
 vfs_context_t	a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeFsdPageout(struct vnop_pageout_args *ap)
/*
 struct vnop_pageout_args {
 struct vnodeop_desc *a_desc;
 struct vnode 	*a_vp,
 upl_t		a_pl,
 vm_offset_t	a_pl_offset,
 off_t		a_f_offset,
 size_t		a_size,
 int		a_flags
 vfs_context_t	a_context;
 } *ap;
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdStrategy(struct vnop_strategy_args *ap)
/*
 struct vnop_strategy_args {
 struct vnodeop_desc *a_desc;
 struct buf          *a_bp;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdAdvlock(struct vnop_advlock_args *ap)
/*
 struct vnop_advlock_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 caddr_t a_id;
 int a_op;
 struct flock *a_fl;
 int a_flags;
 vfs_context_t a_context;
 }; 
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdAllocate(struct vnop_allocate_args *ap)
/*
 struct vnop_allocate_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 off_t a_length;
 u_int32_t a_flags;
 off_t *a_bytesallocated;
 off_t a_offset;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdBlktooff(struct vnop_blktooff_args *ap)
/*
 struct vnop_blktooff_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 daddr64_t a_lblkno;
 off_t *a_offset;
 };
 */
{
    return ENOTSUP;
}


//--------------------------------------------------------------------

int
FltFakeFsdBlockmap(struct vnop_blockmap_args *ap)
/*
 struct vnop_blockmap_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 off_t                a_foffset;
 size_t               a_size;
 daddr64_t           *a_bpn;
 size_t              *a_run;
 void                *a_poff;
 int                  a_flags;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdBwrite(struct vnop_bwrite_args *ap)
/*
struct vnop_bwrite_args {
	struct vnodeop_desc *a_desc;
	buf_t a_bp;
}
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdCopyfile(struct vnop_copyfile_args *ap)
/*
struct vnop_copyfile_args {
	struct vnodeop_desc *a_desc;
	vnode_t a_fvp;
	vnode_t a_tdvp;
	vnode_t a_tvp;
	struct componentname *a_tcnp;
	int a_mode;
	int a_flags;
	vfs_context_t a_context;
}
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdExchange(struct vnop_exchange_args *ap)
/*
struct vnop_exchange_args {
	struct vnodeop_desc *a_desc;
	vnode_t a_fvp;
    vnode_t a_tvp;
	int a_options;
	vfs_context_t a_context;
};
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdFsync(struct vnop_fsync_args *ap)
/*
 struct vnop_fsync_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 int a_waitfor;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdGetattr(struct vnop_getattr_args *ap)
/*
struct vnop_getattr_args {
	struct vnodeop_desc *a_desc;
	vnode_t a_vp;
	struct vnode_attr *a_vap;
	vfs_context_t a_context;
};
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdGetxattr(struct vnop_getxattr_args *ap)
/*
 struct vnop_getxattr_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 const char * a_name;
 uio_t a_uio;
 size_t *a_size;
 int a_options;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdIoctl(struct vnop_ioctl_args *ap)
/*
struct vnop_ioctl_args {
	struct vnodeop_desc *a_desc;
	vnode_t a_vp;
	u_long a_command;
	caddr_t a_data;
	int a_fflag;
	vfs_context_t a_context;
};
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdLink(struct vnop_link_args *ap)
/*
 struct vnop_link_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_vp;
 vnode_t               a_tdvp;
 struct componentname *a_cnp;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdListxattr(struct vnop_listxattr_args *ap)
/*
 struct vnop_listxattr_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 uio_t                a_uio;
 size_t              *a_size;
 int                  a_options;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdKqfiltAdd(struct vnop_kqfilt_add_args *ap)
/*
 struct vnop_kqfilt_add_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_vp;
 struct knote         *a_kn;
 struct proc          *p;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdKqfiltRemove(__unused struct vnop_kqfilt_remove_args *ap)
/*
 struct vnop_kqfilt_remove_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_vp;
 uintptr_t             ident;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdMkdir(struct vnop_mkdir_args *ap)
/*
 struct vnop_mkdir_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_dvp;
 vnode_t              *a_vpp;
 struct componentname *a_cnp;
 struct vnode_attr    *a_vap;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdMmap(struct vnop_mmap_args *ap)
/*
 struct vnop_mmap_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 int                  a_fflags;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdMnomap(struct vnop_mnomap_args *ap)
/*
 struct vnop_mnomap_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

/*
 struct vnop_offtoblk_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 off_t                a_offset;
 daddr64_t           *a_lblkno;
 };
 */
int
FltFakeFsdOfftoblk(struct vnop_offtoblk_args *ap)
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdPathconf(struct vnop_pathconf_args *ap)
/*
 struct vnop_pathconf_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 int                  a_name;
 int                 *a_retval;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdReaddir(struct vnop_readdir_args *ap)
/*
 struct vnop_readdir_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 struct uio          *a_uio;
 int                  a_flags;
 int                 *a_eofflag;
 int                 *a_numdirent;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdReaddirattr(struct vnop_readdirattr_args *ap)
/*
 struct vnop_readdirattr_args {
 struct vnode *a_vp;
 struct attrlist *a_alist;
 struct uio *a_uio;
 u_long a_maxcount;
 u_long a_options;
 u_long *a_newstate;
 int *a_eofflag;
 u_long *a_actualcount;
 vfs_context_t a_context;
 };
*/
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdReadlink(struct vnop_readlink_args *ap)
/*
 struct vnop_readlink_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 struct uio          *a_uio;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdRemove(struct vnop_remove_args *ap)
/*
 struct vnop_remove_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_dvp;
 vnode_t               a_vp;
 struct componentname *a_cnp;
 int                   a_flags;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdRemovexattr(struct vnop_removexattr_args *ap)
/*
 struct vnop_removexattr_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 char                *a_name;
 int                  a_options;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdRevoke(struct vnop_revoke_args *ap)
/*
 *  struct vnop_revoke_args {
 *      struct vnodeop_desc  *a_desc;
 *      vnode_t               a_vp;
 *      int                   a_flags;
 *      vfs_context_t         a_context;
 *  };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

/*
 struct vnop_rename_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_fdvp;
 vnode_t               a_fvp;
 struct componentname *a_fcnp;
 vnode_t               a_tdvp;
 vnode_t               a_tvp;
 struct componentname *a_tcnp;
 vfs_context_t         a_context;
 };
 */
int
FltFakeFsdRename(struct vnop_rename_args *ap)
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

/*
 struct vnop_rmdir_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_dvp;
 vnode_t               a_vp;
 struct componentname *a_cnp;
 vfs_context_t         a_context;
 };
 */
int
FltFakeFsdRmdir(struct vnop_rmdir_args *ap)
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdSearch(struct vnop_searchfs_args *ap)
/*
 struct vnop_searchfs_args{
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 void *a_searchparams1;
 void *a_searchparams2;
 struct attrlist *a_searchattrs;
 u_long a_maxmatches;
 struct timeval *a_timelimit;
 struct attrlist *a_returnattrs;
 u_long *a_nummatches;
 u_long a_scriptcode;
 u_long a_options;
 struct uio *a_uio;
 struct searchstate *a_searchstate;
 vfs_context_t a_context;
 }
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

/*
 struct vnop_select_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 int                  a_which;
 int                  a_fflags;
 void                *a_wql;
 vfs_context_t        a_context;
 };
 */
int
FltFakeFsdSelect(__unused struct vnop_select_args *ap)
{
    return 1;// we are ready
}

//--------------------------------------------------------------------

/*
 struct vnop_setattr_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 struct vnode_attr   *a_vap;
 vfs_context_t        a_context;
 };
 */
int
FltFakeFsdSetattr(struct vnop_setattr_args *ap)
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int
FltFakeFsdSetxattr(struct vnop_setxattr_args *ap)
/*
 struct vnop_setxattr_args {
 struct vnodeop_desc *a_desc;
 vnode_t              a_vp;
 char                *a_name;
 uio_t                a_uio;
 int                  a_options;
 vfs_context_t        a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

int  
FltFakeFsdSymlink(struct vnop_symlink_args *ap)
/*
 struct vnop_symlink_args {
 struct vnodeop_desc  *a_desc;
 vnode_t               a_dvp;
 vnode_t              *a_vpp;
 struct componentname *a_cnp;
 struct vnode_attr    *a_vap;
 char                 *a_target;
 vfs_context_t         a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

#ifdef __APPLE_API_UNSTABLE
#if NAMEDSTREAMS

//--------------------------------------------------------------------

static int
FltFakeGetnamedstream(struct vnop_getnamedstream_args *ap)
/*
 struct vnop_getnamedstream_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 vnode_t *a_svpp;
 const char *a_name;
 enum nsoperation a_operation;
 int a_flags;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeMakenamedstream(struct vnop_makenamedstream_args *ap)
/*
 struct vnop_makenamedstream_args {
 struct vnodeop_desc *a_desc;
 vnode_t *a_svpp;
 vnode_t a_vp;
 const char *a_name;
 int a_flags;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

static int
FltFakeRemovenamedstream(struct vnop_removenamedstream_args *ap)
/*
 struct vnop_removenamedstream_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 vnode_t a_svp;
 const char *a_name;
 int a_flags;
 vfs_context_t a_context;
 };
 */
{
    return ENOTSUP;
}

//--------------------------------------------------------------------

#endif // __APPLE_API_UNSTABLE
#endif // NAMEDSTREAMS

//--------------------------------------------------------------------


/*
 * Global vfs data structures
 */

//
// the fake FSD's handle initialzed by the system when the FSD is registered
//
VOPFUNC* gFltFakeVnodeop = NULL;

//
// the gFltFakeVnodeopEntries  array must be synchronized with the gFltVnodeOpvOffsetDesc array,
// i.e. the arrays must contain the same number and types of entries excluding the terminating ones
// and the vnop_default_desc entry
//
struct vnodeopv_entry_desc gFltFakeVnodeopEntries[] = {
    
#ifndef _VIF_MACOSX_VFS_ISOLATION
    
    { &vnop_default_desc,  (VOPFUNC)vn_default_error },
	{ &vnop_lookup_desc,   (VOPFUNC)FltFakeFsdLookup },		        /* lookup */
	{ &vnop_create_desc,   (VOPFUNC)FltFakeFsdCreate },		        /* create */
	{ &vnop_whiteout_desc, (VOPFUNC)FltFakeFsdWhiteout },	        /* whiteout */
	{ &vnop_mknod_desc,    (VOPFUNC)FltFakeFsdMknod },		        /* mknod */
	{ &vnop_open_desc,     (VOPFUNC)FltFakeFsdOpen },		        /* open */
    { &vnop_close_desc,    (VOPFUNC)FltFakeFsdClose },		        /* close */
    { &vnop_access_desc,   (VOPFUNC)FltFakeFsdAccess },             /* access */
    { &vnop_inactive_desc, (VOPFUNC)FltFakeFsdInactive },	        /* inactive */
    { &vnop_reclaim_desc,  (VOPFUNC)FltFakeFsdReclaim },            /* reclaim */
	{ &vnop_read_desc,     (VOPFUNC)FltFakeFsdRead },		        /* read */
	{ &vnop_write_desc,    (VOPFUNC)FltFakeFsdWrite },		        /* write */
	{ &vnop_pagein_desc,   (VOPFUNC)FltFakeFsdPagein },		        /* Pagein */
	{ &vnop_pageout_desc,  (VOPFUNC)FltFakeFsdPageout },		    /* Pageout */
    { &vnop_strategy_desc, (VOPFUNC)FltFakeFsdStrategy },           /* strategy */
    { &vnop_mmap_desc,     (VOPFUNC)FltFakeFsdMmap },		        /* mmap */
    { &vnop_rename_desc,   (VOPFUNC)FltFakeFsdRename },             /* rename */
    { &vnop_pathconf_desc, (VOPFUNC)FltFakeFsdPathconf },           /* pathconf */
    { (struct vnodeop_desc*)NULL, (VOPFUNC)(int(*)())NULL }
#if 0
    { &vnop_default_desc,  (VOPFUNC)vn_default_error },
	{ &vnop_close_desc, (VOPFUNC)FltFakeFsdClose },		/* close */
	{ &vnop_access_desc, (VOPFUNC)FltFakeFsdAccess },		/* access */
	{ &vnop_getattr_desc, (VOPFUNC)union_getattr },		/* getattr */
	{ &vnop_setattr_desc, (VOPFUNC)union_setattr },		/* setattr */
	{ &vnop_read_desc, (VOPFUNC)union_read },		/* read */
	{ &vnop_write_desc, (VOPFUNC)union_write },		/* write */
	{ &vnop_ioctl_desc, (VOPFUNC)union_ioctl },		/* ioctl */
	{ &vnop_select_desc, (VOPFUNC)union_select },		/* select */
	{ &vnop_revoke_desc, (VOPFUNC)union_revoke },		/* revoke */
	{ &vnop_mmap_desc, (VOPFUNC)union_mmap },		/* mmap */
	{ &vnop_mnomap_desc, (VOPFUNC)union_mnomap },		/* mnomap */
	{ &vnop_fsync_desc, (VOPFUNC)union_fsync },		/* fsync */
	{ &vnop_remove_desc, (VOPFUNC)union_remove },		/* remove */
	{ &vnop_link_desc, (VOPFUNC)union_link },		/* link */
	{ &vnop_rename_desc, (VOPFUNC)union_rename },		/* rename */
	{ &vnop_mkdir_desc, (VOPFUNC)union_mkdir },		/* mkdir */
	{ &vnop_rmdir_desc, (VOPFUNC)union_rmdir },		/* rmdir */
	{ &vnop_symlink_desc, (VOPFUNC)union_symlink },		/* symlink */
	{ &vnop_readdir_desc, (VOPFUNC)union_readdir },		/* readdir */
	{ &vnop_readlink_desc, (VOPFUNC)union_readlink },	/* readlink */
	{ &vnop_pathconf_desc, (VOPFUNC)union_pathconf },	/* pathconf */
	{ &vnop_advlock_desc, (VOPFUNC)union_advlock },		/* advlock */
    { &vnop_copyfile_desc, (VOPFUNC)err_copyfile },		/* Copyfile */
	{ &vnop_blktooff_desc, (VOPFUNC)union_blktooff },	/* blktooff */
	{ &vnop_offtoblk_desc, (VOPFUNC)union_offtoblk },	/* offtoblk */
	{ &vnop_blockmap_desc, (VOPFUNC)union_blockmap },	/* blockmap */
#endif//0
    
#else//#ifndef _VIF_MACOSX_VFS_ISOLATION
    
    { &vnop_default_desc,       (VOPFUNC)vn_default_error                   },
    { &vnop_access_desc,        (VOPFUNC)FltFakeFsdAccess                   },
    { &vnop_advlock_desc,       (VOPFUNC)FltFakeFsdAdvlock                  },
    { &vnop_allocate_desc,      (VOPFUNC)FltFakeFsdAllocate                 },
    { &vnop_blktooff_desc,      (VOPFUNC)FltFakeFsdBlktooff                 },
    { &vnop_blockmap_desc,      (VOPFUNC)FltFakeFsdBlockmap                 },
    { &vnop_bwrite_desc,        (VOPFUNC)FltFakeFsdBwrite                   },
    { &vnop_close_desc,         (VOPFUNC)FltFakeFsdClose                    },
    { &vnop_copyfile_desc,      (VOPFUNC)FltFakeFsdCopyfile                 },
    { &vnop_create_desc,        (VOPFUNC)FltFakeFsdCreate                   },
    { &vnop_exchange_desc,      (VOPFUNC)FltFakeFsdExchange                 },
    { &vnop_fsync_desc,         (VOPFUNC)FltFakeFsdFsync                    },
    { &vnop_getattr_desc,       (VOPFUNC)FltFakeFsdGetattr                  },
    { &vnop_getxattr_desc,      (VOPFUNC)FltFakeFsdGetxattr                 },
    { &vnop_inactive_desc,      (VOPFUNC)FltFakeFsdInactive                 },
    { &vnop_ioctl_desc,         (VOPFUNC)FltFakeFsdIoctl                    },
    { &vnop_link_desc,          (VOPFUNC)FltFakeFsdLink                     },
    { &vnop_listxattr_desc,     (VOPFUNC)FltFakeFsdListxattr                },
    { &vnop_lookup_desc,        (VOPFUNC)FltFakeFsdLookup                   },
    { &vnop_kqfilt_add_desc,    (VOPFUNC)FltFakeFsdKqfiltAdd                },
    { &vnop_kqfilt_remove_desc, (VOPFUNC)FltFakeFsdKqfiltRemove             },
    { &vnop_mkdir_desc,         (VOPFUNC)FltFakeFsdMkdir                    },
    { &vnop_mknod_desc,         (VOPFUNC)FltFakeFsdMknod                    },
    { &vnop_mmap_desc,          (VOPFUNC)FltFakeFsdMmap                     },
    { &vnop_mnomap_desc,        (VOPFUNC)FltFakeFsdMnomap                   },
    { &vnop_offtoblk_desc,      (VOPFUNC)FltFakeFsdOfftoblk                 },
    { &vnop_open_desc,          (VOPFUNC)FltFakeFsdOpen                     },
    { &vnop_pagein_desc,        (VOPFUNC)FltFakeFsdPagein                   },
    { &vnop_pageout_desc,       (VOPFUNC)FltFakeFsdPageout                  },
    { &vnop_pathconf_desc,      (VOPFUNC)FltFakeFsdPathconf                 },
    { &vnop_read_desc,          (VOPFUNC)FltFakeFsdRead                     },
    { &vnop_readdir_desc,       (VOPFUNC)FltFakeFsdReaddir                  },
    { &vnop_readdirattr_desc,   (VOPFUNC)FltFakeFsdReaddirattr              },
    { &vnop_readlink_desc,      (VOPFUNC)FltFakeFsdReadlink                 },
    { &vnop_reclaim_desc,       (VOPFUNC)FltFakeFsdReclaim                  },
    { &vnop_remove_desc,        (VOPFUNC)FltFakeFsdRemove                   },
    { &vnop_removexattr_desc,   (VOPFUNC)FltFakeFsdRemovexattr              },
    { &vnop_rename_desc,        (VOPFUNC)FltFakeFsdRename                   },
    { &vnop_revoke_desc,        (VOPFUNC)FltFakeFsdRevoke                   },
    { &vnop_rmdir_desc,         (VOPFUNC)FltFakeFsdRmdir                    },
    { &vnop_searchfs_desc,      (VOPFUNC)FltFakeFsdSearch                   },
    { &vnop_select_desc,        (VOPFUNC)FltFakeFsdSelect                   },
    { &vnop_setattr_desc,       (VOPFUNC)FltFakeFsdSetattr                  }, 
    { &vnop_setxattr_desc,      (VOPFUNC)FltFakeFsdSetxattr                 },
    { &vnop_strategy_desc,      (VOPFUNC)FltFakeFsdStrategy                 },
    { &vnop_symlink_desc,       (VOPFUNC)FltFakeFsdSymlink                  },
    { &vnop_whiteout_desc,      (VOPFUNC)FltFakeFsdWhiteout                 },
    { &vnop_write_desc,         (VOPFUNC)FltFakeFsdWrite                    },
    
#ifdef __APPLE_API_UNSTABLE
#if NAMEDSTREAMS
    // TO DO - the following three definitions are from Apple unstable kernel
    // portion and requires name streams to be compiled in the kernel,
    // might prevent the driver from loading
    { &vnop_getnamedstream_desc,    (VOPFUNC)FltFakeGetnamedstream          },
    { &vnop_makenamedstream_desc,   (VOPFUNC)FltFakeMakenamedstream         },
    { &vnop_removenamedstream_desc, (VOPFUNC)FltFakeRemovenamedstream       },
#endif // __APPLE_API_UNSTABLE
#endif // NAMEDSTREAMS
    
    { (struct vnodeop_desc*)NULL, (VOPFUNC)(int(*)())NULL }
    
#endif//#else _VIF_MACOSX_VFS_ISOLATION
    
};

struct vnodeopv_desc gFakeFsdVnodeopOpvDesc =
{ &gFltFakeVnodeop, gFltFakeVnodeopEntries };

static struct vnodeopv_desc * gFltFakeFsdVNodeOperationsDescList[1] =
{
	&gFakeFsdVnodeopOpvDesc
};

static vfstable_t gFltFakeFsdHandle;

struct vfsops gFltFakeFasdVFSOps =
{
	FltFakeFsdVfsMount,
	0,			// start
	FltFakeFsdVfsUnmount,
	FltFakeFsdVfsRoot,
	0,			// quotactl
	FltFakeFsdVfsGetAttributes,
	0,			// synchronize
	FltFakeFsdVfsVGet,
	0,			// fhtovp
	0,			// vptofh
	0,			// init
	0,			// sysctl
	0,			// setattr
	{ 0 }		// reserved
};

#define VIF_FAKE_FSD_NAME "FltFakeFsd"


//--------------------------------------------------------------------

//
// the gFltFakeVnodeopEntries  array must be synchronized with the gFltVnodeOpvOffsetDesc array,
// i.e. the arrays must contain the same number and types of entries excluding the terminating ones
// and the vnop_default_desc entry
//
static FltVnodeOpvOffsetDesc  gFltVnodeOpvOffsetDesc[] = {
    
#ifndef _VIF_MACOSX_VFS_ISOLATION
    
	{ &vnop_lookup_desc,   VIF_VOP_UNKNOWN_OFFSET },            /* lookup */
	{ &vnop_create_desc,   VIF_VOP_UNKNOWN_OFFSET },            /* create */
	{ &vnop_whiteout_desc, VIF_VOP_UNKNOWN_OFFSET },            /* whiteout */
	{ &vnop_mknod_desc,    VIF_VOP_UNKNOWN_OFFSET },            /* mknod */
	{ &vnop_open_desc,     VIF_VOP_UNKNOWN_OFFSET },		    /* open */
    { &vnop_close_desc,    VIF_VOP_UNKNOWN_OFFSET },		    /* close */
    { &vnop_access_desc,   VIF_VOP_UNKNOWN_OFFSET },            /* access */
    { &vnop_inactive_desc, VIF_VOP_UNKNOWN_OFFSET },	        /* inactive */
    { &vnop_reclaim_desc,  VIF_VOP_UNKNOWN_OFFSET },            /* reclaim */
	{ &vnop_read_desc,     VIF_VOP_UNKNOWN_OFFSET },		    /* read */
	{ &vnop_write_desc,    VIF_VOP_UNKNOWN_OFFSET },		    /* write */
	{ &vnop_pagein_desc,   VIF_VOP_UNKNOWN_OFFSET },		    /* Pagein */
	{ &vnop_pageout_desc,  VIF_VOP_UNKNOWN_OFFSET },		    /* Pageout */
    { &vnop_strategy_desc, VIF_VOP_UNKNOWN_OFFSET },		    /* Strategy */
    { &vnop_mmap_desc,     VIF_VOP_UNKNOWN_OFFSET },            /* mmap */
    { &vnop_rename_desc,   VIF_VOP_UNKNOWN_OFFSET },            /* rename */
    { &vnop_pathconf_desc, VIF_VOP_UNKNOWN_OFFSET },
    { (struct vnodeop_desc*)NULL, VIF_VOP_UNKNOWN_OFFSET }

    
#else//#ifndef _VIF_MACOSX_VFS_ISOLATION
    
    { &vnop_access_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_advlock_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_allocate_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_blktooff_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_blockmap_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_bwrite_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_close_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_copyfile_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_create_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_exchange_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_fsync_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_getattr_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_getxattr_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_inactive_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_ioctl_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_link_desc,          VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_listxattr_desc,     VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_lookup_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_kqfilt_add_desc,    VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_kqfilt_remove_desc, VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_mkdir_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_mknod_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_mmap_desc,          VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_mnomap_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_offtoblk_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_open_desc,          VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_pagein_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_pageout_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_pathconf_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_read_desc,          VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_readdir_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_readdirattr_desc,   VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_readlink_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_reclaim_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_remove_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_removexattr_desc,   VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_rename_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_revoke_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_rmdir_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_searchfs_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_select_desc,        VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_setattr_desc,       VIF_VOP_UNKNOWN_OFFSET  }, 
    { &vnop_setxattr_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_strategy_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_symlink_desc,       VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_whiteout_desc,      VIF_VOP_UNKNOWN_OFFSET  },
    { &vnop_write_desc,         VIF_VOP_UNKNOWN_OFFSET  },
    
#ifdef __APPLE_API_UNSTABLE
#if NAMEDSTREAMS
    // TO DO - the following three definitions are from Apple unstable kernel
    // portion and requires name streams to be compiled in the kernel,
    // might prevent the driver from loading
    { &vnop_getnamedstream_desc,    VIF_VOP_UNKNOWN_OFFSET },
    { &vnop_makenamedstream_desc,   VIF_VOP_UNKNOWN_OFFSET },
    { &vnop_removenamedstream_desc, VIF_VOP_UNKNOWN_OFFSET },
#endif // __APPLE_API_UNSTABLE
#endif // NAMEDSTREAMS
    
    { (struct vnodeop_desc*)NULL, VIF_VOP_UNKNOWN_OFFSET }
    
#endif//#else _VIF_MACOSX_VFS_ISOLATION
    
};

//
// exclude the terminating entry from the valid entries number
//
static int gNumberOfOpvOffsetDescs = sizeof( gFltVnodeOpvOffsetDesc )/sizeof( gFltVnodeOpvOffsetDesc[0] ) - 0x1;

//
// an offset from the start of vnode to the v_op member, in bytes
//
vm_size_t    gVNodeVopOffset = VIF_VOP_UNKNOWN_OFFSET;

//--------------------------------------------------------------------

FltVfsOperationDesc  gFltVfsOperationDesc[] = {
    
    { kFltVfsOpUnmount, VIF_VOP_UNKNOWN_OFFSET, (VFSFUNC)FltFakeFsdVfsUnmount },
    
    //
    // the terminating entry
    //
    { kFltVfsOpUnknown, VIF_VOP_UNKNOWN_OFFSET, (VFSFUNC)VIF_VOP_UNKNOWN_OFFSET }
};


//
// an offset to the mount structure's operation vector
//
vm_size_t   gVfsOperationVectorOffset = VIF_VOP_UNKNOWN_OFFSET;

//--------------------------------------------------------------------

VOPFUNC*
FltGetVnodeOpVector(
    __in vnode_t vnode
    )
{
    assert( VIF_VOP_UNKNOWN_OFFSET != gVNodeVopOffset );
    
    if( VIF_VOP_UNKNOWN_OFFSET == gVNodeVopOffset )
        return (VOPFUNC*)VIF_VOP_UNKNOWN_OFFSET;
    
    //
    // get the v_op vector's address
    //
    return *(VOPFUNC**)( ((vm_address_t)vnode) + gVNodeVopOffset );
}

//--------------------------------------------------------------------

VFSFUNC*
FltGetVfsOperations(
    __in mount_t   mnt
    )
{
    assert( VIF_VOP_UNKNOWN_OFFSET != gVfsOperationVectorOffset );
    
    if( VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset )
        return (VFSFUNC*)VIF_VOP_UNKNOWN_OFFSET;
    
    return *(VFSFUNC**)( ((vm_address_t)mnt) + gVfsOperationVectorOffset );
}

//--------------------------------------------------------------------

IOReturn
FltRegisterFakeFsd()
{
	errno_t				error		= KERN_FAILURE;
	struct vfs_fsentry	vfsEntry;
	
	bzero ( &vfsEntry, sizeof ( vfsEntry ) );
	
	vfsEntry.vfe_vfsops		= &gFltFakeFasdVFSOps;
	vfsEntry.vfe_vopcnt		= 1;	// Just one vnode operation table
	vfsEntry.vfe_opvdescs	= gFltFakeFsdVNodeOperationsDescList;
	vfsEntry.vfe_flags		= VFS_TBLNOTYPENUM | VFS_TBLLOCALVOL | VFS_TBL64BITREADY | VFS_TBLTHREADSAFE | VFS_TBLFSNODELOCK;
	
	strlcpy ( vfsEntry.vfe_fsname, VIF_FAKE_FSD_NAME, sizeof ( VIF_FAKE_FSD_NAME ) ); 
	
	error = vfs_fsadd ( &vfsEntry, &gFltFakeFsdHandle );
    assert( !error );
    if( error ){
        
        goto __exit;
    }
	
__exit:
	return error ? kIOReturnError : kIOReturnSuccess;
    
}

//--------------------------------------------------------------------

IOReturn
FltUnRegisterFakeFsd()
{
	
	errno_t 	error;
	
    assert( 0x0 == gVnodeCount );
    
    if( NULL == gFltFakeFsdHandle )
        return kIOReturnSuccess;
    
	error = vfs_fsremove ( gFltFakeFsdHandle );
    assert( !error );
	
    if( !error )
        gFltFakeFsdHandle = NULL;
    
	return error ? kIOReturnError : kIOReturnSuccess;
	
}

//--------------------------------------------------------------------

IOReturn
FltFakeFsdCreateVnode(
    __in void*        vNodeData,
    __out vnode_t *   vNodeHandle
    )
{
	
	errno_t 				error    = 0;
	vnode_t 				vNodePtr  = NULLVP;
	struct vnode_fsparam	vfsp;
	
	assert ( vNodeHandle != NULL );
	
    //
	// Zero the FS param structure
    //
	bzero ( &vfsp, sizeof ( vfsp ) );
	
	vfsp.vnfs_mp		 = NULL;
    vfsp.vnfs_vtype 	 = VNON; //VREG;
	vfsp.vnfs_str 		 = VIF_FAKE_FSD_NAME;
	vfsp.vnfs_dvp 		 = NULL;
	vfsp.vnfs_fsnode 	 = vNodeData;
	vfsp.vnfs_cnp 		 = NULL;
	vfsp.vnfs_vops 		 = gFltFakeVnodeop;
	vfsp.vnfs_rdev 		 = 0;
    vfsp.vnfs_flags      = VNFS_NOCACHE;
	vfsp.vnfs_marksystem = 0;
	
    //
	// Note that vnode_create ( ) returns the vnode with an iocount of +1;
	// this routine returns the newly created vnode with this positive iocount.
    //
	error = vnode_create ( VNCREATE_FLAVOR, ( uint32_t ) VCREATESIZE, &vfsp, &vNodePtr );
    assert( !error );
	if( error != 0 ){
		
		assert ( "FltFakeFsdCreateVnode->vnode_create failed" );
        DBG_PRINT_ERROR(("FltFakeFsdCreateVnode->vnode_create failed, error=%u\n", error));
		goto __exit;
		
	}

    //
	// Return the vnode to the caller
    //
	*vNodeHandle = vNodePtr;
    
    //
    // account for the new vnode
    //
    OSIncrementAtomic( &gVnodeCount );
	
	
__exit:
	
	return error ? kIOReturnError : kIOReturnSuccess;
	
}

//--------------------------------------------------------------------

struct vnodeopv_entry_desc*
FltRetriveVnodeOpvEntryDescByFakeFsdVnodeOp(
    __in VOPFUNC   FakeFsdVnodeOp
    )
{
    for( int i = 0x0; NULL != gFltFakeVnodeopEntries[ i ].opve_impl; ++i ){
        
        if( gFltFakeVnodeopEntries[ i ].opve_impl == FakeFsdVnodeOp )
            return &gFltFakeVnodeopEntries[ i ];
        
    }// end for
    
    return (struct vnodeopv_entry_desc*)NULL;
}


FltVnodeOpvOffsetDesc*
FltRetriveVnodeOpvOffsetDescByVnodeOpDesc(
    __in struct vnodeop_desc *opve_op
    )
{
    for( int i = 0x0; NULL != gFltVnodeOpvOffsetDesc[ i ].opve_op; ++i ){
        
        if( opve_op == gFltVnodeOpvOffsetDesc[ i ].opve_op )
            return &gFltVnodeOpvOffsetDesc[ i ];
        
    }// end for
    
    return ( FltVnodeOpvOffsetDesc* )NULL;
}


FltVnodeOpvOffsetDesc*
FltRetriveVnodeOpvOffsetDescByFakeFsdVnodeOp(
    __in VOPFUNC   FakeFsdVnodeOp
    )
{
    struct vnodeopv_entry_desc*    vnodeOpvEntryDesc;
    
    vnodeOpvEntryDesc = FltRetriveVnodeOpvEntryDescByFakeFsdVnodeOp( FakeFsdVnodeOp );
    if( NULL == vnodeOpvEntryDesc )
        return NULL;
    
    FltVnodeOpvOffsetDesc* vnodeOpvOffsetDesc;
    
    vnodeOpvOffsetDesc = FltRetriveVnodeOpvOffsetDescByVnodeOpDesc( vnodeOpvEntryDesc->opve_op );
    
    return vnodeOpvOffsetDesc;
}

//--------------------------------------------------------------------

FltVfsOperationDesc*
FltGetVfsOperationDesc( __in FltVfsOperation   operation)
{
    for( int i = 0x0; kFltVfsOpUnknown != gFltVfsOperationDesc[ i ].operation; ++i ){
        
        if( operation == gFltVfsOperationDesc[ i ].operation )
            return &gFltVfsOperationDesc[ i ];
    }
    
    return NULL;
}

//--------------------------------------------------------------------

bool DdIsFunctionAtKernelAddress( __in vm_address_t address )
{
    const int MAX_INSTRUCTIONS = 15; // 15 is a requirement from distorm, a minimal value
    
	// Holds the result of the decoding
	_DecodeResult res;
	// Decoded instructions information
	_DInst decodedInstructions[ MAX_INSTRUCTIONS ];
	// decodedInstructionsCount holds the count of filled instructions' array by the decoder.
	unsigned int decodedInstructionsCount = 0;
    
    int len = 100;

    //
    // we are decodig the kernel, in case of a user mode the decode type
    // is defined by the process type( 32 or 64 bit process )
    //
#ifdef __LP64__
    _DecodeType dt = Decode64Bits;
#else	/* __LP64__ */
    _DecodeType dt = Decode32Bits;
#endif	/* __LP64__ */
    
    assert( len > 0x1 && len < PAGE_SIZE );
    
    //
    // check that the address is valid and backed by a physical page
    //
    if( 0x0 == FltVirtToPhys( address ) )
        return false;
    
    //
    // if there will be a page crossing, check that the next page is valid
    //
    if( trunc_page( address ) != trunc_page( address + len ) &&
        0x0 == FltVirtToPhys( address + len - 0x1 ) )
       return false;

    _CodeInfo ci;
    
    ci.codeOffset = (_OffsetType)address;
    ci.code = (unsigned char*)address;
    ci.codeLen = len;
    ci.dt = dt;
    ci.features = 0x0;
    
    res = distorm_decompose64( &ci, decodedInstructions, MAX_INSTRUCTIONS, &decodedInstructionsCount);
    
    /*
     if( *(char*)address != (char)0x55 && *((char*)address + 1 ) != (char)0x89 )
        return false;
     */
    
    //
    // check for the pattern at the start
    // push   %ebp
    // mov    %esp,%ebp
    // to simplify check that push and mov instructions are present
    // DECRES_MEMORYERR is returned when the buffer ends inside an instruction
    //
    if( ( DECRES_SUCCESS != res && DECRES_MEMORYERR != res )|| decodedInstructionsCount < MAX_INSTRUCTIONS ){
        
        DBG_PRINT_ERROR(("distorm_decode64() returned res = %u, decodedInstructionsCount = %u\n", res, decodedInstructionsCount));
        return false;
    }
    
    /*
     FYI, the values for a case of genuine VFS operation
     
     0x488e8b <hfs_mount>:	push   %ebp
     0x488e8c <hfs_mount+1>:	mov    %esp,%ebp
     0x488e8e <hfs_mount+3>:	sub    $0xd8,%esp
     0x488e94 <hfs_mount+9>:	mov    %ebx,-0xc(%ebp)
     0x488e97 <hfs_mount+12>:	mov    %esi,-0x8(%ebp)
     0x488e9a <hfs_mount+15>:	mov    %edi,-0x4(%ebp)
     0x488e9d <hfs_mount+18>:	mov    0x10(%ebp),%ebx
     0x488ea0 <hfs_mount+21>:	mov    0x14(%ebp),%esi
     0x488ea3 <hfs_mount+24>:	mov    0x18(%ebp),%eax
     0x488ea6 <hfs_mount+27>:	mov    %eax,(%esp)
     0x488ea9 <hfs_mount+30>:	call   0x32ac2f <vfs_context_proc>
     0x488eae <hfs_mount+35>:	mov    %eax,-0x8c(%ebp)
     0x488eb4 <hfs_mount+41>:	movl   $0x28,0xc(%esp)
     0x488ebc <hfs_mount+49>:	lea    -0x6c(%ebp),%edi
     0x488ebf <hfs_mount+52>:	mov    %edi,0x8(%esp)
     (gdb) p decodedInstructions[0]
     $7 = {
     addr = 4755083, 
     size = 1 '\001', 
     flags = 1280, 
     segment = 255 '?', 
     base = 255 '?', 
     scale = 0 '\0', 
     dispSize = 0 '\0', 
     opcode = 2, 
     ops = {{
     type = 1 '\001', 
     index = 21 '\025', 
     size = 32
     }, {
     type = 0 '\0', 
     index = 0 '\0', 
     size = 0
     }, {
     type = 0 '\0', 
     index = 0 '\0', 
     size = 0
     }, {
     type = 0 '\0', 
     index = 0 '\0', 
     size = 0
     }}, 
     disp = 0, 
     imm = {
     sbyte = 0 '\0', 
     byte = 0 '\0', 
     sword = 0, 
     word = 0, 
     sdword = 0, 
     dword = 0, 
     sqword = 0, 
     qword = 0, 
     addr = 0, 
     ptr = {
     seg = 0, 
     off = 0
     }, 
     ex = {
     i1 = 0, 
     i2 = 0
     }
     }, 
     unusedPrefixesMask = 0, 
     meta = 8 '\b', 
     usedRegistersMask = 32
     }
     (gdb) p decodedInstructions[1]
     $8 = {
     addr = 4755084, 
     size = 2 '\002', 
     flags = 1344, 
     segment = 255 '?', 
     base = 255 '?', 
     scale = 0 '\0', 
     dispSize = 0 '\0', 
     opcode = 42, 
     ops = {{
     type = 1 '\001', 
     index = 21 '\025', 
     size = 32
     }, {
     type = 1 '\001', 
     index = 20 '\024', 
     size = 32
     }, {
     type = 0 '\0', 
     index = 0 '\0', 
     size = 0
     }, {
     type = 0 '\0', 
     index = 0 '\0', 
     size = 0
     }}, 
     disp = 0, 
     imm = {
     sbyte = 0 '\0', 
     byte = 0 '\0', 
     sword = 0, 
     word = 0, 
     sdword = 0, 
     dword = 0, 
     sqword = 0, 
     qword = 0, 
     addr = 0, 
     ptr = {
     seg = 0, 
     off = 0
     }, 
     ex = {
     i1 = 0, 
     i2 = 0
     }
     }, 
     unusedPrefixesMask = 0, 
     meta = 8 '\b', 
     usedRegistersMask = 48
     }
     
     */
    
    //
    // the first instruction must be the push instruction
    //
    if( decodedInstructions[0].opcode != 0x2 )
        return false;
    
    //
    // the second instruction must be the mov instruction
    //
    if( decodedInstructions[1].opcode != 0x2A )
        return false;

    //__asm__ volatile( "int $0x3" );
    
    return true;
}

//--------------------------------------------------------------------

IOReturn
FltGetMountLayoutForMountStructure( __in mount_t mnt )
{
    /*
     in this function we suppose that the mount structure has the layout similar to
     the following
     
     struct mount {
        TAILQ_ENTRY(mount) mnt_list;		// mount list 
        int32_t		mnt_count;		// reference on the mount
        lck_mtx_t	mnt_mlock;		// mutex that protects mount point
        struct vfsops	*mnt_op;		// operations on fs
        struct vfstable	*mnt_vtable;		// configuration info
        struct vnode	*mnt_vnodecovered;	// vnode we mounted on
        ....
        ....
        ....
     };
     */
    
    assert( mnt );
    
    //
    // void* is a type of the VFS vector entry ( a function address )
    //
    for( VFSFUNC** vfsOpPtr = (VFSFUNC**)mnt;
        0x0 != FltVirtToPhys( (vm_offset_t)vfsOpPtr + sizeof(vfsOpPtr) - 0x1 ) &&
        (vm_address_t)vfsOpPtr < ((vm_address_t)mnt + 256);
        vfsOpPtr += 0x1 )
    {
        
        //
        // can the value be a valid address?
        //
        if( 0x0 == FltVirtToPhys( (vm_offset_t)*vfsOpPtr ) )
            continue;
        
        VFSFUNC*       vfsOpVector = *vfsOpPtr;
        vm_address_t   startPage = trunc_page((vm_offset_t)vfsOpVector);
        int            functionsFound = 0x0;
        
        for( int i = 0x0; i < 50; ++i, ++vfsOpVector ){
            
            //
            // check for the page boundary crossing for the following check ( unlikely )
            //
            if( trunc_page((vm_offset_t)vfsOpVector + sizeof(vfsOpVector) - 0x1 ) != startPage &&
               0x0 == FltVirtToPhys( (vm_offset_t)vfsOpVector + sizeof(vfsOpVector) - 0x1 ) )
                break;
            
            if( DdIsFunctionAtKernelAddress( (vm_address_t)*vfsOpVector ) )
                ++functionsFound;
            
            //
            // at least mount(), unmount(), init() and sync() must be implemented
            //
            if( functionsFound < 4 )
                continue;
            
            //
            // as the first function is found we consider that the VFS operation vector is found
            //
            if( VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset )
                gVfsOperationVectorOffset = (vm_address_t)vfsOpPtr - (vm_address_t)mnt;
            
            for( int k = 0x0; kFltVfsOpUnknown != gFltVfsOperationDesc[ k ].operation; ++k ){
                                
                assert( VIF_VOP_UNKNOWN_OFFSET == gFltVfsOperationDesc[ k ].offset );
                
                vm_offset_t  offset;
                
                switch( gFltVfsOperationDesc[ k ].operation ){
                        
                    case kFltVfsOpUnmount:
                        offset = __offsetof( struct vfsops, vfs_unmount );
                        break;
                        
                    default:
                        assert( !"unknown VFS operation" );
                        DBG_PRINT_ERROR(("unknown VFS operation %u\n", (unsigned int)gFltVfsOperationDesc[ k ].operation));
                        break;
                } // end switch
                
                gFltVfsOperationDesc[ k ].offset = offset;
            } // end for
            
            //
            // exit, we've done
            //
            break;
            
            /*
            bool  continueSearch = false;
             
            for( int k = 0x0; kFltVfsOpUnknown != gFltVfsOperationDesc[ k ].operation; ++k ){
                
                if( *vfsOpVector != gFltVfsOperationDesc[ k ].sampleFunction ){
                    
                    //
                    // if there is a not initialized entry then continue the for() cycle which is one level up
                    //
                    continueSearch = ( VIF_VOP_UNKNOWN_OFFSET == gFltVfsOperationDesc[ k ].offset );
                    continue;
                }
                
                //
                // as the first function is found we consider that the VFS operation vector is found
                //
                if( VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset )
                    gVfsOperationVectorOffset = (vm_address_t)vfsOpPtr - (vm_address_t)mnt;
                
                assert( VIF_VOP_UNKNOWN_OFFSET == gFltVfsOperationDesc[ k ].offset );
                
                gFltVfsOperationDesc[ k ].offset = ( i * sizeof( *vfsOpVector ) );
                break;
            }
            
            //
            // stop searching if all entries have been initialized
            //
            if( !continueSearch )
                break;
             */
            
        } // end for
        
        if( VIF_VOP_UNKNOWN_OFFSET != gVfsOperationVectorOffset )
            break;
        
    } // end for
    
    assert( VIF_VOP_UNKNOWN_OFFSET != gVfsOperationVectorOffset );
    
    if( VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset ){
        
        DBG_PRINT_ERROR(("VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset\n"));
        return kIOReturnError;
    }
    
    return kIOReturnSuccess;
}

//--------------------------------------------------------------------

int
FltGetMountLayoutCallback( __in mount_t mnt, __in void* data )
{
    if( kIOReturnSuccess == FltGetMountLayoutForMountStructure( mnt ) )
        return VFS_RETURNED_DONE;
    else
        return VFS_RETURNED;
}

//--------------------------------------------------------------------

IOReturn
FltGetMountLayout()
{
    vfs_iterate( 0x0, FltGetMountLayoutCallback, NULL );
    
    assert( VIF_VOP_UNKNOWN_OFFSET != gVfsOperationVectorOffset );
    
    if( VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset ){
        
        DBG_PRINT_ERROR(("VIF_VOP_UNKNOWN_OFFSET == gVfsOperationVectorOffset\n"));
        return kIOReturnError;
    }
    
    return kIOReturnSuccess;    
}

//--------------------------------------------------------------------

IOReturn
FltGetVnodeLayout()
{
    IOReturn         RC;
    vm_offset_t      vData1 = (vm_offset_t)( (long)(-1)-2 );
    vm_offset_t      vData2 = (vm_offset_t)( (long)(-1)-4 );
    vnode_t          vNode1 = NULL;
    vnode_t          vNode2 = NULL;
    vm_offset_t      vNode1Page;
    vm_offset_t      vNode2Page;
    vm_offset_t      ptrNode1 = NULL;
    vm_offset_t      ptrNode2 = NULL;
    vm_size_t        vNodeHeaderSize;
    vm_size_t        vNodeVopOffset;
    bool             vNodeVopFound = false;
    
    assert( ( sizeof( gFltFakeVnodeopEntries )/sizeof( gFltFakeVnodeopEntries[ 0 ] ) - 2 ) == gNumberOfOpvOffsetDescs );
            
#if defined( DBG )
    {
        //__asm__ volatile( "int $0x3" );
    }
#endif
    
    RC = FltRegisterFakeFsd();
    if( kIOReturnSuccess != RC )
        goto __exit;
    
    RC = FltFakeFsdCreateVnode( (void*)vData1, &vNode1 );
    if( kIOReturnSuccess != RC )
        goto __exit;
    
    RC = FltFakeFsdCreateVnode( (void*)vData2, &vNode2 );
    if( kIOReturnSuccess != RC )
        goto __exit;
    
    assert( 0x2 == gVnodeCount );
    
    ptrNode1 = (vm_offset_t)vNode1;
    ptrNode2 = (vm_offset_t)vNode2;
    
    vNode1Page = trunc_page( ptrNode1 );
    vNode2Page = trunc_page( ptrNode2 );
    
    vNodeHeaderSize = 0x0;
    
    //
    // the assumtion is - the  vnode layout is as follows
    // <some fields that are irrelevant for us>
    // int 	(**v_op)(void *);		/* vnode operations vector */
	// mount_t v_mount;			    /* ptr to vfs we are in */
	// void *	v_data;				/* private data for fs */
    //
    while( *(vm_offset_t*)ptrNode1 != vData1 && *(vm_offset_t*)ptrNode2 != vData2 ){
        
        ptrNode1 += sizeof( void* );
        ptrNode2 += sizeof( void* );
        
        vNodeHeaderSize += sizeof( void* );
        
        //
        // if we are crossing a page boundary check that a next page is wired
        //
        if(  trunc_page( ptrNode1 + sizeof( vm_offset_t* ) - 0x1 ) != vNode1Page && 
              0x0 == FltVirtToPhys( ptrNode1 + sizeof( vm_offset_t* ) - 0x1 ) ){
            
            //
            // the search failed
            //
            vNodeHeaderSize = (-1);
            break;
        }
        
        if( trunc_page( ptrNode2 + sizeof( vm_offset_t* ) - 0x1 ) != vNode2Page &&
            0x0 == FltVirtToPhys( ptrNode2 + sizeof( vm_offset_t* ) - 0x1 ) ){
            
            //
            // the search failed
            //
            vNodeHeaderSize = (-1);
            break;
        }
        
        if( vNodeHeaderSize >= PAGE_SIZE ){
            
            //
            // the search failed
            //
            vNodeHeaderSize = (-1);
            break;
        }
        
    }// end while
    
    assert( 0x0 != vNodeHeaderSize && (-1) != vNodeHeaderSize );
    
    if( 0x0 == vNodeHeaderSize || (-1) == vNodeHeaderSize ){
        
        RC = kIOReturnError;
        goto __exit;
    }
    
    assert( *(vm_offset_t*)ptrNode1 == vData1 && *(vm_offset_t*)ptrNode2 == vData2 );
    
    //
    // move in the opposite direction and find the v_op pointer
    //
    assert( 0x0 == ( ptrNode1 - (vm_offset_t)vNode1 )%sizeof(VOPFUNC) );
    
    vNodeVopOffset = vNodeHeaderSize;
    while( !vNodeVopFound && ptrNode1 > (vm_offset_t)vNode1 && ptrNode1 > (vm_offset_t)vNode1 ){
        
        ptrNode1 -= sizeof( VOPFUNC );
        ptrNode2 -= sizeof( VOPFUNC );
        
        vNodeVopOffset -= sizeof( VOPFUNC );
      
        assert( ptrNode1 >= (vm_offset_t)vNode1);
        assert( ptrNode2 >= (vm_offset_t)vNode2 );
        assert( vNodeVopOffset >= 0 );
        
        //
        // if the offsets contain the equal values and the value is a valid address then
        // this is a good candidate for a v_op field, use a captured values to avoid
        // a field change under us ( unlikely, may be impossible )
        //
        VOPFUNC*  v_op1 = *(VOPFUNC**)ptrNode1;
        VOPFUNC*  v_op2 = *(VOPFUNC**)ptrNode1;
        
        if( v_op1 != 0x0 && v_op1  == v_op2 && 0x0 != FltVirtToPhys( (vm_offset_t)v_op1 ) ){
            
            VOPFUNC*       func = v_op1;
            vm_address_t   startPage = trunc_page((vm_address_t)func);
            int            numberOfHits = 0x0;
            
            //
            // search the FltFakeFsdXXXX's address at no more than 100*sizeof(VOPFUNC) offset
            //
            for( int i = 0x0; i < 100; ++i, ++func ){
                
                //
                // check for the page boundary crossing for the following check ( unlikely )
                //
                if( trunc_page((vm_offset_t)func + sizeof(VOPFUNC) - 0x1 ) != startPage &&
                    0x0 == FltVirtToPhys( (vm_offset_t)func + sizeof(VOPFUNC) - 0x1 ) )
                    break;
                
                FltVnodeOpvOffsetDesc*    vnodeOpvOffsetDesc;
                
                vnodeOpvOffsetDesc = FltRetriveVnodeOpvOffsetDescByFakeFsdVnodeOp( *func );
                if( NULL == vnodeOpvOffsetDesc )
                    continue;
                
                assert( VIF_VOP_UNKNOWN_OFFSET == vnodeOpvOffsetDesc->offset );
                
                //
                // yet another coincidence has been found
                //
                ++numberOfHits;
                vnodeOpvOffsetDesc->offset = i * sizeof( VOPFUNC );
                
#if defined( DBG )
                {
                   // __asm__ volatile( "int $0x3" );
                }
#endif
                
                if( gNumberOfOpvOffsetDescs == numberOfHits ){
                    
                    //
                    // vNodeVopOffset is an offset for the v_op member
                    //
                    vNodeVopFound = true;
                }
                
                if( vNodeVopFound )
                    break;
                
            }// end for
            
        }// end if
        
    }// end while
    
    
    assert( vNodeVopFound );
    
    if( vNodeVopFound ){
        
        assert( vNode1 );
        
        gVNodeVopOffset = vNodeVopOffset;
        RC = FltGetMountLayout();
        assert( kIOReturnSuccess == RC );
        
    } else {
        
        RC = kIOReturnError;
        assert( VIF_VOP_UNKNOWN_OFFSET == gVNodeVopOffset );
    }
    
    
__exit:
    
    
    if( vNode1 ){
        
        assert( 0x2 == gVnodeCount );
        
        //
        // allow the node to be reuses immediately by calling vnode_recycle
        // which calls 
        // vnode_reclaim_internal()->vgone()->vclean()->VNOP_RECLAIM()
        // when the reference count is 0x0 ( which is true for the next vnode_put() call )
        //
        vnode_recycle( vNode1 );
        vnode_put( vNode1 );
        VIF_DBG_MAKE_POINTER_INVALID( vNode1 );
        
    }// end if
    
    
    if( vNode2 ){
        
        assert( 0x1 == gVnodeCount );
        
        //
        // allow the node to be reused immediately by calling vnode_recycle
        // which calls 
        // vnode_reclaim_internal()->vgone()->vclean()->VNOP_RECLAIM()
        // when the reference count is 0x0 ( which is true for the next vnode_put() call )
        //
        vnode_recycle( vNode2 );
        vnode_put( vNode2 );
        VIF_DBG_MAKE_POINTER_INVALID( vNode2 );
        
    }// end if
    
    assert( 0x0 == gVnodeCount );
    
    FltUnRegisterFakeFsd();
        
    return RC;
}

//--------------------------------------------------------------------

