//
//  VFSHooks.cpp
//
//  Copyright (c) 2015 Slava Imameev. All rights reserved.
//

#ifdef __cplusplus
extern "C" {
#endif
    
#include <vfs/vfs_support.h>
#include <sys/ubc.h>
#include <sys/buf.h>
#include <sys/vm.h>
    
#ifdef __cplusplus
}
#endif

#include "VFSHooks.h"
#include "VmPmap.h"
#include "Kauth.h"
#include <sys/fcntl.h>
#include "VNodeHook.h"
#include "FltFakeFSD.h"

//--------------------------------------------------------------------

IOReturn
VFSHookInit()
{
    return kIOReturnSuccess;
}

//--------------------------------------------------------------------

void
VFSHookRelease()
{
}

//--------------------------------------------------------------------

int
FltVnopLookupHook(
    __inout struct vnop_lookup_args *ap
    )
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
    int (*origVnop)(struct vnop_lookup_args *ap);
    
    origVnop = (int (*)(struct vnop_lookup_args*))VifGetOriginalVnodeOp( ap->a_dvp, FltVopEnum_lookup );
    assert( origVnop );

    //
    // add your filter code here
    //
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopCreateHook(
    __inout struct vnop_create_args *ap
    )

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
    int (*origVnop)(struct vnop_create_args *ap);
    
    origVnop = (int (*)(struct vnop_create_args*))VifGetOriginalVnodeOp( ap->a_dvp, FltVopEnum_create );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopCloseHook(struct vnop_close_args *ap)
/*
 struct vnop_close_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 int  a_fflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_close_args *ap);
    
    origVnop = (int (*)(struct vnop_close_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_close );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopInactiveHook(struct vnop_inactive_args *ap)
/*
 struct vnop_inactive_args {
 struct vnodeop_desc *a_desc;
 struct vnode *a_vp;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_inactive_args *ap);
    
    origVnop = (int (*)(struct vnop_inactive_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_inactive );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopReadHook(
    __in struct vnop_read_args *ap
    )
/*
 struct vnop_read_args {
 struct vnodeop_desc *a_desc,
 struct vnode *a_vp;
 struct uio *a_uio;
 int  a_ioflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_read_args *ap);
    
    origVnop = (int (*)(struct vnop_read_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_read );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopOpenHook(
    __in struct vnop_open_args *ap
    )
/*
 struct vnop_open_args {
	struct vnodeop_desc *a_desc;
	vnode_t a_vp;
	int a_mode;
	vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_open_args *ap);
    
    origVnop = (int (*)(struct vnop_open_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_open );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopPageinHook(
    __in struct vnop_pagein_args *ap
    )
/*
 struct vnop_pagein_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 upl_t a_pl;
 upl_offset_t a_pl_offset;
 off_t a_f_offset;
 size_t a_size;
 int a_flags;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_pagein_args *ap);
    
    origVnop = (int (*)(struct vnop_pagein_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_pagein );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
    
 }

//--------------------------------------------------------------------

int
FltVnopWriteHook(
    __in struct vnop_write_args *ap
    )
/*
 struct vnop_write_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 struct uio *a_uio;
 int a_ioflag;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_write_args *ap);
    
    origVnop = (int (*)(struct vnop_write_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_write );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopPageoutHook(
    __in struct vnop_pageout_args *ap
    )
/*
 struct vnop_pageout_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_vp;
 upl_t a_pl;
 upl_offset_t a_pl_offset;
 off_t a_f_offset;
 size_t a_size;
 int a_flags;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_pageout_args *ap);
    
    origVnop = (int (*)(struct vnop_pageout_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_pageout );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopRenameHook(
    __in struct vnop_rename_args *ap
    )
/*
 struct vnop_rename_args {
 struct vnodeop_desc *a_desc;
 vnode_t a_fdvp;
 vnode_t a_fvp;
 struct componentname *a_fcnp;
 vnode_t a_tdvp;
 vnode_t a_tvp;
 struct componentname *a_tcnp;
 vfs_context_t a_context;
 } *ap;
 */
{
    int (*origVnop)(struct vnop_rename_args *ap);
    
    origVnop = (int (*)(struct vnop_rename_args*))VifGetOriginalVnodeOp( ap->a_fvp, FltVopEnum_rename );
    assert( origVnop );
    
    return origVnop( ap );
}

//--------------------------------------------------------------------

int
FltVnopExchangeHook(
    __in struct vnop_exchange_args *ap
    )
{
    int (*origVnop)(struct vnop_exchange_args *ap);
    
    origVnop = (int (*)(struct vnop_exchange_args*))VifGetOriginalVnodeOp( ap->a_fvp, FltVopEnum_exchange );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    return origVnop( ap );
 }

//--------------------------------------------------------------------

int
FltFsdReclaimHook(struct vnop_reclaim_args *ap)
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
    //
    // check that v_data is not NULL, this is a sanity check to be sure we don't damage FSD structures,
    // some FSDs set v_data to NULL intentionally, for example see the msdosfs's msdosfs_check_link()
    // that calls vnode_clearfsnode() for a temporary vnode of VNON type
    //
    assert( !( VNON != vnode_vtype( ap->a_vp ) && NULL == vnode_fsnode( ap->a_vp ) ) );
    
    int (*origVnop)(struct vnop_reclaim_args *ap);
    
    origVnop = (int (*)(struct vnop_reclaim_args*))VifGetOriginalVnodeOp( ap->a_vp, FltVopEnum_reclaim );
    assert( origVnop );
    
    //
    // add your filter code here
    //
    
    //
    // remember that this vnode has gone
    //
    FltUnHookVnodeVop( ap->a_vp );
    return origVnop( ap );
}

//--------------------------------------------------------------------
