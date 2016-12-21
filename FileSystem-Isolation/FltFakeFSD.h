/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#ifndef _VIFFAKEFSD_H
#define _VIFFAKEFSD_H

#ifdef __cplusplus
extern "C" {
#endif
    
#include <IOKit/assert.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
    
#ifdef __cplusplus
}
#endif

#include "Common.h"

//
// a type for the vnode operations
//
typedef int (*VOPFUNC)(void *) ;
typedef int (*VFSFUNC)(void *) ;

//
// the structure defines an offset from the start of a v_op vector for a function
// implementing a corresponding vnode operation
//
typedef struct _FltVnodeOpvOffsetDesc {
	struct vnodeop_desc *opve_op;   /* which operation this is, NULL for the terminating entry */
	vm_offset_t offset;		/* offset in bytes from the start of v_op, (-1) means "unknown" */
} FltVnodeOpvOffsetDesc;

//
// CAVEAT - this value is also an index for an array, so do not use some fancy valuse like 0x80000001
//
typedef enum _FltVfsOperation{
    kFltVfsOpUnknown = 0x0,
    kFltVfsOpUnmount,
    
    //
    // the last operation
    //
    kFltVfsOpMax
    
} FltVfsOperation;

//
// the structure defined an offset for VFS operation in the VFS operations vector
//
typedef struct _FltVfsOperationDesc{
    FltVfsOperation   operation;
    vm_offset_t       offset;		  // offset in bytes from the start of v_op, (-1) means "unknown"
    VFSFUNC           sampleFunction; // address od the sample function
} FltVfsOperationDesc;

#define VIF_VOP_UNKNOWN_OFFSET ((vm_offset_t)(-1))

//
// an offset from the start of vnode to the v_op member, in bytes
// an intial value is VIF_VOP_UNKNOWN_OFFSET
//
// extern vm_size_t    gVNodeVopOffset;
VOPFUNC*
FltGetVnodeOpVector(
    __in vnode_t vnode
    );

extern
IOReturn
FltGetVnodeLayout();

extern
FltVnodeOpvOffsetDesc*
FltRetriveVnodeOpvOffsetDescByVnodeOpDesc(
    __in struct vnodeop_desc *opve_op
    );

extern 
IOReturn
FltUnRegisterFakeFsd();

extern
VFSFUNC*
FltGetVfsOperations(
    __in mount_t   mnt
    );

FltVfsOperationDesc*
FltGetVfsOperationDesc( __in FltVfsOperation   operation);

#endif//_VIFFAKEFSD_H
