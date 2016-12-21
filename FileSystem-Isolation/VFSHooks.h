//
//  VFSHooks.h
//
//  Copyright (c) 2015 Slava Imameev. All rights reserved.
//

#ifndef __VFSFilter0__VFSHooks__
#define __VFSFilter0__VFSHooks__

#include <IOKit/IOService.h>
#include <IOKit/assert.h>

#ifdef __cplusplus
extern "C" {
#endif
    
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/vnode.h>
    
#ifdef __cplusplus
}
#endif

#include "Common.h"

//--------------------------------------------------------------------

typedef enum _FltVnodeType{
    FltVnodeOthers = 0,
    FltVnodeWatched,
    FltVnodeRedirected,
    
    //
    // always the last
    //
    FltVnodeMaximum
} FltVnodeType;

//--------------------------------------------------------------------

IOReturn
VFSHookInit();

void
VFSHookRelease();

IOReturn
FltHookVnodeVop(
                __inout vnode_t vnode,
                __inout bool* isVopHooked // if an error is returned the value is not defined
);

int
FltVnopLookupHook(
                     __inout struct vnop_lookup_args *ap
                     );

int
FltVnopCreateHook(
                     __inout struct vnop_create_args *ap
                     );

int
FltVnopCloseHook(
                   __inout struct vnop_close_args *ap
                   );

int
FltVnopReadHook(
                   __in struct vnop_read_args *ap
                   );
int
FltVnopOpenHook(
                   __in struct vnop_open_args *ap
                   );
int
FltVnopPageinHook(
                     __in struct vnop_pagein_args *ap
                     );

int
FltVnopWriteHook(
                    __in struct vnop_write_args *ap
                    );

int
FltVnopPageoutHook(
                      __in struct vnop_pageout_args *ap
                      );

int
FltVnopRenameHook(
                     __in struct vnop_rename_args *ap
                     );

int
FltVnopExchangeHook(
                       __in struct vnop_exchange_args *ap
                       );

int
FltFsdReclaimHook(struct vnop_reclaim_args *ap);

int
FltVnopInactiveHook(struct vnop_inactive_args *ap);

//--------------------------------------------------------------------

#endif /* defined(__VFSFilter0__VFSHooks__) */
