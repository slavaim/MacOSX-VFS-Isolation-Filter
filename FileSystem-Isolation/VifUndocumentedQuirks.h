/* 
 * Copyright (c) 2010 Slava Imameev. All rights reserved.
 */

#ifndef VIFUNDOCUMENTEDQUIRKS_H
#define VIFUNDOCUMENTEDQUIRKS_H

#include <sys/types.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/vm.h>
#include "Common.h"

//--------------------------------------------------------------------

bool VifInitUndocumentedQuirks();

void VifFreeUndocumentedQuirks();

//--------------------------------------------------------------------

int VifDisablePreemption();

void VifEnablePreemption( __in int cookie );

//--------------------------------------------------------------------

#endif//VIFUNDOCUMENTEDQUIRKS_H
