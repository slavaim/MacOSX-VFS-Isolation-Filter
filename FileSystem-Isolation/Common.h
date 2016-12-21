//
//  Common.h
//
//  Copyright (c) 2015 Slava Imameev. All rights reserved.
//

#ifndef VFSFilter0_Common_h
#define VFSFilter0_Common_h

#include <IOKit/IOLib.h>
#include <libkern/OSAtomic.h>

#ifdef __cplusplus
extern "C" {
#endif
    
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/kauth.h>
#include <sys/vnode.h>
#include <mach/vm_types.h>
#include <kern/sched_prim.h>
#include <sys/lock.h>
#include <sys/proc.h>

#ifdef __cplusplus
}
#endif

//--------------------------------------------------------------------

#define __in
#define __out
#define __inout
#define __in_opt
#define __out_opt
#define __opt

//--------------------------------------------------------------------

//
// ASL - Apple System Logger,
// the macro requires a boolean variable isError being defined in the outer scope,
// the macro uses only ASL, in case of stampede the ASL silently drops data on the floor
//
//
// TO DO - IOSleep called after IOLog allows the system to replenish the log buffer
// by retrieving the existing entries usin syslogd
//
#define VIF_COMM_LOG_EXT_TO_ASL( _S_ ) do{\
    IOLog(" [%-7d] FltKrnLog:" ); \
    IOLog("%s %s(%u):%s: ", isError?"ERROR!!":"", __FILE__ , __LINE__, __PRETTY_FUNCTION__ );\
    IOLog _S_ ; \
}while(0);

//
// a common log
//
#if !defined(_VIF_LOG)

    #define DBG_PRINT( _S_ )   do{ void(0); }while(0);// { kprintf _S_ ; }

#else

    #define DBG_PRINT( _S_ )  do{ bool  isError = false; VIF_COMM_LOG_EXT_TO_ASL( _S_ ); }while(0);

#endif


//
// an errors log
//
#if !defined(_VIF_LOG_ERRORS)

    #define DBG_PRINT_ERROR( _S_ )   do{ void(0); }while(0);//DBG_PRINT( _S_ )

#else

    #define DBG_PRINT_ERROR( _S_ )   do{ bool  isError = true; VIF_COMM_LOG_EXT_TO_ASL( _S_ ); }while(0);

#endif


#if defined(DBG)
    #define VIF_INVALID_POINTER_VALUE ((long)(-1))
    #define VIF_DBG_MAKE_POINTER_INVALID( _ptr ) do{ (*(long*)&_ptr) = VIF_INVALID_POINTER_VALUE; }while(0);
    #define VIF_IS_POINTER_VALID( _ptr ) ( NULL != _ptr && VIF_INVALID_POINTER_VALUE != (long)_ptr )
#else//DBG
    #define VIF_DBG_MAKE_POINTER_INVALID( _ptr )  do{void(0);}while(0);
    #define VIF_IS_POINTER_VALID( _ptr ) ( NULL != _ptr )
#endif//DBG

//---------------------------------------------------------------------

//
// Calculate the address of the base of the structure given its type, and an
// address of a field within the structure.
//

#define CONTAINING_RECORD(address, type, field) ((type *)( \
    (char*)(address) - \
    reinterpret_cast<vm_address_t>(&((type *)0)->field)))


#define __countof( X )  ( sizeof( X ) / sizeof( X[0] ) )

//--------------------------------------------------------------------

#define VIF_STATIC_ARRAY_SIZE( _ARR_ ) ( (unsigned int)( sizeof(_ARR_)/sizeof(_ARR_[0] ) ) )

//--------------------------------------------------------------------

//
// an addition to the native sloppy circle implementation
//
#define	CIRCLEQ_INIT_WITH_TYPE(head, type) do {  \
(head)->cqh_first = (type *)(head); \
(head)->cqh_last = (type *)(head);  \
} while (0)


#define CIRCLEQ_INSERT_HEAD_WITH_TYPE(head, elm, field, type) do {			\
(elm)->field.cqe_next = (head)->cqh_first;			\
(elm)->field.cqe_prev = (type *)(head);				\
if ((head)->cqh_last == (type *)(head))				\
(head)->cqh_last = (elm);				\
else								\
(head)->cqh_first->field.cqe_prev = (elm);		\
(head)->cqh_first = (elm);					\
} while (0)

#define CIRCLEQ_INSERT_TAIL_WITH_TYPE(head, elm, field, type) do {			\
(elm)->field.cqe_next = (type *)(head);				\
(elm)->field.cqe_prev = (head)->cqh_last;			\
if ((head)->cqh_first == (type *)(head))			\
(head)->cqh_first = (elm);				\
else								\
(head)->cqh_last->field.cqe_next = (elm);		\
(head)->cqh_last = (elm);					\
} while (0)

//--------------------------------------------------------------------

#define FIELD_OFFSET(Type,Field) (reinterpret_cast<unsigned long long>( (&(((Type *)(0))->Field)) ) )

//--------------------------------------------------------------------

//
// a type for the vnode operations
//
typedef int (*VOPFUNC)(void *) ;
typedef int (*VFSFUNC)(void *) ;


//--------------------------------------------------------------------

//
// Double linked list manipulation functions, the same as on Windows
//

typedef struct _LIST_ENTRY {
    struct _LIST_ENTRY *Flink;
    struct _LIST_ENTRY *Blink;
} LIST_ENTRY, *PLIST_ENTRY;

inline
void
InitializeListHead(
                   __inout PLIST_ENTRY ListHead
                   )
{
    ListHead->Flink = ListHead->Blink = ListHead;
}

inline
bool
IsListEmpty(
            __in const LIST_ENTRY * ListHead
            )
{
    return (bool)(ListHead->Flink == ListHead);
}

inline
bool
RemoveEntryList(
                __in PLIST_ENTRY Entry
                )
{
    PLIST_ENTRY Blink;
    PLIST_ENTRY Flink;
    
    Flink = Entry->Flink;
    Blink = Entry->Blink;
    Blink->Flink = Flink;
    Flink->Blink = Blink;
    return (bool)(Flink == Blink);
}

inline
PLIST_ENTRY
RemoveHeadList(
               __in PLIST_ENTRY ListHead
               )
{
    PLIST_ENTRY Flink;
    PLIST_ENTRY Entry;
    
    Entry = ListHead->Flink;
    Flink = Entry->Flink;
    ListHead->Flink = Flink;
    Flink->Blink = ListHead;
    return Entry;
}

inline
PLIST_ENTRY
RemoveTailList(
               __in PLIST_ENTRY ListHead
               )
{
    PLIST_ENTRY Blink;
    PLIST_ENTRY Entry;
    
    Entry = ListHead->Blink;
    Blink = Entry->Blink;
    ListHead->Blink = Blink;
    Blink->Flink = ListHead;
    return Entry;
}

inline
void
InsertTailList(
               __in PLIST_ENTRY ListHead,
               __in PLIST_ENTRY Entry
               )
{
    PLIST_ENTRY Blink;
    
    Blink = ListHead->Blink;
    Entry->Flink = ListHead;
    Entry->Blink = Blink;
    Blink->Flink = Entry;
    ListHead->Blink = Entry;
}


inline
void
InsertHeadList(
               __in PLIST_ENTRY ListHead,
               __in PLIST_ENTRY Entry
               )
{
    PLIST_ENTRY Flink;
    
    Flink = ListHead->Flink;
    Entry->Flink = Flink;
    Entry->Blink = ListHead;
    Flink->Blink = Entry;
    ListHead->Flink = Entry;
}

inline
void
AppendTailList(
               __in PLIST_ENTRY ListHead,
               __in PLIST_ENTRY ListToAppend
               )
{
    PLIST_ENTRY ListEnd = ListHead->Blink;
    
    ListHead->Blink->Flink = ListToAppend;
    ListHead->Blink = ListToAppend->Blink;
    ListToAppend->Blink->Flink = ListHead;
    ListToAppend->Blink = ListEnd;
}

//--------------------------------------------------------------------


#endif // VFSFilter0_Common_h
