# MacOSX-VFS-Isolation-Filter

## License

The license model is a BSD Open Source License. This is a non-viral license, only asking that if you use it, you acknowledge the authors, in this case Slava Imameev.

The project uses the distorm disassembler https://github.com/gdabah/distorm which is now released under BSD license.

## Design and Implementation

This is an I/O file system isolation filter for MacOS ( Mac OS X ). The idea is to intercept read and write requests and redirect them to an alternative storage. This provides an isolation layer for data flow. The possible applications for a filter are content analyzing, encryption or any advanced data flow modification.

This project is a proof of concept and has never been a subject of thorough testing.

The filter is based on the following projects

https://github.com/slavaim/MacOSX-FileSystem-Filter  
https://github.com/slavaim/MacOSX-SparseFile-KernelMode  

The MacOSX-FileSystem-Filter project is used to implement filtering for VFS layer.
The MacOSX-SparseFile-KernelMode is used to provide an alternative storage as a sparse file. 

The implementation uses a technique similar to a stackable file system by initialising vnode objects to take control over file caching and mapped file operations. Such vnode is called a covering vnode as it covers(isolates) a vnode created by an underlying file system from I/O operations. The covering vnode technique allows to implement a very flexible control over files as the isolation layer has a full control over vnodes visible to applications. The similar isolation technique for Windows is described in a series of articles from OSR - http://www.osronline.com/article.cfm?article=560  and http://www.osronline.com/article.cfm?article=571 . In case of Windows a file object (FILE_OBJECT) is initialized by the isolation layer.

When the isolation filter detects a lookup operation it creates a covering vnode, below is a call stack for this case 

 ```
    thread #9: tid = 0x1ba6, 0xffffff7faa7577fe FileSystem-Isolation`VifCoveringFsd::VifFsdHookCreateCoveringVnodeInternal(this=0xffffff803d959dc0, ap=0xffffff80c07039f8) + 30 at VifCoveringVnode.cpp:162, name = '0xffffff80356c2a80', queue = '0x0', stop reason = breakpoint 2.1
    frame #0: 0xffffff7faa7577fe FileSystem-Isolation`VifCoveringFsd::VifFsdHookCreateCoveringVnodeInternal(this=0xffffff803d959dc0, ap=0xffffff80c07039f8) + 30 at VifCoveringVnode.cpp:162
    frame #1: 0xffffff7faa757cf3 FileSystem-Isolation`VifCoveringFsd::VifReplaceVnodeByCovering(this=0xffffff803d959dc0, vnodeToCover=0xffffff80c0703d60, dvp=0xffffff8042970b40) + 419 at VifCoveringVnode.cpp:437
    frame #2: 0xffffff7faa76a25b FileSystem-Isolation`VifFsdLookupHook(ap=0xffffff80c0703b18) + 395 at VNodeHook.cpp:230
    frame #3: 0xffffff802813f2f8 kernel`lookup(ndp=0xffffff80c0703d38) + 968 at kpi_vfs.c:2783
    frame #4: 0xffffff802813ea95 kernel`namei(ndp=0xffffff80c0703d38) + 1941 at vfs_lookup.c:371
    frame #5: 0xffffff8028152005 kernel`nameiat(ndp=0xffffff80c0703d38, dirfd=<unavailable>) + 133 at vfs_syscalls.c:2920
    frame #6: 0xffffff8028129fd6 kernel`getattrlistat_internal(ctx=0xffffff80356a5970, path=<unavailable>, alp=0xffffff80c0703f28, attributeBuffer=4476501328, bufferSize=958, options=37, segflg=<unavailable>, pathsegflg=<unavailable>, fd=<unavailable>) + 294 at vfs_attrlist.c:2755
    frame #7: 0xffffff8028123227 kernel`getattrlist(p=<unavailable>, uap=<unavailable>, retval=<unavailable>) + 119 at vfs_attrlist.c:2787
    frame #8: 0xffffff802844dcb2 kernel`unix_syscall64(state=0xffffff8035484ce0) + 610 at systemcalls.c:366
 ```
 
 For VFS operation that requires to pass through request to an underlying(original) file system the isolation layer converts covering vnode to an original vnode to call the underlying file system, for example below is a stack for a call to retrieve file attributes
 
 ```
    frame #3: 0xffffff7fae557ea2 FileSystem-Isolation`VifCoveringFsd::GetCoveringFsd(mnt=0xffffff803aa89600) + 82 at VifCoveringVnode.cpp:79
    frame #4: 0xffffff7fae56c399 FileSystem-Isolation`VifVnopArgs<vnop_getattr_args_class>::getInputArguments(this=0xffffff80c5da3550, apWrapper=0xffffff80c5da35a0) + 201 at VifCoveringVnode.h:508
    frame #5: 0xffffff7fae56c0b5 FileSystem-Isolation`VifFsdGetattrHook(ap=0xffffff80c5da35e0) + 53 at VNodeHook.cpp:1742
    frame #6: 0xffffff802bf71c6b kernel`vnode_getattr [inlined] VNOP_GETATTR(vp=<unavailable>, vap=<unavailable>, ctx=<unavailable>) + 44 at kpi_vfs.c:3110
    frame #7: 0xffffff802bf71c3f kernel`vnode_getattr(vp=0xffffff803a7932d0, vap=0xffffff80c5da3858, ctx=0xffffff8038c1a530) + 79 at kpi_vfs.c:2213
    frame #8: 0xffffff802bf27c42 kernel`getattrlist_internal(ctx=0xffffff8038c1a530, vp=0xffffff803a7932d0, alp=0xffffff80c5da3f28, attributeBuffer=<unavailable>, bufferSize=<unavailable>, options=5, segflg=<unavailable>, alt_name=0x0000000000000000) + 1218 at vfs_attrlist.c:2660
    frame #9: 0xffffff802bf2a011 kernel`getattrlistat_internal(ctx=0xffffff8038c1a530, path=<unavailable>, alp=0xffffff80c5da3f28, attributeBuffer=4335917968, bufferSize=920, options=5, segflg=<unavailable>, pathsegflg=<unavailable>, fd=<unavailable>) + 353 at vfs_attrlist.c:2762
    frame #10: 0xffffff802bf23227 kernel`getattrlist(p=<unavailable>, uap=<unavailable>, retval=<unavailable>) + 119 at vfs_attrlist.c:2787
    frame #11: 0xffffff802c24dcb2 kernel`unix_syscall64(state=0xffffff8038c38640) + 610 at systemcalls.c:366
 ```
 
 When a first read or write request is received a sparse file is created to support data flow isolation for a file
 
 ```
    frame #1: 0xffffff7f83359994 FileSystem-Isolation`VifCoveringFsd::createIsolationSparseFile(this=0xffffff801444ca20, coveredVnode=0xffffff809a9abd38, vfsContext=0xffffff80144ea440) + 1764 at VifCoveringVnode.cpp:788
    frame #2: 0xffffff7f8335ac81 FileSystem-Isolation`VifCoveringFsd::rwData(this=0xffffff801444ca20, coveredVnode=0xffffff809a9abd38, args=0xffffff809a9abca0) + 801 at VifCoveringVnode.cpp:1183
    frame #3: 0xffffff7f8335dfdc FileSystem-Isolation`VifCoveringFsd::processRead(this=0xffffff801444ca20, coverdVnode=0xffffff809a9abd38, ap=0xffffff809a9abde8, result=0xffffff809a9abd28) + 1756 at VifCoveringVnode.cpp:2411
    frame #4: 0xffffff7f83376208 FileSystem-Isolation`VifFsdReadHook(ap=0xffffff809a9abde8) + 296 at VNodeHook.cpp:835
    frame #5: 0xffffff8000d690d1 kernel`vn_read(fp=0xffffff8013c92660, uio=0xffffff809a9abe70, flags=<unavailable>, ctx=0xffffff809a9abf10) + 529 at kpi_vfs.c:3246
    frame #6: 0xffffff8000feebba kernel`dofileread(ctx=0xffffff809a9abf10, fp=0xffffff8013c92660, bufp=4376717600, nbyte=8, offset=<unavailable>, flags=0, retval=<unavailable>) + 282 at kern_descrip.c:5615
    frame #7: 0xffffff8000fee923 kernel`read_nocancel(p=0xffffff8010a77000, uap=0xffffff80111ef800, retval=<unavailable>) + 115 at sys_generic.c:213
    frame #8: 0xffffff800104dcb2 kernel`unix_syscall64(state=0xffffff8010f1d700) + 610 at systemcalls.c:366
 ```
 
 The sparse file related data structures are released when a vnode is being reclaimed. The data in a sparse file is left intact and can be used later if required.
 
 ```
     frame #3: 0xffffff7fa997ff34 FileSystem-Isolation`VifSparseFile::freeDataFile(this=0xffffff803bdbf100) + 356 at VifSparseFile.cpp:866
    frame #4: 0xffffff7fa997f422 FileSystem-Isolation`VifSparseFile::free(this=0xffffff803bdbf100) + 1266 at VifSparseFile.cpp:378
    frame #5: 0xffffff7fa995a7b8 FileSystem-Isolation`VifCoveringFsd::VifReclaimCoveringVnode(this=0xffffff803221c560, coveringVnode=0xffffff803bdbe4b0) + 856 at VifCoveringVnode.cpp:1111
    frame #6: 0xffffff7fa9976458 FileSystem-Isolation`VifFsdReclaimHook(ap=0xffffff80c0413d00) + 264 at VNodeHook.cpp:756
    frame #7: 0xffffff802734b870 kernel`vclean(vp=0xffffff803bdbe4b0, flags=<unavailable>) + 656 at kpi_vfs.c:4799
    frame #8: 0xffffff802734b35a kernel`vnode_reclaim_internal [inlined] vgone(vp=0xffffff803bdbe4b0, flags=<unavailable>) + 15 at vfs_subr.c:2429
    frame #9: 0xffffff802734b34b kernel`vnode_reclaim_internal(vp=0xffffff803bdbe4b0, locked=<unavailable>, reuse=<unavailable>, flags=<unavailable>) + 251 at vfs_subr.c:4597
    frame #10: 0xffffff8027345e49 kernel`vnode_put_locked(vp=0xffffff803bdbe4b0) + 185 at vfs_subr.c:4334
    frame #11: 0xffffff8027368521 kernel`vn_closefile(fg=<unavailable>, ctx=0xffffff80c0413e60) + 177 at vfs_subr.c:4289
    frame #12: 0xffffff80275b1680 kernel`closef_locked [inlined] fo_close(fg=0xffffff8037b81c20, ctx=0xffffff8034c4d950) + 14 at kern_descrip.c:5711
    frame #13: 0xffffff80275b1672 kernel`closef_locked(fp=<unavailable>, fg=0xffffff8037b81c20, p=0xffffff8037de3960) + 354 at kern_descrip.c:4982
    frame #14: 0xffffff80275ad88e kernel`close_internal_locked(p=0xffffff8037de3960, fd=<unavailable>, fp=0xffffff80389b5240, flags=<unavailable>) + 542 at kern_descrip.c:2765
    frame #15: 0xffffff80275b13d6 kernel`close_nocancel(p=0xffffff8037de3960, uap=<unavailable>, retval=<unavailable>) + 342 at kern_descrip.c:2666
    frame #16: 0xffffff802764dcb2 kernel`unix_syscall64(state=0xffffff80348f5f80) + 610 at systemcalls.c:366
 ```
 
The current implementation of a sparse file associates a timestamp with each data block and flushes data up to a provided timestamp to original file on underlying file system when VifSparseFile::flushUpToTimeStamp is called. The timestamp is a counter and has nothing to do with system or wall clocks.

```
    errno_t VifSparseFile::flushUpToTimeStamp( __in SInt64 timeStamp,
                                __in vnode_t coveredVnode,
                                __in off_t fileDataSize,
                                __in vfs_context_t vfsContext );
```

If gFlushDataForTest is set to TRUE the data will be flushed synchronously for each write, this is a test option as name suggests. Overwise all written data will be retained in a sparse file until flushUpToTimeStamp is called. It is responsibility of an application that employs this isolation filter to decide when and how to flush data from a sparse file to original location.

Below are call stacks for read and write requests

```
    thread #6: tid = 0x191c, 0xffffff7faa776c9a FileSystem-Isolation`VifSparseFile::performIO(this=0xffffff8041c10100, descriptors=0xffffff8033583500, dscrCount=1) + 26 at VifSparseFile.cpp:4379, name = '0xffffff8035f8e130', queue = '0x0', stop reason = breakpoint 1.1
    frame #0: 0xffffff7faa776c9a FileSystem-Isolation`VifSparseFile::performIO(this=0xffffff8041c10100, descriptors=0xffffff8033583500, dscrCount=1) + 26 at VifSparseFile.cpp:4379
    frame #1: 0xffffff7faa759488 FileSystem-Isolation`VifCoveringFsd::rwData(this=0xffffff803d959dc0, coveredVnode=0xffffff80d67ebd38, args=0xffffff80d67ebca0) + 2184 at VifCoveringVnode.cpp:1454
    frame #2: 0xffffff7faa75acb7 FileSystem-Isolation`VifCoveringFsd::processRead(this=0xffffff803d959dc0, coverdVnode=0xffffff80d67ebd38, ap=0xffffff80d67ebde8, result=0xffffff80d67ebd28) + 1079 at VifCoveringVnode.cpp:2411
    frame #3: 0xffffff7faa769a4f FileSystem-Isolation`VifFsdReadHook(ap=0xffffff80d67ebde8) + 159 at VNodeHook.cpp:835
    frame #4: 0xffffff80281690d1 kernel`vn_read(fp=0xffffff80348a4d50, uio=0xffffff80d67ebe70, flags=<unavailable>, ctx=0xffffff80d67ebf10) + 529 at kpi_vfs.c:3246
    frame #5: 0xffffff80283eebba kernel`dofileread(ctx=0xffffff80d67ebf10, fp=0xffffff80348a4d50, bufp=4558647984, nbyte=335, offset=<unavailable>, flags=1, retval=<unavailable>) + 282 at kern_descrip.c:5615
    frame #6: 0xffffff80283eed32 kernel`pread_nocancel(p=0xffffff80340bf4b0, uap=0xffffff8035f4d3c0, retval=0xffffff8035f4d400) + 130 at sys_generic.c:252
    frame #7: 0xffffff802844dcb2 kernel`unix_syscall64(state=0xffffff8035f36760) + 610 at systemcalls.c:366
```

```
    thread #5: tid = 0x18ae, 0xffffff7faa776c9a FileSystem-Isolation`VifSparseFile::performIO(this=0xffffff8040b5b600, descriptors=0xffffff803356f0c0, dscrCount=1) + 26 at VifSparseFile.cpp:4379, name = '0xffffff8034d54c88', queue = '0x0', stop reason = breakpoint 1.1
    frame #0: 0xffffff7faa776c9a FileSystem-Isolation`VifSparseFile::performIO(this=0xffffff8040b5b600, descriptors=0xffffff803356f0c0, dscrCount=1) + 26 at VifSparseFile.cpp:4379
    frame #1: 0xffffff7faa759488 FileSystem-Isolation`VifCoveringFsd::rwData(this=0xffffff803d959dc0, coveredVnode=0xffffff80c1c9bce0, args=0xffffff80c1c9bc50) + 2184 at VifCoveringVnode.cpp:1454
    frame #2: 0xffffff7faa75a782 FileSystem-Isolation`VifCoveringFsd::processWrite(this=0xffffff803d959dc0, coverdVnode=0xffffff80c1c9bce0, ap=0xffffff80c1c9bd68, result=0xffffff80c1c9bcd8) + 1282 at VifCoveringVnode.cpp:2169
    frame #3: 0xffffff7faa769dfb FileSystem-Isolation`VifFsdWriteHook(ap=0xffffff80c1c9bd68) + 171 at VNodeHook.cpp:940
    frame #4: 0xffffff8028173700 kernel`VNOP_WRITE(vp=0xffffff8040b88b40, uio=0xffffff80c1c9be70, ioflag=<unavailable>, ctx=<unavailable>) + 112 at kpi_vfs.c:3287
    frame #5: 0xffffff8028168c5f kernel`vn_write(fp=0xffffff80348a4a68, uio=0xffffff80c1c9be70, flags=0, ctx=0xffffff80c1c9bf10) + 895 at vfs_vnops.c:1112
    frame #6: 0xffffff80283ef305 kernel`dofilewrite(ctx=0xffffff80c1c9bf10, fp=0xffffff80348a4a68, bufp=140518634042512, nbyte=348, offset=<unavailable>, flags=<unavailable>, retval=<unavailable>) + 309 at kern_descrip.c:5636
    frame #7: 0xffffff80283ef152 kernel`write_nocancel(p=0xffffff80340271a0, uap=<unavailable>, retval=0xffffff8035e4cb60) + 274 at sys_generic.c:476
    frame #8: 0xffffff802844dcb2 kernel`unix_syscall64(state=0xffffff8035f34700) + 610 at systemcalls.c:366
```

## Usage

To activate the isolation layer just load the kernel extension (kext) with the kextload command.
For testing purposes the isolation layer intercepts requests to files on a removable drive mounted at /Volumes/Untitled . Sparse files are created in /work/isolation directory which must exist before the kextd is being loaded.
These test settings are defined as

```
const static char* TestPathPrefix = "/Volumes/Untitled/";
...
#define TEST_ISOLATION_DIR   "/work/isolation/"
#define TEST_ISOLATION_FILE_NAME   TEST_ISOLATION_DIR"XXXXXXXX_XXXXXXXX_XXXXXXXX_XXXXXXXX_AAAAAAAA"
```
