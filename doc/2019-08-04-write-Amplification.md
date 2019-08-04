# Proposal: Reduce Write Amplification By Modifying Garbage Collection Of Titan

- Author(s):    
  -  WenJun Huang [WjHuang](https://github.com/wjhuang2016)
  -  Ning Lin [NingLin-P](https://github.com/NingLin-P)
  -  Hanyang Liu [lhy1024](https://github.com/lhy1024)
- Last updated:  2019/08/04

## Abstract

By using technique call hole-punching, we redesign the garbage collection (GC) process in Titan, to further reduce write amplification on GC

## Background

### Write amplification in TiKV

As a distributed transactional key-value database, TiKV use raft consensus protocol to provide high available and use RocksDB as the underlying storage engine. While these components work fine 
togather and provide high performence, there still have a problem that reduce IO throughout and consume CPU and disk resource, that is the write amplification problem. As our observed, while 
writing key-value pairs to TiKV, write amplification happen in the following places: 

- Raft logs. In raft consensus protocol a peer must first persist the raft logs after received a raft logs, and when these logs had committed later, a peer also need to apply these log to its state machine, in TiKV that procedure will be write to RocksDB.
- Write ahead log to level 0 SSTable. When RocksDB received a update operation it first write that operation to its write ahead log, and later when it flush memtable to disk, it will again need to write (some of) these operation to the level 0 SSTables.
- LSM tree compaction. Within LSM tree, SSTables are organized as many levels, when a SSTables from upper level flow to lower level, the SSTables in lower level whose key range are overlap with that SSTable, will need to read, merge sort and write back again to disk.

As persist raft logs is part of the raft consensus protocol to provide its correctness, it is a bit hard to remove or improve write amplification on it. But in LSM tree, as its widely use to 
build storage engine, there are so many efforts had been made to reduce write amplification on LSM tree, and so far we had readed some papers on this feild:

- Key value separation: WiscKey, HashKV.
- Delay compaction: PebblesDB, dCompaction, TRIAD-DISK in TRIAD, Universal compaction (build-in on RocksDB).
- Hot cold separation: TRIAD-MEM in TRIAD, HashKV.
- Transform WAL into SSTable: TRIAD-LOG in TRIAD.

Although RocksDB export rich interfaces and tuning parameters to allow user to customize RocksDB's behaviour, it is still so hard to implement some ideas above by using RocksDB's interface without modified its internal and that is a non-trivial work.

Instead, as we know, Titan is a RocksDB plugin implement by using RocksDB's interface, inspired by WiscKey for key-value separation, which aims to reduce write amplification in RocksDB with large values when compaction.

Titan uses a value storage for large values, and need to periodic garbage collection (GC) to reclaim space occupied by stale value in the blob file. But during garbage collection (GC), it merges the valid data in the blob files and writes it to disk on new blob files, and also writing the new location information for those valid records back to LSM tree.

And this introduces new write amplification: Even if some records are valid, we need to rewrite them to disk. In other words, though Titan reduces write amplification of the LSM tree by reducing the size of the LSM tree, it introduces new write amplification at the time of GC. We tried to reduce the write operations on disk to reduce this write amplification.

## Proposal

We proposal a new garbage collection algorithm which further reduces the write amplification. In our new garbage collection algorithm, we reduce write amplification by trying to to minimize rewriting valid record to the disk by use a technique call hole-punching to actively reclam disk space occupied by invalid records without the need of writing valid record to new blob files.

### Concepts 

#### Dig Holes

To efficiently reclaim the free space of the blob files without writing to the disk, we use a technique called hole-punching(fallocate) provided by some modern file systems(such as ext4, xfs, btrfs). Punching a hole in a file can deallocate the physical space and keep the offset untouch. By doing this, we can not only reclaim the disk space, but also avoid writing the new offsets back to the lsm-tree. 

#### Two Stage GC

In addition to the direct compaction and write valid record to new blob files like Titan did before, we perform hole-punching on blob files to reclam disk space occupied by invalid records while leaving the valid records in the same position in blob files when performing Garbage Collection (GC). 
Thus, new GC is divided into two stages:
- First, we perform hole-punching on blob files whose discardable size exceed a given threshold, and prioritize take files with larger discardable size.
- Second, we compaction and merge blob files whose size below a given threshold, and prioritize take files with smaller size.

We choose this greedy approach to select files to perform GC instead of use discardable ratio like Titan used to do, because in Titan it need to consider the disk space can reclaim (discardable size) and the valid records need to write back to disk. While in our approach, valid records will leave in the same position without the cost of writing back to disk, so we only need to reclaim disk space as much as we can. Since most of the disk space have been reclaimed by hole-punching, the resulting write amplification will be much lower than before.

### Overview

We will perform space reclamation through a two-stage GC, in which digging holes will significantly reduce write amplification.

### Change

- Blob file iterator
- Blob GC job
- Blob GC picker

### Positively influenced

- Write amplification. With a two-stage GC, this proposal will reduce write amplification.
- Reduction in LSM Tree writes. Since the offset does not change during the first phase of the GC, there is no need to write back to the LSM-tree. The reduction in LSM Tree writes both increases the speed of the GC and reduces the impact of the GC on the online business.

### Negatively influenced

- Slower read speed. Digging holes causes the records to become discontinuous, it will affect the speed of reading. However, considering that the record size of Titan is above 4k, the reading speed will not decrease much.
- Repeated digging holes. The API of fallocate will physically reclaim space only when the released space completely covers the block, usually 4k. In order to reclaim the free space of the cross-block, we had to choose to dig between the two valid records instead of  only one invalid record. This will result in repeated digging holes of the space.


## Rationale

1. Inside RocksDB(pro: good result, con: complex)
2. Put WAL and vlog together(badger) (pro: reduce WAL by 1, con: see Titan design)
3. Our design(pro: simple, reduce wa sharply in Update-intensive workload, con: change the immut 
   of blob file)
4. Incur unnecessary wa and affect performance during gc.

## Compatibility

It does not affect the compatibility with Linux. Unfortunately, the method, fallocate,  used in this proposal is limited to Linux 3.1, not to the broader system interface, such as POSIX.

## Implementation

### Dig Hole

- Who: Hanyang Liu [lhy1024](https://github.com/lhy1024)
- When: 7.23-8.12
- Step 1: Get input files after sample and parallelly dig holes in files.
- Step 2: 
We will foreach through all the records in a file and save the ending offset of the last valid record. If a new valid record is found, we will dig holes between the end offset of the last valid record and the start offset of the record.
In addition to buffering the end offset of a valid record, we will also save the key of the previous record and whether it is valid, which will reduce access to the LSM-tree and reduce decoding with record.

### File operation support

- Who: Wenjun Huang [WjHuang](https://github.com/wjhuang2016)
- When: 7.23-8.12
- Step1: Implement a file class bases on PosixRandomRWFile provide by rocksdb and provides some functions, such as seek, punch_hole. 
- Step2: Rewriter the blob file iterator to support reading from blob file with holes. 

### Blob GC picker

- Who: Ning Lin [NingLin-P](https://github.com/NingLin-P)
- When: 7.23-8.12
- Step1: according to the discardable size in each blob files, choose some files to perform hole-punching, and also choose blob files with small size to perform compaction like Titan
- Step2: While in Titan blob files are immutable after write finish and being selected to GC mean this blob file will be delete soon, but in our approch this property no longer hold and hence we need to carefully rethink about the management of blob files, especially in case of concurrent operation.

## Open issues (if applicable)

None
