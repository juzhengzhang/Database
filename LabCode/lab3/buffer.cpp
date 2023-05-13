/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)//构造函数，必要的初始化
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];//含一个由numBufs个BufDesc类的实例组成的数组，用于描述缓冲池中每个缓冲区的状态

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs]; //实际的缓冲池，它由numBufs个大小为数据库页面的缓冲区组成

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // 一个哈希表用于跟踪当前驻留在缓冲池中的页面。
                                        // 将文件和页面编号映射到缓冲池中的缓冲区

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {//析构函数，Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
	                 //清除所有脏页并释放缓冲池和BufDesc表
	
	for (FrameId i = 0; i < numBufs; i++)
	{
		if (bufDescTable[i].dirty)
		{
			bufDescTable[i].file->writePage(bufPool[i]);
		}
	}
	delete[] bufPool;
	delete[] bufDescTable;
	//delete hashTable;
}

void BufMgr::advanceClock()//Advance clock to next frame in the buer pool.
{
	clockHand = (clockHand + 1) % numBufs//使用模运算，使得它不会超过numBufs-1
}

void BufMgr::allocBuf(FrameId & frame) 
{
	/*Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk.Throws
		BufferExceededException if all buer frames are pinned.This private method will get called by the
		readPage() and allocPage() methods described below.Make sure that if the buer frame allocated
		has a valid page in it, you remove the appropriate entry from the hash table.*/
	bool flag = true;
	for (FrameId i = 0; i < numBufs || !flag; ++i)
	{
		advanceClock();
		if (!bufDescTable[clockHand].valid)
		{
			frame = clockHand;
			hashTable->remove(bufDescTable[clockHand].file,
				bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
			return;
		}
		if (bufDescTable[clockHand].refbit)
		{
			bufDescTable[clockHand].refbit = false;
			continue;
		}
		if (bufDescTable[clockHand].pinCnt)
			continue;
		flag = false;
		
		if (bufDescTable[clockHand].dirty)
		{
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
			
		}
		hashTable->remove(bufDescTable[clockHand].file,
			bufDescTable[clockHand].pageNo);
		bufDescTable[clockHand].Clear();
		frame = clockHand;
		return;
	}
	throw BufferExceededException();
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{/*首先，通过调用 hashtable 的 lookup() 方法来检查页面是否已经在缓冲池中，该
 方法可能会在页面不在缓冲池中时抛出 HashNotFoundException 异常，以获取一个帧号。
 根据 lookup() 调用的结果，有两种情况需要处理：
情况 1：页面不在缓冲池中。调用 allocBuf() 来分配一个缓冲帧，然后调用 file->readPage() 方法将页面从磁盘读入缓冲池帧中。
接下来，将页面插入到 hashtable 中。最后，对帧调用 Set() 方法来正确地设置它。Set() 方法会将页面的 pinCnt 设置为 1。
通过 page 参数返回指向包含页面的帧的指针。
情况 2：页面在缓冲池中。在这种情况下，设置适当的 refbit，增加页面的 pinCnt，然后通过 page 参数返回指向包含页面的帧的指针。*/
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);
		bufDescTable[frame].refbit = true;
		bufDescTable[frame].pinCnt++;
	}
	catch (HashNotFoundException&)  // 接受异常，页面不在缓冲池
	{
		allocBuf(frame);  // 分配一个新的空闲页框
		bufPool[frame] = file->readPage(pageNo);  // 从磁盘读入到这个页框
		hashTable->insert(file, pageNo, frame);  // 该页面插入哈希表
		bufDescTable[frame].Set(file, pageNo);  // 设置页框状态
	}
	page = bufPool + frame;  // 通过page返回指向该页框的指针
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{  /*减少包含 (file, PageNo) 的帧的 pinCnt，
   如果 dirty == true，设置 dirty 位。
   如果 pin count 已经为 0，抛出 PAGENOTPINNED 异常。
   如果在哈希表查找中找不到页面，什么也不做。 */
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);  // 找到(file,PageNo)所在页框
		if (!bufDescTable[frame].pinCnt)
			throw PageNotPinnedException(file->filename(),
				pageNo, frame);
		bufDescTable[frame].pinCnt--;  //表示页面所在的页框的pinCnt减一
		if (dirty)
			bufDescTable[frame].dirty = dirty;  // 将页框的dirty设置true
	}
	catch (HashNotFoundException&)
	{//如果在哈希表查找中找不到页面，则什么也不做。
	}
}

void BufMgr::flushFile(const File* file) 
{/*应该扫描bufTable中属于该文件的页面。
 对于遇到的每个页面，应该执行以下操作：
 (a) 如果页面是脏的，调用file->writePage()将页面刷新到磁盘，然后将页面的脏位设置为false，
 (b) 从哈希表中删除页面（无论页面是干净的还是脏的），
 © 调用BufDesc的Clear()方法清除页面帧。
 如果文件的某个页面被固定，抛出PagePinnedException异常。
 如果遇到文件的无效页面，抛出BadBufferException异常。*/
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) {
			if (!bufDescTable[i].valid) {//文件的无效页面
				throw BadBufferException(i,
					bufDescTable[i].dirty,
					bufDescTable[i].valid,
					bufDescTable[i].refbit)//BadBufferException(FrameId frameNoIn, bool dirtyIn, bool validIn, bool refbitIn);
			}
			if (bufDescTable[i].pinCnt) {//页面被固定
				throw PagePinnedException(file->filename(),
					bufDescTable[i].pageNo, i)
			}
			if (bufDescTable[i].dirty) {//页面是脏的
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{/*通过调用file->allocatePage()方法在指定的文件中分配一个空白页。
 这个方法将返回一个新分配的页。
 然后调用allocBuf()来获取一个缓冲池帧。
 接下来，将一个条目插入到哈希表中，并调用Set()方法来正确地设置帧。
 该方法通过pageNo参数将新分配页的页号返回给调用者，
 并通过page参数将为该页分配的缓冲帧的指针返回给调用者。*/
	FrameId frame;
	allocBuf(frame);  // 分配一个新的缓冲池帧
	bufPool[frame] = file->allocatePage(); // 返回一个空闲页面
	pageNo = bufPool[frame].page_number(); // 
	hashTable->insert(file, pageNo, frame); // 哈希表中插入该页面
	bufDescTable[frame].Set(file, pageNo);  // 设置页框状态
	page = bufPool + frame;  // 通过page参数返回指向缓冲池中包含该页面的页框的指针
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    /*这个方法从文件中删除一个特定的页。
	在从文件中删除页之前，它要确保如果要删除的页在缓冲池中分配了一个帧，那么该帧被释放，
	并相应地从哈希表中删除条目。*/
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);//如果要删除的页在缓冲池中有frame
		hashTable->remove(file, PageNo);//从哈希表中删除条目
		bufDescTable[frame].Clear();//将bufDescTable[frame]初始化，那么该帧被释放
	}
	catch (HashNotFoundException&)
	{

	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
