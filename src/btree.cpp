/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->scanExecuting = false;

	// construct index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // name of index file
	outIndexName = indexName;

	// open index file if it exists
	if (File::exists(indexName))
	{
		std::cout<< "index file exists\n";
		BlobFile* indexFile = new BlobFile(outIndexName, false);
		File* indexFileCastToFile = (File*) indexFile; // cast indexFile to File object
		PageId metaPageNo = indexFile->getFirstPageNo();
		std::cout<<"metaPageNo: "<<metaPageNo<<std::endl;
		Page* metaPage;
		this->bufMgr->readPage(indexFileCastToFile, metaPageNo, metaPage);
		IndexMetaInfo* metaInfo = (struct IndexMetaInfo*) metaPage;
		std::cout <<"metaInfo->rootPageNo: "<< metaInfo->rootPageNo<<std::endl;

		// check whether existing metapage data matches construction parameters
		if (metaInfo->relationName != relationName || metaInfo->attrByteOffset != attrByteOffset
				|| metaInfo->attrType != attrType)
		{
			this->bufMgr->unPinPage(indexFileCastToFile, metaPageNo, false);
			throw BadIndexInfoException("Index file exists but metapage data don't match construction parameters");
		}
		this->headerPageNum = metaPageNo;
		this->rootPageNum = metaInfo->rootPageNo;
		this->file = indexFileCastToFile;
		this->bufMgr->unPinPage(this->file, metaPageNo, false);

	} else
	{
		// create new index file
		std::cout<< "create new index file\n";
		BlobFile* indexFile = new BlobFile(indexName, true);
		File* indexFileCastToFile = (File*) indexFile;
		// create meta page
		Page* metaPage;
		PageId metaPageNo;
		// allocate meta info page
		this->bufMgr->allocPage(this->file, metaPageNo, metaPage);
		// first page of index file is meta page
		// set page number of meta page
		this->headerPageNum = metaPageNo;
		// cast metaPage to IndexMetaInfo struct and set its variables 
		IndexMetaInfo* metaInfo = (struct IndexMetaInfo*) metaPage;
		
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		strcpy(metaInfo->relationName, relationName.c_str());
		
		Page* rootPage;
		PageId rootPageNo;
		this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
		// after alloc, rootPage need not be a page object
		// so cast to leaf node
		LeafNodeInt* rootPageCastToLeaf = (LeafNodeInt *) rootPage;
		rootPageCastToLeaf->rightSibPageNo = Page::INVALID_NUMBER;
		this->bufMgr->unPinPage(this->file, rootPageNo, true);
		this->rootPageNum = rootPageNo;
		metaInfo->rootPageNo = rootPageNo;
		this->bufMgr->unPinPage(this->file, metaPageNo, true);
		// set level to 0 since root is leaf
		this->rootIsLeaf = true;
		FileScan* fScan = new FileScan(relationName, bufMgrIn);
		try
		{
			RecordId scanRid;
			while(1)
			{
				fScan->scanNext(scanRid);
				std::string recordStr = fScan->getRecord();
				const char *record = recordStr.c_str();
				int key = *((int*)(record + attrByteOffset));
				std::cout << "Extracted : " << key << std::endl;
				// this->insertEntry(&key, scanRid);
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}
		delete fScan;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	if(scanExecuting == true) {
		endScan();
	}
	this->bufMgr->flushFile(this->file);	// flushing the index file
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	if (this->rootIsLeaf == true)
	{
		this->insertIntoLeaf(key, rid, this->rootPageNum);
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	if(scanExecuting == false) {
		throw ScanNotInitializedException();
	} else {
		scanExecuting = true;
	}
}

void BTreeIndex::insertIntoLeaf(const void* key, const RecordId rid, const PageId pageNo)
{
	Page* page;
	this->bufMgr->readPage(this->file, pageNo, page);
	LeafNodeInt* leafNode = (struct LeafNodeInt*)page;
	int insertPosition;
	if (leafNode->numKeys >= INTARRAYLEAFSIZE)
	{
		std::cout<<"Insert into leaf attempted: no space available, split needed\n"<<std::endl;
		this->splitLeaf(key, rid, pageNo);

	}
	else 
	{
		int i;
		for (i = 0; i < leafNode->numKeys; i++)
		{
			if (*((int*)key) > leafNode->keyArray[i])
			{
				insertPosition = i;
				break;
			}
		}
		for (i = insertPosition; i < leafNode->numKeys; i++)
		{
			leafNode->keyArray[i+1] = leafNode->keyArray[i];
		}
		leafNode->keyArray[insertPosition] = *((int*)key);
		for (i = insertPosition; i < leafNode->numKeys; i++)
		{
			leafNode->ridArray[i+1] = leafNode->ridArray[i];
		}
		leafNode->ridArray[insertPosition] = rid;
	}


}

void BTreeIndex::insertIntoNonLeaf(const void* key, const RecordId rid, const PageId pageNo)
{
}
void BTreeIndex::splitLeaf(const void* key, const RecordId rid, const PageId pageNo)
{
}
}
