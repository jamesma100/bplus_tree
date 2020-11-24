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
	scanExecuting = false;

	// construct index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // name of index file
	outIndexName = indexName;

	// open index file if it exists; otherwise create one
	if (File::exists(indexName))
	{
		BlobFile* indexFile = new BlobFile(indexName, false);
		File* indexFileCastToFile = (File*) indexFile; // cast indexFile to File object
		PageId metaPageNo = 1;
		Page* metaPage;
		this->bufMgr->readPage(indexFileCastToFile, metaPageNo, metaPage);
		IndexMetaInfo* metaInfo = (IndexMetaInfo*) metaPage; 
		
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
		BlobFile* indexFile = new BlobFile(indexName, true);
		File* indexFileCastToFile = (File*) indexFile;
		// create meta page
		PageId metaPageNo;
		Page* metaPage;
		// allocate meta info page
		this->bufMgr->allocPage(this->file, metaPageNo, metaPage);
		// first page of index file is meta page
		// set page number of meta page
		this->headerPageNum = metaPageNo;
		// cast metaPage to IndexMetaInfo struct and set its variables 
		IndexMetaInfo* metaInfo = (IndexMetaInfo*) metaPage;
		relationName.copy(metaInfo->relationName,20,0);
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;

		this->headerPageNum = metaPageNo;
		PageId rootPageNo;
		Page* rootPage;
		this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
		// after alloc, rootPage need not be a page object
		// so cast to leaf node
		LeafNodeInt* rootPageCastToLeaf = (LeafNodeInt *) rootPage;
		rootPageCastToLeaf->rightSibPageNo = Page::INVALID_NUMBER;
		this->bufMgr->unPinPage(this->file, rootPageNo, true);
		this->rootPageNum = rootPageNo;
		metaInfo->rootPageNo = rootPageNo;

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
	bufMgr->flushFile(this->file);	// flushing the index file
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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

void insertIntoLeaf(const void* key, const RecordId rid, const PageId pageNo)
{
}

void insertIntoNonLeaf(const void* key, const RecordId rid, const PageId pageNo)
{
}
}
