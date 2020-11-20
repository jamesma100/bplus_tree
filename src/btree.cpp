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

	// construct index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // name of index file
	outIndexName = indexName;

	// open index file if it exists; otherwise create one
	if (File::exists(indexName))
	{
		BlobFile* indexFile = new BlobFile(indexName, false);
		File* indexFileCastToFile = (File*) indexFile;
		PageId metaPageNo = 1;
		Page* metaPage;
		this->BufMgr->readPage(indexFileCastToFile, metaPageNo, metaPage);
		IndexMetaInfo* metaInfo = (IndexMetaInfo*) metaPage;

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
		return;

	} else
	{
		BlobFile* indexFile = new BlobFile(indexName, true);
	}
	
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
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

}

}
