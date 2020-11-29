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
	bufMgr->flushFile(this->file);	// flushing the index file
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	
	insertHelper(metaInfo.rootPageNo,*(int *)key,rid,0);
}
void BTreeIndex::insertHelper(PageId pageNo,int key,RecordId rid, PageId newChildPageNo){
	Page *page;
	bufMgr->readPage(file, pageNo, page);
	//leaf Node
	if(isLeaf(pageNo)){
		newChildPageNo = insertIntoLeaf((LeafNodeInt*)page,key,rid,pageNo);
	}else{
		//not root,find child
		NonLeafNodeInt* currentNode=(NonLeafNodeInt*)page;
		int childIndex=0;
		for (childIndex=0;childIndex<nonLeafNodeRecNo(currentNode);childIndex++){
			if(key<currentNode->keyArray[childIndex]){
				break;
			}
		}
		if(key>currentNode->keyArray[nonLeafNodeRecNo(currentNode)]){
			childIndex = nonLeafNodeRecNo(currentNode);
		}
		//get pageNo of child
		PageId childPageNo = currentNode->pageNoArray[childIndex];
		//recursiively insert to child
		insertHelper(childPageNo,key,rid,newChildPageNo);
		//no spliting
		if(newChildPageNo==0){
			return; 
		}else{
		//split
				//insert the first key of newNode into parent
				Page* newChildPage;
				bufMgr->readPage(file,newChildPageNo,newChildPage);
				NonLeafNodeInt* newChildNode = (NonLeafNodeInt*) newChildPage;
				newChildPageNo = insertIntoNonLeaf(currentNode,newChildNode->keyArray[0],newChildPageNo);
				//current Node is root, build a new node
				if(metaInfo.rootPageNo==pageNo){
					NonLeafNodeInt* newRootNode;
					PageId newRootPageNo;
					bufMgr->allocPage(file,newRootPageNo,(Page *&)newRootNode);	
					newRootNode->keyArray[0] = newChildNode->keyArray[0];
					newRootNode->pageNoArray[0]=pageNo;
					newRootNode->pageNoArray[1]=newChildPageNo;
					metaInfo.rootPageNo = newRootPageNo;
				}
				if(currentNode->level==1){
					//copy up, no need to change original child node
					return ;
				}else{
					//push up, delete first entry of newChildNode
					for(int i=0;i<nonLeafNodeRecNo(newChildNode)-1;i++){
						newChildNode->keyArray[i] = newChildNode->keyArray[i+1];
						return;
					}
				}
			
			
		}

		
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

PageId BTreeIndex::insertIntoLeaf(LeafNodeInt *leafNode, int key, const RecordId rid, const PageId pageNo)
{
	//find index to insert
	int insertIndex=0;
	for(insertIndex=0;insertIndex<INTARRAYLEAFSIZE+1;insertIndex++){
		if(key<leafNode->keyArray[insertIndex]){
			break;
		}
	}
	PageId newPageNo=0;
	//split if full
	if(isLeafFull(leafNode)){
		newPageNo = splitLeaf(leafNode,INTARRAYLEAFSIZE/2);
		Page* newPage;
		bufMgr->readPage(file,newPageNo,newPage);
		LeafNodeInt* newLeafNode = (LeafNodeInt*) newPage;
		//insert into original leaf node
		if(insertIndex<INTARRAYLEAFSIZE/2){
			insertIntoLeaf(leafNode,key,rid,pageNo);
		}else{
			//insert to new leaf node
			insertIntoLeaf(newLeafNode,key,rid,pageNo);
		}
	}else{
		//if leaf node is not full, shift the rest and directly insert
		for(int i=leafNodeRecNo(leafNode)-1;i>insertIndex-1;i--){
			leafNode->keyArray[i+1]=leafNode->keyArray[i];
		}
		leafNode->keyArray[insertIndex] = key;
		leafNode->ridArray[insertIndex] = rid;
	}
	//todo
	return newPageNo;
}

PageId BTreeIndex::insertIntoNonLeaf(NonLeafNodeInt *nonLeafNode,int key,PageId pid)
{
	//find index to insert
	int insertIndex = 0;
	for(insertIndex=0;insertIndex<INTARRAYNONLEAFSIZE+1;insertIndex++){
		if(key<nonLeafNode->keyArray[insertIndex]){
			break;
		}
	}
	PageId newPageNo=0;
	//split if full
	if(isNonLeafFull(nonLeafNode)){
		newPageNo = splitNonLeaf(nonLeafNode,INTARRAYNONLEAFSIZE/2);
		//insert into old non leaf node
		Page* newPage;
		bufMgr->readPage(file,newPageNo,newPage);
		NonLeafNodeInt *newNonLeafNode = (NonLeafNodeInt*) newPage;
		if(insertIndex<INTARRAYNONLEAFSIZE/2){
			insertIntoNonLeaf(nonLeafNode,key,pid);
		}else{
			//insert to new non leaf Node
			insertIntoNonLeaf(newNonLeafNode,key,pid);
		}
	}else{
		//if leaf node is not full, shift the rest and directly insert
		for(int i=INTARRAYNONLEAFSIZE-1;i>insertIndex-1;i--){
			nonLeafNode->keyArray[i+1]=nonLeafNode->keyArray[i];
		}
		nonLeafNode->keyArray[insertIndex] = key;
		nonLeafNode->pageNoArray[insertIndex+1]=pid;
	}
	return newPageNo;

}

bool BTreeIndex::isLeaf(PageId pageNo){
	Page *page;
	bufMgr->readPage(file, pageNo, page);
	return *((int *)page) == -1;
}
bool BTreeIndex::isLeafFull(LeafNodeInt *leafNode){
	// if(leafNode->ridArray[INTARRAYLEAFSIZE-1].page_number == 0
	// ||leafNode->ridArray[INTARRAYLEAFSIZE-1].slot_number == 0){
	// 	return false;
	// }else{
	// 	return true;
	// }
	return leafNodeRecNo(leafNode)==INTARRAYLEAFSIZE-1;
}
bool BTreeIndex::isNonLeafFull(NonLeafNodeInt *nonLeafNode){
	// if(nonleafNode->pageNoArray[INTARRAYNONLEAFSIZE]==0){
	// 	return false;
	// }else{
	// 	return true;
	// }
	return nonLeafNodeRecNo(nonLeafNode)==INTARRAYNONLEAFSIZE;
}

int BTreeIndex::leafNodeRecNo(LeafNodeInt *leafNode){
	int	count = 0;
	for(int i=0;i<INTARRAYLEAFSIZE;i++){
		if(leafNode->ridArray[i].page_number!=0&&leafNode->ridArray[i].slot_number!=0){
			count++;
		}else{
			break;
		}
	}
return count;
}
int BTreeIndex::nonLeafNodeRecNo(NonLeafNodeInt *nonLeafNode){
	int count=0;
	for(int i=0;i<INTARRAYNONLEAFSIZE+1;i++){
		if(nonLeafNode->pageNoArray[i]!=0){
			count++;
		}else{
			break;
		}
	}
return count;
}
PageId BTreeIndex::splitLeaf(LeafNodeInt *leafNode,int splitIndex){
	PageId newPageId;
	LeafNodeInt *newLeafNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newLeafNode);
  	memset(newLeafNode, 0, Page::SIZE);
	//TODO: insert middle value into nonleaf node

	for(int i=splitIndex;i<=leafNodeRecNo(leafNode);i++){
		//copy data to newLeafNode
		newLeafNode->keyArray[i-splitIndex]=leafNode->keyArray[i];
		newLeafNode->ridArray[i-splitIndex]=leafNode->ridArray[i];
		//remove from old leafNode
		leafNode->keyArray[i]=0;
		leafNode->ridArray[i].page_number=0;
		leafNode->ridArray[i].slot_number=0;
	}
	//copy to new node
  	// memcpy(&newLeafNode->keyArray, &leafNode->keyArray[splitIndex], (INTARRAYLEAFSIZE - splitIndex) * sizeof(int));
  	// memcpy(&newLeafNode->ridArray, &leafNode->ridArray[splitIndex], (INTARRAYLEAFSIZE - splitIndex) * sizeof(RecordId));
	// remove from old leafNode
  	// memset(&leafNode->keyArray[splitIndex], 0, (INTARRAYLEAFSIZE - splitIndex) * sizeof(int));
  	// memset(&leafNode->ridArray[splitIndex], 0, (INTARRAYLEAFSIZE - splitIndex) * sizeof(RecordId));

	//set the sibling
	newLeafNode->rightSibPageNo=leafNode->rightSibPageNo;
	leafNode->rightSibPageNo=newPageId;
	return newPageId;

}
PageId BTreeIndex::splitNonLeaf(NonLeafNodeInt *nonLeafNode,int splitIndex){
	PageId newPageId;
	NonLeafNodeInt *newNonLeafNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newNonLeafNode);
  	memset(newNonLeafNode, 0, Page::SIZE);
	for(int i=splitIndex;i<nonLeafNodeRecNo(nonLeafNode);i++){
		//copy to new node
		newNonLeafNode->keyArray[i-splitIndex]= nonLeafNode->keyArray[i];
		newNonLeafNode->pageNoArray[i-splitIndex+1] = nonLeafNode->pageNoArray[i+1];
		//remove from old nonLeafNode
		nonLeafNode->keyArray[i]=0;
		nonLeafNode->pageNoArray[i+1]=0;
	}
	//two nonleafode will be in the same level after split
	newNonLeafNode->level=nonLeafNode->level;
	return newPageId;
}
}
}
