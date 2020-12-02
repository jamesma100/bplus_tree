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
		// read existing metapage
		File* indexFileCastToFile = (File*) indexFile; // cast indexFile to File object
		Page* metaPage;
		PageId metaPageNo = 1; 
		this->bufMgr->readPage(indexFileCastToFile, metaPageNo, metaPage);
		metaInfo = (IndexMetaInfo*) metaPage;
		std::cout<<"reached metapage check\n";
		std::cout<<"metaInfo relationName: " << metaInfo->relationName[0] <<std::endl;
		IndexMetaInfo* metaInfo = (IndexMetaInfo*) metaPage;
		std::cout<<metaInfo->relationName<<std::endl;
		// check whether existing metapage data matches construction parameters
		std::cout<< "Check started\n";
		if (metaInfo->relationName != relationName || metaInfo->attrByteOffset != attrByteOffset
				|| metaInfo->attrType != attrType)
		{
			std::cout<< "Enter if statement\n";
			this->bufMgr->unPinPage(indexFileCastToFile, metaPageNo, false);
			throw BadIndexInfoException("Index file exists but metapage data don't match construction parameters");
		}
		std::cout<< "Check ended\n";
		this->headerPageNum = 1;
		this->rootPageNum = metaInfo->rootPageNo;

		this->file = indexFileCastToFile;
		this->bufMgr->unPinPage(this->file, metaPageNo, false);

	} else
	{
		// create new index file
		std::cout<< "Create new index file\n";
		BlobFile* indexFile = new BlobFile(indexName, true);
		File* indexFileCastToFile = (File*) indexFile;
		this->file = indexFileCastToFile;
		// create meta page
		Page* metaPage;
		PageId metaPageNo;
		// allocate meta info page
		this->bufMgr->allocPage(this->file, metaPageNo, metaPage);
		// first page of index file is meta page
		// set page number of meta page
		this->headerPageNum = metaPageNo;
		// cast metaPage to IndexMetaInfo struct and set its variables 
		metaInfo = (struct IndexMetaInfo*) metaPage;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		strcpy(metaInfo->relationName, relationName.c_str());
		// allocate page for root
		Page* rootPage;
		PageId rootPageNo;
		this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
		this->rootPageNum = rootPageNo;
		metaInfo->rootPageNo = rootPageNo;
		// after alloc, rootPage need not be a page object
		// so cast to leaf node
		LeafNodeInt* rootPageCastToLeaf = (LeafNodeInt *) rootPage;
		rootPageCastToLeaf->rightSibPageNo = Page::INVALID_NUMBER;
		rootPageCastToLeaf->level = -1;
		this->bufMgr->unPinPage(this->file, rootPageNo, true);
		this->bufMgr->unPinPage(this->file, metaPageNo, true);
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
				this->insertEntry(&key, scanRid);
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
	//std::cout << " metaInfo->rootPageNo: " << metaInfo->rootPageNo << std::endl;
	std::cout << " insertEntry is called on key: "<< *(int *)key << std::endl;
	insertHelper(metaInfo->rootPageNo,*(int *)key,rid,0);
}
void BTreeIndex::insertHelper(PageId pageNo,int key,RecordId rid, PageId newChildPageNo){
	Page *page;
	bufMgr->readPage(file, pageNo, page);
	//leaf Node
	if(isLeaf(pageNo)){
		newChildPageNo = insertIntoLeaf((LeafNodeInt*)page,key,rid,pageNo);
		return;
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
		if(isLeaf(childPageNo)){
			currentNode->level=1;
		}else{
			currentNode->level=0;
		}
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
				if(metaInfo->rootPageNo==pageNo){
					NonLeafNodeInt* newRootNode;
					PageId newRootPageNo;
					bufMgr->allocPage(file,newRootPageNo,(Page *&)newRootNode);	
					newRootNode->keyArray[0] = newChildNode->keyArray[0];
					newRootNode->pageNoArray[0]=pageNo;
					newRootNode->pageNoArray[1]=newChildPageNo;
					metaInfo->rootPageNo = newRootPageNo;
					return;
				}else if(isLeaf(childPageNo)){
					//copy up, no need to change original child node
					return ;
				}else{
					//push up, delete first entry of newChildNode
					for(int i=0;i<nonLeafNodeRecNo(newChildNode)-1;i++){
						newChildNode->keyArray[i] = newChildNode->keyArray[i+1];
					}
					return;
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
	if (this->scanExecuting == true)
	{
		this->endScan();
	}else{
		this->scanExecuting=true;
	}
	if ((lowOpParm!=GT && lowOpParm!=GTE) || (highOpParm!=LT && highOpParm!=LTE))
	{
		throw BadOpcodesException();
	}
	if (*((int*) lowValParm) > *((int*) highValParm))
	{
		throw BadScanrangeException();
	}
	this->lowValInt = *((int*) lowValParm);
	this->highValInt = *((int*) highValParm);
	

	NonLeafNodeInt* currentNode;
	// if root is leaf, root is the node we will search
	if (this->isLeaf(this->rootPageNum) == true)
	{
		this->currentPageNum = this->rootPageNum;
		
	}
	else
	{
		this->currentPageNum = this->rootPageNum; // start searching from root
		// traverse tree until leaf is found
		while (this->isLeaf(this->currentPageNum) == false)
		{
			this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
			currentNode = (struct NonLeafNodeInt*) this->currentPageData;
			for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				if (this->lowValInt >= currentNode->keyArray[i])
				{
					this->currentPageNum = currentNode->pageNoArray[i+1];
				}
				this->bufMgr->unPinPage(this->file,currentNode->pageNoArray[i],false);
			}
			
		}
	}
	if (this->currentPageNum == 0)
	{
		throw NoSuchKeyFoundException();
	}
	// save leaf node in this->currentPageData
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	this->nextEntry = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (this->scanExecuting == false)
	{
		throw ScanNotInitializedException();
	}
	
	//read from currentNode
	LeafNodeInt* currentNode = (LeafNodeInt*) this->currentPageData;
	std::cout << "currentNode->keyArray[0]: "<< currentNode->keyArray[0] << std::endl;
	if (currentNode->keyArray[nextEntry] <= this->highValInt)
	{
		outRid = currentNode->ridArray[nextEntry];
		std::cout << "outRid pageNo: "<< outRid.page_number << std::endl;
	}
	else
	{
		throw IndexScanCompletedException();
	}	
	
	// finish scanning current node, so move to right sibling
	if (this->nextEntry+1 >= INTARRAYLEAFSIZE)
	{
		std::cout << "moving to sibling" << std::endl;
		this->currentPageNum = currentNode->rightSibPageNo;
		this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
		currentNode = (LeafNodeInt*)this->currentPageData;
		this->nextEntry = 0; // start searching from beginning of new node
	}
	else
	{
		this->nextEntry++;
	}
	// found key entry that exists and satisfies scan criteria
	// save corresponding record in outRid
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
	std::cout << "rid.page_number: "<< rid.page_number << std::endl;
	std::cout << "rid.slot_number: "<< rid.slot_number << std::endl;

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
		newLeafNode->level=-1;
		//insert into original leaf node
		if(insertIndex<INTARRAYLEAFSIZE/2){
			insertIntoLeaf(leafNode,key,rid,pageNo);
		}else{
			//insert to new leaf node
			insertIntoLeaf(newLeafNode,key,rid,pageNo);
		}
		bufMgr->unPinPage(file, newPageNo, true);
	}else{
		//if leaf node is not full, shift the rest and directly insert
		for(int i=leafNodeRecNo(leafNode)-1;i>insertIndex-1;i--){
			leafNode->keyArray[i+1]=leafNode->keyArray[i];
		}
		leafNode->keyArray[insertIndex] = key;
		leafNode->ridArray[insertIndex] = rid;
	}
	//std::cout << "leafNode->keyArray[insertIndex]: "<< leafNode->keyArray[insertIndex] << std::endl;
	//std::cout << "leafNode->ridArray[insertIndex].page_number: "<< leafNode->ridArray[insertIndex].page_number << std::endl;
	bufMgr->unPinPage(file, pageNo, true);
	
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
		newNonLeafNode->level = nonLeafNode->level;
		if(insertIndex<INTARRAYNONLEAFSIZE/2){
			insertIntoNonLeaf(nonLeafNode,key,pid);
		}else{
			//insert to new non leaf Node
			insertIntoNonLeaf(newNonLeafNode,key,pid);
		}
		bufMgr->unPinPage(file, newPageNo, true);
	}else{
		//if leaf node is not full, shift the rest and directly insert
		for(int i=INTARRAYNONLEAFSIZE-1;i>insertIndex-1;i--){
			nonLeafNode->keyArray[i+1]=nonLeafNode->keyArray[i];
		}
		nonLeafNode->keyArray[insertIndex] = key;
		nonLeafNode->pageNoArray[insertIndex+1]=pid;
	}
	bufMgr->unPinPage(file, pid, true);
	return newPageNo;

}

bool BTreeIndex::isLeaf(PageId pageNo){
	Page *page;
	bufMgr->readPage(file, pageNo, page);
	LeafNodeInt* currNode = (LeafNodeInt*) page;
	return currNode->level==-1;
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
  	memset(newLeafNode, 0, sizeof(LeafNodeInt));
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
	newLeafNode->level = -1;
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
