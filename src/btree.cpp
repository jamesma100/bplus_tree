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
		relationName.copy(metaInfo.relationName, 20, 0);
		std::cout<< "index file exists\n";
		BlobFile* indexFile = new BlobFile(outIndexName, false);
		// read existing metapage
		File* indexFileCastToFile = (File*) indexFile; // cast indexFile to File object
		Page* metaPage;
		PageId metaPageNo = indexFile->getFirstPageNo(); 
		this->bufMgr->readPage(indexFileCastToFile, metaPageNo, metaPage);
		std::cout<<"reached metapage check\n";
		std::cout<<"metaInfo relationName: " << metaInfo.relationName[0] <<std::endl;
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
		this->headerPageNum = metaPageNo;
		this->rootPageNum = metaInfo->rootPageNo;
std::cout<<"if: metaInfo pageNo: " << metaPageNo <<std::endl;
std::cout<<"if: root pageNo: " <<  metaInfo->rootPageNo<<std::endl;
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
		metaInfo.attrByteOffset = attrByteOffset;
		metaInfo.attrType = attrType;
		strcpy(metaInfo.relationName, relationName.c_str());
		// allocate page for root
		Page* rootPage;
		PageId rootPageNo;
		this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
		this->rootPageNum = rootPageNo;
		metaInfo.rootPageNo = rootPageNo;
		// after alloc, rootPage need not be a page object
		// so cast to leaf node
		LeafNodeInt* rootPageCastToLeaf = (LeafNodeInt *) rootPage;
		rootPageCastToLeaf->rightSibPageNo = Page::INVALID_NUMBER;
		rootPageCastToLeaf->level = -1;
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
			std::cout << "inside btree Read all records" << std::endl;
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
PageId BTreeIndex::insertHelper(PageId pageNo,int key,RecordId rid, PageId newChildPageNo){
	Page *page;
	bufMgr->readPage(file, pageNo, page);
	//leaf Node
	if(isLeaf(pageNo)){
		newChildPageNo = insertIntoLeaf((LeafNodeInt*)page,key,rid,pageNo);
		if(newChildPageNo!=0 && metaInfo.rootPageNo == pageNo){
		std::cout << "split leaf which is also root" << std::endl;
			Page *newChildPage;
			bufMgr->readPage(file,newChildPageNo,newChildPage);
			LeafNodeInt* newChildNode = (LeafNodeInt*) newChildPage;
			newChildNode->level=-1;
			Page *newRootPage;
			PageId newRootPageNo;
			bufMgr->allocPage(file,newRootPageNo,newRootPage);
			NonLeafNodeInt* newRootNode = (NonLeafNodeInt*) newRootPage;
			metaInfo.rootPageNo = newRootPageNo;
			newRootNode->level = 1;
			newRootNode->keyArray[0] = newChildNode->keyArray[0];
			newRootNode->pageNoArray[0]=pageNo;
			newRootNode->pageNoArray[1]=newChildPageNo;
			newChildPageNo=0;//already resolve split
			//std::cout << "metaInfo.rootPageNo: "<<metaInfo.rootPageNo<< std::endl;
		}
		return newChildPageNo;
	}else{
		//not leaf,find child
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
		std::cout << "childIndex: " << childIndex << std::endl;
		//get pageNo of child
		PageId childPageNo = currentNode->pageNoArray[childIndex];
		if(isLeaf(childPageNo)){
			currentNode->level=1;
		}else{
			currentNode->level=0;
		}
		if(newChildPageNo!=0){
			Page* newChildPage;
			bufMgr->readPage(file,newChildPageNo,newChildPage);
			LeafNodeInt* newChildNode = (LeafNodeInt*) newChildPage;
			newChildNode->level=-1;
			newChildPageNo=insertIntoNonLeaf(currentNode,newChildNode->keyArray[0],newChildPageNo);
			bufMgr->unPinPage(file,newChildPageNo,false);
		}
		//recursiively insert to child
		newChildPageNo = insertHelper(childPageNo,key,rid,newChildPageNo);
		//no spliting
		if(newChildPageNo==0){
			return newChildPageNo; 
		}else{
		//split
		std::cout << "split happened"<< std::endl;
				//insert the first key of newNode into parent
				Page* newChildPage;
				bufMgr->readPage(file,newChildPageNo,newChildPage);
				NonLeafNodeInt* newChildNode = (NonLeafNodeInt*) newChildPage;
				newChildPageNo = insertIntoNonLeaf(currentNode,newChildNode->keyArray[0],newChildPageNo);
				//current Node is root, build a new node
				if(newChildPageNo!=0 && metaInfo.rootPageNo==pageNo){
					NonLeafNodeInt* newRootNode;
					PageId newRootPageNo;
					bufMgr->allocPage(file,newRootPageNo,(Page *&)newRootNode);	
					newRootNode->keyArray[0] = newChildNode->keyArray[0];
					newRootNode->pageNoArray[0]=pageNo;
					newRootNode->pageNoArray[1]=newChildPageNo;
					metaInfo.rootPageNo = newRootPageNo;
					if(isLeaf(pageNo)){
						newRootNode->level=1;
					}else{
						newRootNode->level=0;
					}
				}
				if(isLeaf(childPageNo)){
					//copy up, no need to change original child node
					return newChildPageNo;
				}else{
					//push up, delete first entry of newChildNode
					for(int i=0;i<nonLeafNodeRecNo(newChildNode)-1;i++){
						newChildNode->keyArray[i] = newChildNode->keyArray[i+1];
					}
					bufMgr->unPinPage(file,newChildPageNo,true);
					return newChildPageNo;
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
	this->lowOp = lowOpParm;
	this->highOp = highOpParm;

	NonLeafNodeInt* currentNode;
	// find the leaf ndoe to begin search
	// if root is leaf, root is the node we will search
	if (this->isLeaf(this->rootPageNum) == true)
	{
		this->currentPageNum = this->rootPageNum;
		this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	}
	else
	{
		this->currentPageNum = this->rootPageNum; // start searching from root
		// traverse tree until leaf is found
		while (this->isLeaf(this->currentPageNum) == false)
		{
			this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
			currentNode = (struct NonLeafNodeInt*) this->currentPageData;
			for (int i = 0; i < nonLeafNodeRecNo(currentNode); i++)
			{
				if (lowOpParm == GT)
				{
					if (currentNode->keyArray[i] <= this->lowValInt)
					{
						this->currentPageNum = currentNode->pageNoArray[i+1];
					}
					else 
					{
						this->currentPageNum = currentNode->pageNoArray[i];
					}
					this->bufMgr->unPinPage(this->file,currentNode->pageNoArray[i],false);
					break;
				}
				else if (lowOpParm == GTE)
				{
					if (currentNode->keyArray[i] < this->lowValInt)
					{
						this->currentPageNum = currentNode->pageNoArray[i+1];
					}
					else
					{
						this->currentPageNum = currentNode->pageNoArray[i];
					this->bufMgr->unPinPage(this->file, currentNode->pageNoArray[i], false);
					}
					break;
				}
			}
		}
	}
	if (this->currentPageNum == 0)
	{
		throw NoSuchKeyFoundException();
	}
	
	// save leaf node in this->currentPageData
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	this->nextEntry = 0; // just initialized scan, so next entry to insert is at slot 0 of page
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
	// next entry still satisfies criteria
	if (currentNode->keyArray[nextEntry] <= this->highValInt)
	{
		// outRid.page_number = this->currentPageNum;
		// outRid.slot_number = this->nextEntry;
		outRid = currentNode->ridArray[nextEntry];
	}
	else
	{
		throw IndexScanCompletedException();
	}	
	
	// finish scanning current node, so move to right sibling
	if (this->nextEntry+1 >= INTARRAYLEAFSIZE)
	{
		std::cout << "scanNext moving to sibling" << std::endl;
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
	//find index to insert
	int insertIndex=0;
	for(int insertIndex=0;insertIndex<leafNodeRecNo(leafNode);insertIndex++){
		if(key<leafNode->keyArray[insertIndex]){
			break;
		}
	}
	if(key>leafNode->keyArray[leafNodeRecNo(leafNode)-1]){
		insertIndex=leafNodeRecNo(leafNode);
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
	}else{
		//if leaf node is not full, shift the rest and directly insert
		
		for(int i=leafNodeRecNo(leafNode)-1;i>=insertIndex;i--){
			leafNode->keyArray[i+1]=leafNode->keyArray[i];
			leafNode->ridArray[i+1]=leafNode->ridArray[i];
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
	if(key>nonLeafNode->keyArray[nonLeafNodeRecNo(nonLeafNode)-1]){
		insertIndex=nonLeafNodeRecNo(nonLeafNode);
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
	}else{
		//if leaf node is not full, shift the rest and directly insert
		for(int i=nonLeafNodeRecNo(nonLeafNode)-1;i>=insertIndex;i--){
			nonLeafNode->keyArray[i+1]=nonLeafNode->keyArray[i];
			nonLeafNode->pageNoArray[i+2]=nonLeafNode->pageNoArray[i+1];
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
	//bufMgr->unPinPage(file,pageNo,false);
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
		if(nonLeafNode->keyArray[i]!=0){
			count++;
		}else{
			break;
		}
	}
return count;
}
PageId BTreeIndex::splitLeaf(LeafNodeInt *leafNode,int splitIndex){
std::cout << "splitLeaf" << std::endl;
	PageId newPageId;
	LeafNodeInt *newLeafNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newLeafNode);
  	memset(newLeafNode, 0, sizeof(LeafNodeInt));

	for(int i=splitIndex;i<INTARRAYLEAFSIZE;i++){
		//copy data to newLeafNode
		newLeafNode->keyArray[i-splitIndex]=leafNode->keyArray[i];
		//std::cout << "newLeafNode->keyArray[i-splitIndex]: "<<newLeafNode->keyArray[i-splitIndex] << std::endl;
		newLeafNode->ridArray[i-splitIndex]=leafNode->ridArray[i];
		//remove from old leafNode
		leafNode->keyArray[i]=0;
		leafNode->ridArray[i].page_number=0;
		leafNode->ridArray[i].slot_number=0;
	}

	//set the sibling
	newLeafNode->rightSibPageNo=leafNode->rightSibPageNo;
	leafNode->rightSibPageNo=newPageId;
	newLeafNode->level = -1;
std::cout << "newPageId: "<<newPageId << std::endl;
	return newPageId;

}
PageId BTreeIndex::splitNonLeaf(NonLeafNodeInt *nonLeafNode,int splitIndex){
	PageId newPageId;
	NonLeafNodeInt *newNonLeafNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newNonLeafNode);
  	memset(newNonLeafNode, 0, Page::SIZE);
	for(int i=splitIndex;i<INTARRAYNONLEAFSIZE;i++){
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
