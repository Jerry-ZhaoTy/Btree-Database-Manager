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
#include "exceptions/page_not_pinned_exception.h"

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType) {
    bufMgr = bufMgrIn; // initialize buffer manager with given input
    this->attributeType = attrType;  // initialize attrByteOffset and attrType
    this->attrByteOffset = attrByteOffset;

    // get the corresponding index file name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();

    // check to see if the corresponding index file exists
    if (File::exists(outIndexName)) {
        // Case: file exists, open the file.
        File *file = (File *) new BlobFile(outIndexName, false);
        // Access page with metadata of the existing file
        PageId metaPageId = 1; // metapage is always first page of the btree index file
        Page *metaPage;
        bufMgr -> readPage(file, metaPageId, metaPage);
        // casting to retrieve information
        IndexMetaInfo *metadata = (IndexMetaInfo *) metaPage;
        // check if values in metapage match with values received through constructor parameters
        if(metadata->relationName != relationName || metadata->attrByteOffset != attrByteOffset || metadata->attrType != attrType){
            // unpin the metapage after use and throw exception and print error info
            bufMgr -> unPinPage(file, metaPageId, false);
            throw BadIndexInfoException("Error: value in metapage do not match with given parameters.");
        }
        headerPageNum = metaPageId;
        rootPageNum = metadata -> rootPageNo;
        this->file = file;
        // unpin the metapage after use
        bufMgr -> unPinPage(file, metaPageId, false);
        return;
    }

    // Case: file does not exist, create it
    file = (File *) new BlobFile(outIndexName, true);
    // create the metadata (header) page and root page
    PageId metaPageId, rootPageId;
    Page *metaPage, *rootPage;
    bufMgr -> allocPage(file, metaPageId, metaPage);
    bufMgr -> allocPage(file, rootPageId, rootPage);
    // set up the index file's metadata, casting to store information
    IndexMetaInfo *metadata = (IndexMetaInfo *) metaPage;
    strcpy(metadata -> relationName, relationName.c_str());
    metadata -> attrByteOffset = attrByteOffset;
    metadata -> attrType = attrType;
    metadata -> rootPageNo = rootPageId;
    // set up the rootPage
    ((LeafNodeInt *) rootPage) -> numOccupied = 0;
    ((LeafNodeInt *) rootPage) -> rightSibPageNo = Page::INVALID_NUMBER;
    // unpin rootpage and metapage after initialization
    bufMgr -> unPinPage(file, metaPageId, true);
    bufMgr -> unPinPage(file, rootPageId, true);

    // set up necessary private variables
    headerPageNum = metaPageId;
    rootPageNum = rootPageId;
    // initialize leaf and node occupancy with integer size for insertion
    leafOccupancy = INTARRAYLEAFSIZE; 
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    
    onlyOneRoot = true;  // initialized tree to be empty at first
    scanExecuting = false;  // intialize the status of scanExecuting

    // insert entries for every tuple in the base relation using FileScan class
    FileScan fscan(relationName, bufMgr);
    try {
        RecordId scanRid;
        while(1) {
            fscan.scanNext(scanRid);
            std::string recordStr = fscan.getRecord();
            const char *record = recordStr.c_str();
            int key = *((int *)(record + attrByteOffset));
            insertEntry(&key, scanRid);
        }

    // check if reach the end of the relation file    
    } catch(EndOfFileException e) {
        // all records have been read
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    if (scanExecuting) endScan();  // End any initialized scan
    bufMgr -> flushFile(file);  // flush index file
    delete file;  // delete file instance thereby closing the index file
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    PageId pageNo = Page::INVALID_NUMBER;  // initialize pageNo, i.e. page, to be inserted
    std::vector<PageId> visitedNodes;  // a list to track all visited nodes
    searchEntry(*(int*)key, pageNo, this->rootPageNum, visitedNodes);  // we search through the tree to find a leaf page to insert in
    insertEntryLeaf(*(int*)key, rid, pageNo, visitedNodes);  // performs actual insert
}

/**
  * Helper method.
  * Searches for the node in B+ Tree where the wanted key value belongs recursively, 
  * loop through all keys in the node and stop once it finds a key that is strictly greater than the given key value.
  * When there is only one root in the tree, it defaults to return the rootPageNum.
  * @param key  Key to insert, according specification, assuming it is an int
  * @param pageNo   PageId of a Page/node, this is being passed in from caller method
  * @param rootPageNum  PageId of the Page/node we are operating on, rootPageNum is passed in to this method in the initial call to this method
  * @param visitedNodes   List of Pages, stores all visited Pages/nodes, used in splitting
  */
void BTreeIndex::searchEntry(int key, PageId &pageNo, PageId rootPageNum, std::vector<PageId> &visitedNodes){
    // When there is only one root node, the pageNo should be equal to rootPageNum
    if (onlyOneRoot) {
        pageNo = rootPageNum;
        return; 
    }

    Page* currPage;  // initialize the current page we are on
    bufMgr->readPage(file, rootPageNum, currPage);
    NonLeafNodeInt* currNode = (NonLeafNodeInt*) currPage;
    // search for the index we want by comparing key value with the keys in keyArray
    // stop once we find a key that is strictly greater than the given key value
    int i = 0;
    while (i < currNode->numOccupied) {
        if (currNode->keyArray[i] < key) {
            i++;
        } else {
            break;
        }
    }
    
    // According to btree.h, if the level of an internal node == 0, meaning that the node below current level is still an internal node, we continue search
    if (currNode->level == 0) {
        bufMgr -> unPinPage(file, rootPageNum, false);  // remember to unpin 
        visitedNodes.push_back(rootPageNum);  // add current page to the back of visitedNodes list
        searchEntry(key, pageNo, currNode->pageNoArray[i], visitedNodes);  // recurse down to next level in the tree

    // else when the level of an internal node == 1, the leaf node at the level below is a leaf page
    } else {
        bufMgr->unPinPage(file, rootPageNum, false);  // remember to unpin 
        visitedNodes.push_back(rootPageNum);  // add current page to the back of visitedNodes list
        pageNo = currNode->pageNoArray[i];  // pageNo is returned to the called method, we assign the next pageNo to pageNo
    }
}

/**
  * Helper method.
  * Inserts data entry key-rid pair into the leaf node specified by pageNo. 
  * If the leaf node has enough space, we insert. Otherwise, we split by calling splitLeaf.
  * @param key   Key to insert, according specification, assuming it is an int
  * @param rid	Record ID of a record whose entry is getting inserted into the index.
  * @param pageNo PageId of a Page/node, this is being passed in from caller method. const because we prevent modifying it
  * @param visitedNodes  List of Pages, stores all visited Pages/nodes, used in splitting
  */
void BTreeIndex::insertEntryLeaf(int key, const RecordId rid, const PageId pageNo, std::vector<PageId> &visitedNodes) {
    Page* currPage;  // page to read into
    bufMgr->readPage(file, pageNo, currPage);
    LeafNodeInt* currLeafNode = (LeafNodeInt*) currPage;  // the assumption is that a page is a node

    // Two general cases: if leaf node is not full or leaf node is full
    // 1. first check if overflow occurs, i.e. not enough open spots to insert into current leaf node, we need to perform split
    if (currLeafNode->numOccupied >= leafOccupancy) {
        bufMgr->unPinPage(file, pageNo, false);  // before insertion, unpin curr page
        splitLeaf(key, rid, pageNo, visitedNodes);  // calls helper method splitLeaf

    // 2. if there is enough open spots to insert into current leaf node, 
    // we insert key into keyArray/ridArray of current leaf node by shifting up elements keyArray/ridArray until we find the right slot to insert into 
    } else {
        // implements a sorting algorithm where we start from the end and move all elements that are greater than 'key' in keyArray upward by 1 index
        // Note: GTE is not considered since the assumption is that no duplicate key is present in the B+ tree
        int i = 0;
        while (i < currLeafNode->numOccupied) {
            // if key is greater than last position tracked, break and insert key to last tracked position, i.e. [currLeafNode->numOccupied-i]
            if (key > currLeafNode->keyArray[currLeafNode->numOccupied-1-i]) {
                break;

            // else, shift each element up by 1
            } else {
                // e.g. i=0, numOccupied=3, array[3-0]=array[3-1-0], array[3-1]=array[3-1-1]
                currLeafNode->ridArray[currLeafNode->numOccupied-1 - i + 1] = currLeafNode->ridArray[currLeafNode->numOccupied-1 - i];
                currLeafNode->keyArray[currLeafNode->numOccupied-1 - i + 1] = currLeafNode->keyArray[currLeafNode->numOccupied-1 - i];
            }
            i++;
        }
        currLeafNode->ridArray[currLeafNode->numOccupied - i] = rid;  // insert in the keyArray[currLeafNode->numOccupied - i] position
        currLeafNode->keyArray[currLeafNode->numOccupied - i] = key;
        currLeafNode->numOccupied += 1;  // increment numOccupied in curr node
        bufMgr->unPinPage(file, pageNo, true);  // after insertion, unpin curr page
        return;
    }
}

/**
  * Helper method.
  * Splits a leaf Page/node after an overflow in leaf node
  * The size, or capacity, of the new node is calculated based on leafOccupancy using the formula ceil((leafOccupancy+1)/2.
  * @param key   Key to insert, according specification, assuming it is an int
  * @param rid	Record ID of a record whose entry is getting inserted into the index.
  * @param pageNo PageId of a Page/node, this is being passed in from caller method.
  * @param visitedNodes  List of Pages, stores all visited Pages/nodes, used in splitting
  */
void BTreeIndex::splitLeaf(int key, const RecordId rid, PageId pageNo, std::vector<PageId> &visitedNodes) {
    Page* currPage;  // page to read into
    bufMgr->readPage(file, pageNo, currPage);
    LeafNodeInt* currLeafNode = (LeafNodeInt*) currPage;  // the assumption is that a page is a node

    Page* newPage;  // new page to split into
    PageId newPageNo;
    bufMgr->allocPage(file, newPageNo, newPage);
    LeafNodeInt* newLeafPage = (LeafNodeInt*) newPage;  // new leaf node

    // the curr node points to the right, i.e. pointing to the new node using rightSibPageNo, as defined in btree.h
    newLeafPage->rightSibPageNo = currLeafNode->rightSibPageNo;
    currLeafNode->rightSibPageNo = newPageNo;
    newLeafPage->numOccupied = 0;  // set numOccupied to zero for new node

    bool insertedFlag = false; // var to keep track whether the new key has been inserted or not.
    int capacity = ceil((leafOccupancy+1)/2);  // by definition, this is the maximum number of keys to split the nonleaf node into

    // moving & splitting keys
    int i = 0;
    while (i < leafOccupancy) {
        // if key is greater than the key at keyArray[leafOccupancy-1-i] in curr node
        if (key >= currLeafNode->keyArray[leafOccupancy-1-i]) {
            // if the new key hasnt been inserted yet
            if (!insertedFlag) {
                // if we can't insert in the new node, i.e. it's filled, we insert in curr node
                if (newLeafPage->numOccupied+capacity >= leafOccupancy+1) {
                    currLeafNode->numOccupied += 1;
                    currLeafNode->ridArray[leafOccupancy-i] = rid;
                    currLeafNode->keyArray[leafOccupancy-i] = key;
                    insertedFlag = true;


                // this means the key hasn't been inserted AND the new key is larger than currLeafNode->keyArray[leafOccupancy-1 - i]
                // in this case, we insert in new node first if new node has open spots
                } else {
                    newLeafPage->numOccupied += 1;
                    newLeafPage->ridArray[leafOccupancy+1-capacity-1-i] = rid;
                    newLeafPage->keyArray[leafOccupancy+1-capacity-1-i] = key;
                    insertedFlag = true;
                }
            }

            // else if the new key has been previously inserted AND new node does not have open spots left, i.e. it's filled, continue to next step
            if(newLeafPage->numOccupied+capacity >= leafOccupancy+1){
                break;
               
            // else if the new key has been previously inserted AND new node has open spots left, hence we reposition the key in keyArray[leafOccupancy-i-1] in the curr node
            } else {
                currLeafNode->numOccupied -= 1;
                newLeafPage->ridArray[leafOccupancy+1-capacity-2-i] = currLeafNode->ridArray[leafOccupancy-i-1];
                newLeafPage->keyArray[leafOccupancy+1-capacity-2-i] = currLeafNode->keyArray[leafOccupancy-i-1];
                newLeafPage->numOccupied += 1;
            }

        // else if key > currLeafNode->keyArray[leafOccupancy-1 - i]
        // this happens when the insert key can be inserted in either curr node or new node
        } else {
            // insert key,rids from curr node into new node
            if (newLeafPage->numOccupied+capacity < leafOccupancy+1) {
                currLeafNode->numOccupied -= 1;

                // copy from curr node keyArray into new node keyArray
                newLeafPage->ridArray[leafOccupancy+1-capacity-1-i] = currLeafNode->ridArray[leafOccupancy-1-i];
                newLeafPage->keyArray[leafOccupancy+1-capacity-1-i] = currLeafNode->keyArray[leafOccupancy-1-i];
                newLeafPage->numOccupied += 1;

            //  else when new node reaches maximum capacity, meaning that we filled new node, we inset in curr node
            } else {
                // implements a sorting algorithm where we start from the end and move all elements that are greater than 'key' in keyArray upward by 1 index
                currLeafNode->keyArray[leafOccupancy - i] = currLeafNode->keyArray[leafOccupancy-1 - i];
                currLeafNode->ridArray[leafOccupancy - i] = currLeafNode->ridArray[leafOccupancy-1 - i];

                // case when i goes out of range of leafOccupancy, i.e. curr node contains all keys greater than new key
                if (leafOccupancy == i+1) {
                    currLeafNode->numOccupied += 1;
                    currLeafNode->ridArray[leafOccupancy-1 - i] = rid;
                    currLeafNode->keyArray[leafOccupancy-1 - i] = key;
                    insertedFlag = true;
                }
            }
        }
        i++;
    }
    int propagateUpKey = newLeafPage->keyArray[0];  // need to propagate key up tree, so unpin curr page for buffer manager
    bufMgr->unPinPage(file, pageNo, true);
    bufMgr->unPinPage(file, newPageNo, true);

    // checks if there is only one node in this tree using our visitedNodes list, a non-empty visited nodes list means we are at least one level in depth of tree
    if (visitedNodes.size() != 0) {
        PageId parentPageNo = visitedNodes[visitedNodes.size() - 1];  // gets the parent internal node one level above
        visitedNodes.erase(visitedNodes.begin()+visitedNodes.size()-1);  // update the current visitedNodes list by erasing the parentPageNo from it since we moved up one level
        insertEntryInternal(propagateUpKey, parentPageNo, newPageNo, visitedNodes, true);  // need to insert in internal node, we pass in the parent node

    // else, we need to propagate up a key to be a new root
    } else {
        PageId rootId; // page to read into
        Page* rootPage;
        bufMgr->allocPage(file, rootId, rootPage); 
        NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;  // new root node

        rootNode->keyArray[0] = propagateUpKey;  // we insert key into this new internal node
        rootNode->pageNoArray[0] = pageNo;  
        rootNode->pageNoArray[1] = newPageNo;
        rootNode->numOccupied++;
        rootNode->level = 1;

        bufMgr->unPinPage(file, rootId, true);  // unpin curr page for buffer manager
        onlyOneRoot = false;  // set this to false since we are on internal node
        rootPageNum = rootId;  // update root for this B+ tree index

        Page * metaPage;  // initialize the meta page according to btree.h
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo* metadata = (IndexMetaInfo*) metaPage;
        metadata->rootPageNo = rootId;
        bufMgr->unPinPage(file, headerPageNum, true);  // remember to unpin
    }
}

/**
  * Helper method.
  * Inserts the key that was propagated up into the internal node specified by pageNo. 
  * If the internal node has enough space, we insert. Otherwise, we split by calling splitInternal.
  * @param key   Key to insert, according specification, assuming it is an int
  * @param pageNo PageId of a Page/node, this is being passed in from caller method.
  * @param newPageNo PageId of a Page/node created after split. const because we prevent modifying it
  * @param visitedNodes  List of Pages, stores all visited Pages/nodes, used in splitting
  * @param splitFromLeaf  Boolean, whether the split happens at an internal node 1 level above leaf node or more levels above leaf node
  */
void BTreeIndex::insertEntryInternal(int key, PageId pageNo, const PageId newPageNo, std::vector<PageId> visitedNodes, bool splitFromLeaf) {
    Page* currPage;  // page to read into
    bufMgr->readPage(file, pageNo, currPage);
    NonLeafNodeInt * currInternalNode = (NonLeafNodeInt*) currPage;  // the assumption is that a page is a node

    // Two general cases: if internal node is not full or internal node is full
    // 1. first check if overflow occurs, i.e. not enough open spots to insert into current internal node, we need to perform split
    if (currInternalNode->numOccupied > nodeOccupancy) {
        bufMgr->unPinPage(file, pageNo, false);  // before insertion, unpin curr page
        splitInternal(key, pageNo, newPageNo, visitedNodes, splitFromLeaf);  // calls helper method splitInternal

    // 2. if there is enough open spots to insert into current leaf node, 
    // we insert key into keyArray/ridArray of current leaf node by shifting up elements keyArray/ridArray until we find the right slot to insert into 
    } else {
        // When inserting internal node, we move the last element in currInternalNode->pageNoArray up by 1 index,
        // this is because the new node will always be on the left of parent node since it has smaller key values than the key we want to insert
        currInternalNode->pageNoArray[currInternalNode->numOccupied+1] = currInternalNode->pageNoArray[currInternalNode->numOccupied];

        // implements a sorting algorithm where we start from the end and move all elements that are greater than 'key' in keyArray upward by 1 index
        // Note: this is not LTE since the assumption is that no duplicate key is present in the B+ tree
        int i = 0; 
        while (i < currInternalNode->numOccupied) {
            // if key is greater than last position tracked, break and insert key to last tracked position, i.e. [currLeafNode->numOccupied-i]
            if(key > currInternalNode->keyArray[currInternalNode->numOccupied-1-i]) {
                break;

            // else, shift each element up by 1
            } else {
                // e.g. i=0, numOccupied=3, array[3-0]=array[3-1-0], array[3-1]=array[3-1-1]
                currInternalNode->pageNoArray[currInternalNode->numOccupied-1 - i + 1] =  currInternalNode->pageNoArray[currInternalNode->numOccupied-1 - i];
                currInternalNode->keyArray[currInternalNode->numOccupied-1 - i + 1] =  currInternalNode->keyArray[currInternalNode->numOccupied-1 - i];

            }
            i++;
        }

        // Insertion here, unlike insertEntryLeaf, there are two cases: 
        // if the split happens at an internal node 1 level above leaf node, we shift pageNoArray[currInternalNode->numOccupied-i+1] down by one index, 
        // and put newPageNo into that index 
        if (splitFromLeaf) {
            currInternalNode->pageNoArray[currInternalNode->numOccupied-i] = currInternalNode->pageNoArray[currInternalNode->numOccupied-i+1];
            currInternalNode->keyArray[currInternalNode->numOccupied-i] = key;
            currInternalNode->pageNoArray[currInternalNode->numOccupied-i+1] = newPageNo;
            currInternalNode->numOccupied += 1;  // increment numOccupied in curr node

        // else if the key to be inserted is not coming from a leaf node, this means that the key has already been propagated up
        } else {
            currInternalNode->pageNoArray[currInternalNode->numOccupied-i] = newPageNo;
            currInternalNode->keyArray[currInternalNode->numOccupied-i] = key;
            currInternalNode->numOccupied += 1;  // increment numOccupied in curr node
        }
        bufMgr->unPinPage(file, pageNo, true);  // after insertion, unpin curr page
        return;
    }
}

/**
  * Helper method.
  * Splits an internal Page/node after an overflow in internal node
  * The size, or capacity, of the new node is calculated based on nodeOccupancy using the formula ceil((nodeOccupancy+1)/2.
  * @param key   Key to insert, according specification, assuming it is an int
  * @param pageNo PageId of a Page/node, this is being passed in from caller method.
  * @param newPageNo PageId of a Page/node created after split. const because we prevent modifying it
  * @param visitedNodes  List of Pages, stores all visited Pages/nodes, used in splitting
  * @param splitFromLeaf  Boolean, whether the split happens at an internal node 1 level above leaf node or more levels above leaf node
  */
void BTreeIndex::splitInternal(int key, PageId pageNo, const PageId newPageNo, std::vector<PageId> &visitedNodes, bool splitFromLeaf) {
    Page* currPage;  // page to read into
    bufMgr->readPage(file, pageNo, currPage);
    NonLeafNodeInt* currInternalNode = (NonLeafNodeInt*) currPage;  // the assumption is that a page is a node

    Page* newPageTemp;  // new page to split into
    PageId newPageNoTemp;
    bufMgr->allocPage(file, newPageNoTemp, newPageTemp);
    NonLeafNodeInt* newInternalNode = (NonLeafNodeInt*) newPageTemp;  // new leaf node

    newInternalNode->numOccupied = 0;   // set numOccupied to zero for new node
    newInternalNode->level = currInternalNode->level;

    bool insertedFlag = false; // var to keep track whether the new key has been inserted or not.
    int propagateUpKey; // the key value needed to push up into the upper layer non-leaf node
    int capacity = ceil((nodeOccupancy+1)/2);  // by definition, this is the maximum number of keys to split the nonleaf node into
    // moving & splitting keys
    int i = 0;
    while (i < nodeOccupancy) {
        // if key to be inserted is less than the curr internal node's i-th key
        if (key < currInternalNode->keyArray[i]) {
            if (!insertedFlag) {
                // First, if the propagate up value has been pushed up, we reposition curr Internal Node by moving every index down by 1 for (newInternalNode->numOccupied-1) times
                if (newInternalNode->numOccupied > capacity) {
                    currInternalNode->keyArray[i-(newInternalNode->numOccupied-1)-1] = key;
                    // here, currInternalNode->pageNoArray[i-(newInternalNode->numOccupied-1)-1] is the actual left node
                    if (splitFromLeaf) {
                        currInternalNode->pageNoArray[i-(newInternalNode->numOccupied-1)-1] = currInternalNode->pageNoArray[i];
                        currInternalNode->pageNoArray[i] = newPageNo;
                    } else {
                        currInternalNode->pageNoArray[i-(newInternalNode->numOccupied-1)-1] = newPageNo;
                    }
                    currInternalNode->numOccupied ++;

                // else if the new internal node has open slots, we insert key into new key first
                } else if (newInternalNode->numOccupied < capacity) {
                    newInternalNode->keyArray[i] = key;

                    // or reassign curr Internal node's left most child node to be new internal node's left most child
                    if (splitFromLeaf) {
                        newInternalNode->pageNoArray[i] = currInternalNode->pageNoArray[i];
                        currInternalNode->pageNoArray[i] = newPageNo;
                    // then, depending on if the split happens at an internal node 1 level above leaf node,
                    // we either assign newPageNo to newInternalNode, or copy nodes in curr internal node to new internal node
                    } else {
                        newInternalNode->pageNoArray[i] = newPageNo;
                    }
                    newInternalNode->numOccupied ++;

                // else if the new internal node is full (newInternalNode->numOccupied == capacity), we split and propagate up the middle key in the curr internal node.
                // Note that when splitting an internal node, we do not need to preserve the propagate up key because it can just be pushed up 
                } else {
                    propagateUpKey = key;  // need to propagate key up in tree

                    // here, currInternalNode->pageNoArray[i] is the actual left node
                    if (splitFromLeaf) {
                        newInternalNode->pageNoArray[newInternalNode->numOccupied] = currInternalNode->pageNoArray[i];
                        currInternalNode->pageNoArray[i] = newPageNo;
                    } else {
                        newInternalNode->pageNoArray[newInternalNode->numOccupied] = newPageNo;
                    }
                    newInternalNode->numOccupied ++;
                }
                insertedFlag = true;
            }
            // at this point, the key must has been inserted, we copy old keys/nodes from currInternalNode over to newInternalNode.
            // if the propagate up value has been pushed up, we reposition curr Internal Node by moving every index down by 1 for (newInternalNode->numOccupied-1) times
            if (newInternalNode->numOccupied > capacity) {
                currInternalNode->keyArray[i-(newInternalNode->numOccupied-1)] = currInternalNode->keyArray[i];
                currInternalNode->pageNoArray[i-(newInternalNode->numOccupied-1)] =   currInternalNode->pageNoArray[i];

            // if the new internal node has open slots, we copy old keys/nodes from currInternalNode over to newInternalNode.
            } else if (newInternalNode->numOccupied < capacity) {
                newInternalNode->keyArray[i+1] = currInternalNode->keyArray[i];
                newInternalNode->pageNoArray[i+1] = currInternalNode->pageNoArray[i];
                newInternalNode->numOccupied += 1;
                currInternalNode->numOccupied -= 1;

            // else if the new internal node is full (newInternalNode->numOccupied == capacity), we split and propagate up the middle key in the curr internal node
            // Note that when splitting an internal node, we do not need to preserve the propagate up key because it can just be pushed up    
            } else {
                propagateUpKey = currInternalNode->keyArray[i];  // need to propagate key up in tree
                newInternalNode->pageNoArray[newInternalNode->numOccupied] = currInternalNode->pageNoArray[i];
                currInternalNode->numOccupied -= 1;
                newInternalNode->numOccupied += 1;
            }

        // else if key to be inserted is greater than the curr internal node's i-th key
        // note that we do not insert here, instead, we insert after this loop
        } else {            
            // if the propagate up value has been pushed up, we reposition curr Internal Node by moving every index down by 1 for (newInternalNode->numOccupied-1) times
            if (newInternalNode->numOccupied > capacity) {
                currInternalNode->keyArray[i-(newInternalNode->numOccupied-1)-1] = currInternalNode->keyArray[i];
                currInternalNode->pageNoArray[i-(newInternalNode->numOccupied-1)-1] = currInternalNode->pageNoArray[i];

            // else if the new internal node has open slots, we insert key into new key first    
            } else if (newInternalNode->numOccupied < capacity) {
                newInternalNode->keyArray[i] =  currInternalNode->keyArray[i];
                newInternalNode->pageNoArray[i] =  currInternalNode->pageNoArray[i];
                newInternalNode->numOccupied += 1;
                currInternalNode->numOccupied -= 1;

            // else if the new internal node is full (newInternalNode->numOccupied == capacity), we propagate up the middle key in the curr internal node.
            // Note that when splitting an internal node, we do not need to preserve the propagate up key because it can just be pushed up        
            } else {
                propagateUpKey =  currInternalNode->keyArray[i];  // propagate this key up
                newInternalNode->pageNoArray[newInternalNode->numOccupied] = currInternalNode->pageNoArray[i];
                currInternalNode->numOccupied -= 1;
                newInternalNode->numOccupied += 1;
            }
        }
        i++;
    }

    // At this point, keys and internal nodes have been shifted, except for the node at currInternalNode->pageNoArray[nodeOccupancy], 
    // therefore, we need to manually move this last node to the last position in the currInternalNode after the insertion and splitting
    currInternalNode->pageNoArray[currInternalNode->numOccupied] =  currInternalNode->pageNoArray[nodeOccupancy];

    // Here, we continue the check at line 496, 
    // This is a last check to see if the key has been inserted, if not, it is because the key to be inserted is greater than all keys in currInternal node
    if (!insertedFlag) {
        currInternalNode->pageNoArray[currInternalNode->numOccupied] = key;
        if (splitFromLeaf) {
            currInternalNode->pageNoArray[currInternalNode->numOccupied+1] = newPageNo;
        } else {
            // here, currInternalNode->pageNoArray[currInternalNode->numOccupied] is actually the right node
            currInternalNode->pageNoArray[currInternalNode->numOccupied+1] = currInternalNode->pageNoArray[currInternalNode->numOccupied];
            currInternalNode->pageNoArray[currInternalNode->numOccupied] = newPageNo;
        }
        currInternalNode->numOccupied++;
        insertedFlag = true;
    }

    bufMgr->unPinPage(file, pageNo, true);
    bufMgr->unPinPage(file, newPageNoTemp, true);

    // checks if there is only one node in this tree using our visitedNodes list, a non-empty visited nodes list means we are at least one level in depth of tree
    if (visitedNodes.size() != 0) {
        PageId parentPageNo = visitedNodes[visitedNodes.size() - 1];  // gets the parent internal node one level above
        visitedNodes.erase(visitedNodes.begin() + visitedNodes.size() - 1);  // update the current visitedNodes list by erasing the parentPageNo from it since we moved up one level
        insertEntryInternal(propagateUpKey, parentPageNo, newPageNoTemp, visitedNodes, false);  // need to insert in internal node, we pass in the parent node

    // else, we need to propagate up a key to be a new root
    } else {
        PageId rootId; // empty page to allocate or is already in buffer pool
        Page* rootPage;
        bufMgr->allocPage(file, rootId, rootPage); 
        NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;  // new root node

        rootNode->keyArray[0] = propagateUpKey;  // we insert key into this new internal node
        rootNode->pageNoArray[0] = newPageNoTemp;
        rootNode->pageNoArray[1] = pageNo;
        rootNode->numOccupied++;
        rootNode->level = 0;

        bufMgr->unPinPage(file, rootId, true);  // unpin curr page for buffer manager
        rootPageNum = rootId;  // update root for this B+ tree index

        Page * metaPage;  // initialize the meta page according to btree.h
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo* metadata = (IndexMetaInfo*) metaPage;
        metadata->rootPageNo = rootId;
        bufMgr->unPinPage(file, headerPageNum, true);  // remember to unpin
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
	if (scanExecuting) endScan();  //end last scan if needed 
	scanExecuting = true;  //begin new scan

	//check if the lower bound and higher bound are vaild
	if (*((int*)lowValParm) > *((int*)highValParm))
		throw BadScanrangeException();  //if lowValue > highValue, throw the exception BadScanrangeException

	//set up class variables
	lowValInt = *((int*) lowValParm);
	highValInt = *((int*) highValParm);

	//if lowOperator not belong to GT or GTE, throw BadOpcodesException
	if (lowOpParm != GT && lowOpParm != GTE)
		throw BadOpcodesException();
	//if highOperator not belong to LT or LTE, throw BadOpcodesException
	if (highOpParm != LT && highOpParm != LTE)
		throw BadOpcodesException();
    
	lowOp = lowOpParm;  //set up class variables
	highOp = highOpParm;
	PageId pageNo; // store the lowest value in the boundry if founded
	std::vector<PageId> RootToLeafPath; // store the root to lead path (without the lead node)
	searchEntry(*((int*) lowValParm), pageNo, rootPageNum, RootToLeafPath);  // search and get a leaf page

	currentPageNum = pageNo;
	bufMgr->readPage(file, currentPageNum, currentPageData);
	LeafNodeInt* leafNode = (LeafNodeInt*) currentPageData;

	int i = 0;
	nextEntry = -1; // an initial value for nextEntry for test
	// go through values starting from the head of the current page
	while (i < leafNode->numOccupied) {
		if (leafNode->keyArray[i] < lowValInt){
			i++;	
		} else {
			//if a value equal to the lowValInt, it might not in the give range
			if (lowOp != GTE && leafNode->keyArray[i] == lowValInt){
				i++;
				continue;
			}
			//in other cases, we know that current value is the start value of scan
			else{
				nextEntry = i;
				break;
			}
		}
	}

	// if nextEntry == leafNode->numOccupied, that means all the records
	// in current page are not satisfiled the given range
	if (nextEntry == leafNode->numOccupied) {
		bufMgr -> unPinPage(file, currentPageNum, false);
		endScan();
		throw NoSuchKeyFoundException();
	}
	
	//check if it satisfied the given upper boundry, 
	//since it is possible that the value is greater than the lower boundry
	//but also greater than the upper boundry 
	if (leafNode->keyArray[nextEntry] > highValInt ||
		(leafNode->keyArray[nextEntry] == highValInt && highOp == LT)) {
		bufMgr->unPinPage(file, currentPageNum, false);
		endScan();
		throw NoSuchKeyFoundException();
	}

	bufMgr->unPinPage(file, currentPageNum, false);  //unpin the current page
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) {
    //throw exception if there is no executing scan
    if (!scanExecuting)
        throw ScanNotInitializedException();

    bufMgr->readPage(file, currentPageNum, currentPageData);  //read current page

    // check if we reached the last node within our range or if we reached an invalid node, the scan is complete in either case
    if (nextEntry == -1) {
        bufMgr -> unPinPage(file, currentPageNum, false);
        throw IndexScanCompletedException();
    }

    LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;  //get the current Node

    // use nextEntry to get the corresponding record id from the currentNode as the returned value
    outRid = currentNode->ridArray[nextEntry];

    // In the current node, there might exist another valid value
    if ((nextEntry +1) < currentNode->numOccupied) {
        //in this case, the next scan is invaild
        if ((currentNode->keyArray[nextEntry +1] == highValInt) &&
            (highOp  != LTE)){
            bufMgr->unPinPage(file, currentPageNum, false);
            //for the next call of scanNext, it would just end the Scan
            nextEntry = -1;
            return;
        } else if (currentNode->keyArray[nextEntry +1] <= highValInt) {
            nextEntry += 1;
            bufMgr->unPinPage(file, currentPageNum, false);
            return;
        } else {
            bufMgr->unPinPage(file, currentPageNum, false);
            //for the next call of scanNext, it would just end the Scan, since the next value is not in the range
            nextEntry = -1;
            return;
        }
    } else {
        if (currentNode->rightSibPageNo == Page::INVALID_NUMBER) {
            bufMgr->unPinPage(file, currentPageNum, false);
            //for the next call of scanNext, it would just end the Scan, since the next value is not in the range
            nextEntry = -1;
            return;
        } else {
            bufMgr->unPinPage(file, currentPageNum, false);
            //update to the next page
            currentPageNum = currentNode->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);

            //update the current node that we are currently go through
            currentNode = (LeafNodeInt*) currentPageData;

            if (currentNode->numOccupied == 0) {
                nextEntry = -1;
                bufMgr->unPinPage(file, currentPageNum, false);
                return;
            }
            if ((currentNode->keyArray[0] == highValInt) &&
                highOp  != LTE){
                bufMgr->unPinPage(file, currentPageNum, false);
                //for the next call of scanNext, it would just end the Scan
                nextEntry = -1;
                return;
            } else if (currentNode->keyArray[0] < highValInt) {
                nextEntry = 0;
                bufMgr->unPinPage(file, currentPageNum, false);
                return;
            } else {
                bufMgr->unPinPage(file, currentPageNum, false);
                //for the next call of scanNext, it would just end the Scan, since the next value is not in the range
                nextEntry = -1;
                return;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
	//throw exception if there is no executing scan
	if (!scanExecuting)
		throw ScanNotInitializedException();
	scanExecuting = false;
	//no need to unpin pages since we already unpinned in scanNext and startScan
}
}
