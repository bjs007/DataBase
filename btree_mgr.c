#include "dberror.h"
#include "btree_mgr.h"
#include "tables.h"
#include "string.h"
#include "btree_helper.h"



RC initIndexManager (void *mgmtData){
    initStorageManager();
    return RC_OK;
}


RC shutdownIndexManager (){
    int status;
    status = shutdownStorageManager();
   if (status == RC_OK)
           return(status);
    return(RC_OK);
}

RC createBtree (char *idxId, DataType keyType, int serachedNode){
 
    RC status;
    SM_FileHandle fileHandler;
    int nodenum = serachedNode;
    RC nodepresent = (((PAGE_SIZE - sizeof(Node))/sizeof(NodeElement))-1);
    if ( nodenum > nodepresent)
        return(RC_FILE_NOT_FOUND);

    char data[PAGE_SIZE];
    char *pointer= data;
    createIndents(pointer,keyType,serachedNode);

status = 100; // assigning an arbitary value
    if ((status=createPageFile(idxId)) != RC_OK)
        return status;
status = 100;
    if ((status=openPageFile(idxId, &fileHandler)) != RC_OK)
        return status;
status = 100;
    if ((status=writeBlock(0, &fileHandler, data)) != RC_OK)
        return status;
status = 100;
    if ((status=closePageFile(&fileHandler)) != RC_OK)
        return status;

    return(RC_OK);
}


RC openBtree (BTreeHandle **tree, char *idxId){
    *tree= (BTreeHandle*) malloc( sizeof(BTreeHandle) );
     memset(*tree, 0, sizeof(BTreeHandle));
    TreeManager *treeManager=(TreeManager*) malloc( sizeof(TreeManager) );
    RC status;
    (*tree)->mgmtData= (void*) treeManager;
// initiating BufferPool from buffer manager with FIFO Strategy
     status = initBufferPool(&treeManager->bufferPool, idxId, 1000,RS_FIFO, NULL);
    if ( status == RC_OK) {
          prepareSchema(tree,treeManager);
        return(RC_OK);
    }
   else
	{
	return -1;
	}
      return status;
}


void prepareSchema(BTreeHandle **tree, TreeManager *treeManager){
	int sizeofint = sizeof(int);        
	char *pointer;
        
        pointer= (char*) retrieveNode(*tree, (PageNumber)0);

        treeManager->top= *(PageNumber*)pointer;

        pointer=  pointer + sizeofint;

        treeManager->nCount= *(int*)pointer;

        pointer= pointer + sizeofint;

        treeManager->numOfElem= *(int*)pointer;

        pointer= pointer + sizeofint;

        treeManager->keyType= *(int*)pointer;

        pointer= pointer + sizeofint;

        treeManager->level= *(int*)pointer;

        pointer= pointer + sizeofint;

        unHookNode(*tree, (PageNumber)0);
}

void deAllocateSchema(TreeManager * treeManager, BTreeHandle *tree){
     char *pointer;
     int sizeofint = sizeof(int);  
    pointer= (char*) retrieveNode(tree, (PageNumber)0);
  *(int*)pointer= treeManager->top;
    pointer= pointer + sizeofint;
    *(int*)pointer= treeManager->nCount;
    pointer= pointer + sizeofint;
    *(int*)pointer= treeManager->numOfElem;
    pointer= pointer + sizeofint;
    unHookNode(tree, (PageNumber)0);
}


RC closeBtree (BTreeHandle *tree){

    TreeManager *treeManager;
    RC status;
    status = RC_OK;
    treeManager= (TreeManager*) tree->mgmtData;
    markDirty(&treeManager->bufferPool, &treeManager->pageHandler);
    deAllocateSchema(treeManager,tree);
    shutdownBufferPool(&treeManager->bufferPool);
    free(treeManager);
    free(tree);
    return(status);

}

RC deleteBtree (char *idxId)
{
    RC status =RC_OK;
       status = destroyPageFile(idxId);
    if (status != RC_OK)
        return status;
    return status;
}


RC getNumNodes (BTreeHandle *tree, int *result){
    TreeManager *treeManager;
    treeManager= (TreeManager*) tree->mgmtData;
    *result= treeManager->nCount;
    return(RC_OK);
}
RC getNumEntries (BTreeHandle *tree, int *result){
    *result= ((TreeManager*) tree->mgmtData)->numOfElem;
    return RC_OK ;
}


RC getKeyType (BTreeHandle *tree, DataType *result){
    *result= ((TreeManager*) tree->mgmtData)->keyType;
    return RC_OK;

}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    
    RC pos;
    Node *nodetosearch, *node_found;
    RC rc;
    TreeManager *tManager= (TreeManager*) tree->mgmtData;
    PageNumber Res;
    nodetosearch= retrieveNode(tree, tManager->top);
    Res= tManager->top;
    node_found= searchElm (tree, nodetosearch, key, &pos, &Res, 0);
    unHookNode(tree, tManager->top);
    if (!node_found)
        return(RC_READ_ERROR);

   
    return(destroy(tree, Res, key));
}


RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    
    TreeScanner *tScan;
    int sizeofBTSchanHandle = sizeof(BT_ScanHandle);
    (*handle) = (BT_ScanHandle*) malloc(sizeofBTSchanHandle);
    memset(*handle, 0, sizeofBTSchanHandle);

    tScan= (TreeScanner*) malloc(sizeof(TreeScanner));
    memset(tScan, 0, sizeof(TreeScanner));

    (*handle)->tree= tree;
    (*handle)->mgmtData= tScan;
    tScan->pageNumber= -1;
    tScan->curPos= -1;

    return(RC_OK);
}
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
 
   int curpos,numKeys;
    TreeScanner *tScan= handle->mgmtData;
    TreeManager *tManager= handle->tree->mgmtData;
    Node *nodetosearch;
    NodeElement *element;
    PageNumber p;
    PageNumber tempPageNumber;

    if (!tManager->numOfElem)
        return(RC_IM_NO_MORE_ENTRIES);

    if (tScan->pageNumber == -1)
    {
        p= tManager->top;
        nodetosearch= retrieveNode(handle->tree, p);
        while (!nodetosearch->leaf)
        {
           tempPageNumber= nodetosearch->elementPointer.index;
           unHookNode(handle->tree, p);
           p= tempPageNumber;
           nodetosearch= retrieveNode(handle->tree, p);
        }

        tScan->pageNumber= p;
        tScan->curPos= 0;
        tScan->curNode= nodetosearch;
    }
    nodetosearch= tScan->curNode;
curpos = tScan->curPos;
numKeys = nodetosearch->numKeys;
    if (curpos == numKeys )
    {
        if (nodetosearch->nodePtr == -1)
        {
            unHookNode(handle->tree, tScan->pageNumber);
            tScan->pageNumber= -1;
            tScan->curPos= -1;
            tScan->curNode= NULL;

            return(RC_IM_NO_MORE_ENTRIES);
        }

        p= nodetosearch->nodePtr;
        unHookNode(handle->tree, tScan->pageNumber);
    	tScan->pageNumber= p;
        tScan->curNode= retrieveNode(handle->tree, p);
        nodetosearch=tScan->curNode;
        tScan->curPos= 0;


    }

    element= &nodetosearch->elementPointer;
    *result= *((RID*) &element[tScan->curPos].index);
    tScan->curPos++;

    return(RC_OK);
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    RC rc;
    TreeScanner *treeScan= handle->mgmtData;
    if (treeScan->pageNumber != -1)
    {
        unHookNode(handle->tree, treeScan->pageNumber);
        treeScan->pageNumber= -1;
    }

    free(handle->mgmtData);
    free(handle);

    return(RC_OK);
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    RC rc;
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;

    int elemPos= -1, cnt;
    Node *nodefound;
    Node *tempNode;
    NodeElement *elementPointer;
    Value resVal;
    PageNumber pageNum, pageNumber;


    if (!treeManager->numOfElem)
    {
       pageNum= createBTNode(tree);
       treeManager->top= pageNum;
       tempNode= retrieveNode(tree, pageNum);
       tempNode->leaf= 1;
       tempNode->parent= -1;
       tempNode->nodePtr= -1;
       addKey(tree, tempNode, *((long long*) &rid), key);
       unHookNode(tree, pageNum);

       return(RC_OK);
    }

    nodefound= retrieveNode(tree, treeManager->top);
    pageNumber= treeManager->top;
    nodefound= searchElm (tree, nodefound, key, &elemPos, &pageNumber, 1);
    elementPointer= &nodefound->elementPointer;
    valueEquals(&elementPointer[elemPos].key, key, &resVal);
    unHookNode(tree, treeManager->top);
    if ((resVal.v.boolV))
      return(RC_READ_ERROR);
    nodefound= retrieveNode(tree, pageNumber);
    cnt= addKey(tree, nodefound, *((long long*) &rid), key);
    if (nodefound->numKeys <= (treeManager->level))
    {
       unHookNode(tree, pageNumber);
       return(RC_OK);
    }
    unHookNode(tree, pageNumber);
    return (sliceMeregKey(tree, pageNumber, 1));
}


void createIndents(char * indent,DataType keyType, int serachedNode){
    memset(indent, 0, PAGE_SIZE);
char *pointer = indent;
int sizeofint = sizeof(int);
    *(int*)pointer = 1;
    pointer= pointer + sizeofint;
    *(int*)pointer = 0;
    pointer= pointer + sizeofint;
    *(int*)pointer = 0;
    pointer= pointer + sizeofint;
    *(int*)pointer = keyType;
    pointer= pointer + sizeofint;
    *(int*)pointer = serachedNode;
    pointer= pointer + sizeofint;
}


