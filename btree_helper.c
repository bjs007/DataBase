#include "btree_helper.h"
#include "btree_mgr.h"
#include "assert.h"



Node* searchElm (BTreeHandle *tree, Node *node, Value *key,
                      int *elemPos, int *pageNum, bool match)
{
    int counter= 0;
    Value value1, value2;
    int isGreater;
    Node *n;
    PageNumber pageNumber;
    NodeElement *elementPointer;


    if (!node->numKeys) return (NULL);
    elementPointer= &node->elementPointer;
    while (counter < node->numKeys) 
    {
        valueSmaller(&elementPointer[counter].key, key, &value1);
        valueEquals(&elementPointer[counter].key, key, &value2);
        isGreater= !value1.v.boolV && !value2.v.boolV;

        if (node->leaf) 
        {
            if (value2.v.boolV)
            {
                *elemPos= counter;
                return(node);
            }
            if (isGreater)
            {
                if(match)
                {
                  *elemPos= counter;
                  return(node);
                }
                *elemPos= *pageNum= -1;
                return(NULL);
            }
        }
        else if (isGreater) 
        {
            PageNumber pageNumber= (PageNumber) elementPointer[counter].index;
            n= retrieveNode(tree, pageNumber);
            *pageNum= pageNumber;
            node= searchElm (tree, n, key, elemPos, pageNum, match);
            unHookNode(tree, pageNumber);
            return(node);
        }

        counter++;
    } 


    if (node->leaf)
    {
        if (match)
        {
            *elemPos= counter;
            return(node);
        }
        *elemPos= *pageNum= -1;
        return(NULL);
    }


    pageNumber= node->nodePtr;
    n= retrieveNode(tree, pageNumber);
    *pageNum= pageNumber;
    node= searchElm (tree, n, key, elemPos, pageNum, match);
    unHookNode(tree, pageNumber);

    return(node);
}




 RC sliceMeregKey(BTreeHandle *tree, PageNumber leftPageNum, bool leaf)
{
    char *destination;
    char  *source;
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    int copy, ignore=0;
    Value splitKey;
    PageNumber splitPtr;

    Node *left, *right;
    NodeElement *rightElem, *leftElem;
    PageNumber rightPageNum;

    left= retrieveNode(tree, leftPageNum);
    leftElem= &left->elementPointer;
    rightPageNum= createBTNode(tree);
    right= retrieveNode(tree, rightPageNum);
    right->parent= left->parent;
    right->leaf= leaf;
    rightElem= &right->elementPointer;

    copy=(treeManager->level+1)/2;
    right->nodePtr= left->nodePtr;
    if (!leaf)
    {
        splitKey= leftElem[left->numKeys-copy-1].key;
        splitPtr= leftElem[left->numKeys-copy-1].index;
        ignore= 1;

        left->nodePtr= splitPtr;
    }
    else
      left->nodePtr= rightPageNum;


    destination= (char*) &rightElem[0];
    source= (char*) &leftElem[left->numKeys-copy];
    right->numKeys= copy;
    memcpy(destination, source, right->numKeys*(sizeof(NodeElement)));

    left->numKeys -= copy+ignore;

    unHookNode(tree, leftPageNum);
    unHookNode(tree, rightPageNum);

   
    return(insertInTree(tree, leftPageNum, rightPageNum, splitKey));
}

RC findKey (BTreeHandle *tree, Value *key, RID *value1ult)
{

    RC rc;
    Node *n;
    Node *node;
    NodeElement *elementPointer;
    PageNumber pageNumber, pnRes;

     int elemPos= -1;

    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    pageNumber= treeManager->top;
    n= retrieveNode(tree, pageNumber);
    pnRes= pageNumber;
    node= searchElm (tree, n, key, &elemPos, &pnRes, 0);
    unHookNode(tree, pageNumber);

    if (!node)
      return(RC_IM_KEY_NOT_FOUND);
    n= retrieveNode(tree, pnRes);
    elementPointer= &node->elementPointer;
    *value1ult= *((RID*) &elementPointer[elemPos].index);
    unHookNode(tree, pnRes);

    return(RC_OK);
}


 RC insertInTree(BTreeHandle *tree, PageNumber pageNo,
                            PageNumber rightPn, Value key)
{
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    Node *parent, *left, *right;
    NodeElement *elementPointer;
    PageNumber pageNumber;
    int counter=0, parentKeys;

    left= retrieveNode(tree, pageNo);
    right= retrieveNode(tree, rightPn);

   
    if (left->parent == -1)
    {
        pageNumber= createBTNode(tree);
        treeManager->top= pageNumber;

       
        parent= retrieveNode(tree, pageNumber);
        parent->parent= -1;
        parent->nodePtr= -1;
        parent->leaf= 0;
        parent->numKeys= 0;
    }
    else{
        pageNumber= left->parent;
        parent= retrieveNode(tree, pageNumber);
    }
    if (left->leaf){
        elementPointer= &right->elementPointer;
        key= elementPointer[0].key;
    }
    counter= addKey(tree, parent, pageNo, &key);
    parentKeys=parent->numKeys;
    if (counter == parentKeys-1)
      parent->nodePtr= rightPn;
    else{
        elementPointer= &parent->elementPointer;
        elementPointer[counter+1].index= rightPn;
    }
    left->parent= right->parent= pageNumber;
    unHookNode(tree, pageNumber);
    unHookNode(tree, pageNo);
    unHookNode(tree, rightPn);

    if (parentKeys <= treeManager->level)
    {
       unHookNode(tree, pageNumber);
       return(RC_OK);
    }
    unHookNode(tree, pageNumber);
    return (sliceMeregKey(tree, pageNumber, 0));
}

 int addKey(BTreeHandle *tree, Node *node, long long index, Value *key)
{
    int counter=0;
    NodeElement *elementPointer;
    TreeManager *treeManager;
    treeManager= (TreeManager*) tree->mgmtData;
    Value value1;

    elementPointer= &node->elementPointer;
    while(counter < node->numKeys)
    {
        valueSmaller(&elementPointer[counter].key, key, &value1);
        if (!value1.v.boolV)
            break;
        counter++;
    }

    if (counter < node->numKeys)
      memmove((char*) &elementPointer[counter+1], (char*) &elementPointer[counter],
              (int) (node->numKeys-counter+1)*sizeof(NodeElement));

    elementPointer[counter].index= index;
    elementPointer[counter].key= *key;
    if (node->leaf)
       treeManager->numOfElem++;
    node->numKeys++;

    return(counter);
}
int deleteKeyInTree(BTreeHandle *tree, Node *node, PageNumber index, Value *key)
{
    int counter=0;
    NodeElement *elementPointer;
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    Value value1;

    elementPointer= &node->elementPointer;
    while(counter < node->numKeys)
    {
        if (node->leaf)
        {
            valueEquals(&elementPointer[counter].key, key, &value1);
            if (value1.v.boolV)
            {
                if (counter < node->numKeys-1)
                    memmove((char*) &elementPointer[counter], (char*) &elementPointer[counter+1],
                            (int) (node->numKeys-counter+1)*sizeof(NodeElement));
                node->numKeys--;
                treeManager->numOfElem--;

                return(counter);
            }
        }
        else
        {
            valueEquals(&elementPointer[counter].key, key, &value1);
            if (value1.v.boolV || elementPointer[counter].index==index)
            {
                PageNumber tempNodePn;
                if (counter==0)
                    tempNodePn= elementPointer[0].index;

                memmove((char*) &elementPointer[counter], (char*) &elementPointer[counter+1],
                        (int) (node->numKeys-counter+1)*sizeof(NodeElement));
                if (counter==0)
                    elementPointer[0].index= tempNodePn;

                node->numKeys--;
                treeManager->numOfElem--;

                return(counter);
            }
            if (counter+1 == node->numKeys && node->nodePtr == index)
            {
                node->numKeys--;
                treeManager->numOfElem--;
                return(counter+1);
            }
        }
        counter++;
    }
    return (0);
}
 int createBTNode(BTreeHandle *tree){
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    PoolManager *poolManager= treeManager->bufferPool.dataHandler;

    if (appendEmptyBlock(&poolManager->fhandler) != RC_OK)
        return(RC_INSERT_ERROR);
    treeManager->nCount++;
    return(poolManager->fhandler.totalNumPages-1);
}


RC mergeElements(BTreeHandle *tree, PageNumber leftPageNum, PageNumber rightPageNum)
{
    Node *leftElem1, *rightElem1, *tempNodeNode;
    NodeElement *leftElem, *rightElem;
    bool mergeRight= (leftPageNum < 0);
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    int counter=0, mergePtr;
    Value valueMergeKey;

    leftPageNum= mergeRight ? (leftPageNum*-1) : leftPageNum;
    leftElem1= retrieveNode(tree, leftPageNum);
    rightElem1= retrieveNode(tree, rightPageNum);

    leftElem= &leftElem1->elementPointer;
    rightElem= &rightElem1->elementPointer;

    if (leftElem1->leaf)
    {
        valueMergeKey= rightElem[0].key;
        mergePtr= rightElem1->parent;
        memcpy((char*) &leftElem[leftElem1->numKeys], (char*) &rightElem[0],
               (int) rightElem1->numKeys*sizeof(NodeElement));
        if (! mergeRight) {
                    leftElem1->nodePtr= rightElem1->nodePtr;
        }
        else {
              char *tempNode= (char*) malloc(leftElem1->numKeys*sizeof(NodeElement));
                memcpy(tempNode, (char*) &leftElem[0],
                    leftElem1->numKeys*sizeof(NodeElement));
                memcpy((char*) &leftElem[0], (char*) &rightElem[0],
                    rightElem1->numKeys*sizeof(NodeElement));
                memcpy((char*) &leftElem[rightElem1->numKeys], (char*) tempNode,
                   leftElem1->numKeys*sizeof(NodeElement));
        }


        leftElem1->numKeys+= rightElem1->numKeys;

        Value value2;
        tempNodeNode= retrieveNode(tree, rightElem1->parent);

        if (tempNodeNode->nodePtr == rightPageNum)
            tempNodeNode->nodePtr = leftPageNum;
        unHookNode(tree, rightElem1->parent);


        rightElem1->numKeys= 0;
        treeManager->nCount--;

        unHookNode(tree, leftPageNum);
        unHookNode(tree, rightPageNum);

        return(destroy(tree, mergePtr, &valueMergeKey));
    }

    else if ( (leftElem1->numKeys+rightElem1->numKeys) < treeManager->level )
    {
        NodeElement *elementPointer;
        Node *tempNode;
        Node *parentNode;
        int parentIdx;
        parentNode= retrieveNode(tree, rightElem1->parent);
        elementPointer= &parentNode->elementPointer;
        for(counter=0; counter<parentNode->numKeys; counter++)
            if (elementPointer[counter].index == rightPageNum)
            {
              parentIdx= counter;
              break;
            }


        leftElem[leftElem1->numKeys].key= elementPointer[parentIdx].key;
        leftElem[leftElem1->numKeys].index= leftElem1->nodePtr;
        leftElem1->numKeys++;


        memcpy((char*) &leftElem[leftElem1->numKeys], (char*) &rightElem[0],
               (int) rightElem1->numKeys*sizeof(NodeElement));
        if (mergeRight)
        {
            char *tempNode= (char*) malloc(leftElem1->numKeys*sizeof(NodeElement));
            memcpy(tempNode, (char*) &leftElem[0],
                   leftElem1->numKeys*sizeof(NodeElement));
            memcpy((char*) &leftElem[0], (char*) &rightElem[0],
                   rightElem1->numKeys*sizeof(NodeElement));
            memcpy((char*) &leftElem[rightElem1->numKeys], (char*) tempNode,
                   leftElem1->numKeys*sizeof(NodeElement));
        }
        else
          leftElem1->nodePtr= rightElem1->nodePtr;

        valueMergeKey= rightElem[0].key;
        mergePtr= rightElem1->parent;
        leftElem1->numKeys+= rightElem1->numKeys;


        rightElem1->numKeys= 0;
        treeManager->nCount--;

        for(counter=0; counter<leftElem1->numKeys; counter++)
        {
           tempNode= retrieveNode(tree, leftElem[counter].index);
           tempNode->parent= leftPageNum;
           unHookNode(tree, leftElem[counter].index);
        }

        unHookNode(tree, rightElem1->parent);
        unHookNode(tree, leftPageNum);
        unHookNode(tree, rightPageNum);

        return(destroy(tree, mergePtr, &valueMergeKey));
    }
    return(RC_OK);
}
RC span(BTreeHandle *tree, PageNumber leftPageNum, PageNumber rightPageNum)
{
    Node *leftElem1, *rightElem1;
    NodeElement *leftElem, *rightElem;
    bool mergeRight= (leftPageNum < 0);
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    int mergePtr;
    Value valueMergeKey;

    leftPageNum= mergeRight ? (leftPageNum*-1) : leftPageNum;
    leftElem1= retrieveNode(tree, leftPageNum);
    rightElem1= retrieveNode(tree, rightPageNum);
    leftElem= &leftElem1->elementPointer;
    rightElem= &rightElem1->elementPointer;

    if (leftElem1->leaf)
    {
        valueMergeKey= rightElem[0].key;
        mergePtr= rightElem1->parent;

        memcpy((char*) &leftElem[leftElem1->numKeys], (char*) &rightElem[0],
               (int) sizeof(NodeElement));
        if (mergeRight)
        {
            char *tempNode= (char*) malloc(leftElem1->numKeys*sizeof(NodeElement));
            memcpy(tempNode, (char*) &leftElem[0], leftElem1->numKeys*sizeof(NodeElement));
            memcpy((char*) &leftElem[0], (char*) &rightElem[0], sizeof(NodeElement));
            memcpy((char*) &leftElem[1], (char*) tempNode, leftElem1->numKeys*sizeof(NodeElement));
        }
        else
          leftElem1->nodePtr= rightElem1->nodePtr;

        leftElem1->numKeys+= 1;
        rightElem1->numKeys-= 1;

        unHookNode(tree, leftPageNum);
        unHookNode(tree, rightPageNum);

        return(destroy(tree, mergePtr, &valueMergeKey));
    }
    return(RC_OK);
}

Node* retrieveNode(BTreeHandle *tree, PageNumber pageNumber)
{
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    BM_PageHandle pageHandler;

    pinPage(&treeManager->bufferPool, &pageHandler, (PageNumber)pageNumber);
    return( (Node*) pageHandler.data);
}

void unHookNode(BTreeHandle *tree, PageNumber pageNumber)
{
    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    BM_PageHandle pageHandler;
    pageHandler.pageNum= pageNumber;
    unpinPage(&treeManager->bufferPool, &pageHandler);
}





PageNumber retrieveAdjacentNode(BTreeHandle *tree, PageNumber pageNumber){
    Node *parent;

    PageNumber pageNumNegih;
    int counter, parentPage;
    Node *tempNode;
    NodeElement *elementPointer;


    tempNode= retrieveNode(tree, pageNumber);
    parentPage= tempNode->parent;
    unHookNode(tree, pageNumber);
    if (parentPage < 0)
        return 0;

    parent= retrieveNode(tree, parentPage);

    elementPointer= &parent->elementPointer;
    for (counter=0; counter<parent->numKeys; counter++)
        if (pageNumber == elementPointer[counter].index)
            break;
    if (pageNumber == parent->nodePtr && counter == parent->numKeys )
        pageNumNegih= elementPointer[parent->numKeys-1].index;

    else if (counter == 0 )
        pageNumNegih= -elementPointer[counter+1].index;
       else if (counter == parent->numKeys-1)
        pageNumNegih= -parent->nodePtr;
    else
        pageNumNegih = elementPointer[counter-1].index;

    unHookNode(tree, parentPage);

    return pageNumNegih;
}


RC destroy(BTreeHandle *tree, PageNumber fromPageNum, Value *key)
{
    int neighborKeys, remainingKeys, counter;
    Node *node;
    Node  *neighborNode;
    NodeElement *elementPointer;

    PageNumber pageNumNegih;
    PageNumber  parentPn;

    Node  *parentNode;
    Node  *tempNodeNode;

    TreeManager *treeManager= (TreeManager*) tree->mgmtData;
    RC rc= RC_OK;

    node= retrieveNode(tree, fromPageNum);
    parentPn= node->parent;

    counter= deleteKeyInTree(tree, node, fromPageNum, key);
    remainingKeys= node->numKeys;

    unHookNode(tree, fromPageNum);


    if (parentPn>0){
        parentNode= retrieveNode(tree, parentPn);
        if (node->numKeys && counter==0){
            Value value2;
            elementPointer= &parentNode->elementPointer;
            for(counter=0; counter<parentNode->numKeys; counter++){
                valueEquals(&elementPointer[counter].key, key, &value2);
                if ( value2.v.boolV){
                    elementPointer[counter].key = node->elementPointer.key;
                    break;
                }
            }
        }
        else if(node->numKeys==0 && parentNode->numKeys==1) 
        {
            if (fromPageNum == parentNode->elementPointer.index)
                treeManager->top= parentNode->nodePtr;
            else
                treeManager->top= parentNode->elementPointer.index;
            node->parent= -1;
            treeManager->nCount--;
            parentNode->numKeys--;
            if (node->leaf)
                node->nodePtr= -1;
        }
        unHookNode(tree, parentPn);
    }

    if (treeManager->top == fromPageNum || node->parent < 0)
    {

        if (treeManager->numOfElem == 0){
            treeManager->top= 1;
            treeManager->nCount= 0;
            treeManager->numOfElem= 0;

        }

        if (node->parent < 0 && node->numKeys == 1 )
        {
            tempNodeNode= retrieveNode(tree, node->elementPointer.index);
            if (tempNodeNode->numKeys==0)
            {
                treeManager->top= node->nodePtr;
                tempNodeNode->parent= -1;
                treeManager->nCount--;
                if (tempNodeNode->leaf)
                    tempNodeNode->nodePtr= -1;
            }
            unHookNode(tree, node->elementPointer.index);
            tempNodeNode= retrieveNode(tree, node->nodePtr);
            if (tempNodeNode->numKeys==0)
            {
                treeManager->top= node->elementPointer.index;
                tempNodeNode->parent= -1;
                treeManager->nCount--;
                if (tempNodeNode->leaf)
                    tempNodeNode->nodePtr= -1;
            }
            unHookNode(tree, node->nodePtr);
        }
        return(RC_OK);
    }
    if (remainingKeys  <(treeManager->level+1)/2){


    pageNumNegih= retrieveAdjacentNode(tree, fromPageNum);

    neighborNode= retrieveNode(tree, (PageNumber) pageNumNegih<0 ? pageNumNegih*-1:pageNumNegih);
    neighborKeys= neighborNode->numKeys;
    unHookNode(tree, (PageNumber) pageNumNegih);
    if ((neighborKeys+remainingKeys) <= treeManager->level)
       rc= mergeElements(tree, pageNumNegih, fromPageNum);
    else if (remainingKeys < (treeManager->level+1)/2)
       rc= span(tree, pageNumNegih, fromPageNum);

    return(rc);

    }else{
         return(RC_OK);
    }


}
