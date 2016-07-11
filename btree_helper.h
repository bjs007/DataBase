#ifndef BTREE_HELPER_H_INCLUDED
#define BTREE_HELPER_H_INCLUDED
#include "btree_mgr.h"


 Node* searchElm (BTreeHandle *bTree, Node *node, Value *key,
                             int *elemPos, int *pageNum,
                             bool match);


 RC sliceMeregKey(BTreeHandle *bTree, PageNumber pn, bool leaf);

 int addKey(BTreeHandle *bTree, Node *node, long long index, Value *key);
 RC destroy(BTreeHandle *bTree, PageNumber fromPn, Value *key);


PageNumber retrieveAdjacentNode(BTreeHandle *bTree, PageNumber pn);

 int createNode(BTreeHandle *bTree);
 RC insertInTree(BTreeHandle *bTree, PageNumber pageNo,PageNumber rightPn, Value key);

 RC mergeElements(BTreeHandle *bTree, PageNumber left, PageNumber right);

 RC span(BTreeHandle *bTree, PageNumber left, PageNumber right);

  int deleteKeyInTree(BTreeHandle *bTree, Node *node, PageNumber index, Value *key);
 Node* retrieveNode(BTreeHandle *bTree, PageNumber pn);


 void unHookNode(BTreeHandle *bTree, PageNumber pn);




#endif // BTREE_HELPER_H_INCLUDED
