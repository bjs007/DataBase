*****************************************************Record Manager***************************************************************
----------------------------------------------------------------------------------------------------------------------------------
ID No  :  A20351185
NAME   : Bijay Sharma 
EMAIL  : bsharma@hawk.iit.edu

ID No  :  A20353463
NAME   : Arnab Mukhopadhyay
EMAIL  :  amukhopa@hawk.iit.edu 

__________________________________________________________________________________________________________________________________

Porblem Statement : The objective of the project is to make a project that will implement B+-tree index.The index should be backed up by a page file and pages of the index should be accessed through the buffer manager.Each node should occupy one page.

--------------------------------------------------------------------------------------------------------------------------------------

How to run the project?

Step 1: Run makefile command
Step 2: Run ./test_assign4_1




FILES IN THE DIRECTORY
----------------------
record_mgr.c
record_mgr.h
expr.c
expr.h
tables.h
rm_serializer.c
buffer_mgr.c
buffer_mgr.h
buffer_mgr_stat.c
buffer_mgr_stat.h
storage_mgr.c
Storage_mgr.h
test_assign2_1.c
test_helper.h
bm_link_list_impl.c
bm_link_list_impl.h
btree_helper.c
btree_helper.h
btree_mgr.c
btree_mgr.h
constant.h
constants.h
------------------------
Functions and their details:

1. initIndexManager: Function used to initialize a Index Manager.

2. shutdownIndexManager : This function is used to shutdown the Index manager.

3. createBtree : This function helps to create a b-tree index. 

4. createIndents: This function make a series of call
    1>Create A Page File
    2>Open Page File
    3>Write A Block
    4>Close A Page File

5.openBtree:  This function opens the b-tree index before a client can access it.

6.closeBtree: This function closes the b-tree index. When closing the b-tree, the index manager ensures that all new or modified pages of the index are flushed back to disk.

7.deleteBtree: It deletes the index.

8.getNumNodes,getNumEntries,getKeyType:It access information about a b-tree.

9.deleteKey:This function deletes keys from a given b+ tree.

10.openTreeScan,nextEntry,closeTreeScan:Tree Scanner fuction is used to trasvere throught the tree and internally the progam calls tge nextEntry to find the elements.

11.insertKey : This function basically helps to do all the operation related to insertion of key in the tree. If the tree is full we try to split the niode in to  two half left subtree and right subtree and then will call another function all key to Root NOde to insert the key in the tree at the appropriate postion with upating the root node in the structre accordingly.





