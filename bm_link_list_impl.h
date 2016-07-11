#ifndef BM_LINK_LIST_IMPL_H_INCLUDED
#define BM_LINK_LIST_IMPL_H_INCLUDED
#include "buffer_mgr.h"
BM_Pool_Page_Details* getFrame(BM_Pool_Replace_Data *data);
void insertFrameAtEnd(BM_Pool_Replace_Data *data, BM_Pool_Page_Details *pf);
void getFromPool(BM_Pool_Replace_Data *data, BM_Pool_Page_Details *pf);
void removeLinkEntry(BM_Pool_Replace_Data *data);
#endif // BM_LINK_LIST_IMPL_H_INCLUDED
