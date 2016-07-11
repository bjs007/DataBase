#include "buffer_mgr.h"
#include <string.h>
#include "storage_mgr.h"
#include "bm_link_list_impl.h"
#include "assert.h"
#define ADDRESS_LEVEL 8

static BM_Pool_Page_Details* getUseableFrame(BM_BufferPool *bm);
static RC updatePage(BM_BufferPool *const bm, BM_Pool_Page_Details *pageFrame);
static void removeFrame(BM_Pool_VPTable *pt, PageNumber pageNumber, int level);
static BM_Pool_Page_Details* getFrameForPage(BM_Pool_VPTable *pt, PageNumber pageNumber, int step);
static void initializeVirtualTable(BM_Pool_VPTable *pt);

/**typedef struct PoolManager {
    BM_Pool_Replace_Data strategyInfo;
    SM_FileHandle fhandler;
    BM_Pool_Page_Details *bm_pool_arr;
    BM_Pool_VPTable pageFrame_pointer;
    int readCount;
    int writeCount;
} PoolManager;**/

void initializeVirtualTable(BM_Pool_VPTable *pt){
  memset((void*)pt, 0, sizeof(BM_Pool_VPTable));
}
// Buffer Manager Interface Pool Handling
// ***************************************
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *strategyInfo){
   PoolManager *dataHandler;
    dataHandler= ((PoolManager*) malloc (sizeof(PoolManager)));   // dynamically allocate the memory for initialization

    initializeVirtualTable(&dataHandler->pageFrame_pointer);
     bm->pageFile= strdup(pageFileName);
     RC rc=RC_OK;
    rc =openPageFile(bm->pageFile, &dataHandler->fhandler);
    if(rc == RC_OK){

    dataHandler->readCount= 0;// this count will be incremented every time the data is read form the page
    dataHandler->writeCount= 0;// this count will be incremented every time the data is changed and updated to the page

  bm->numPages= numPages;// The number of pages the buffer is supposed to handle


  /**Initialize the linked list**/
  dataHandler->strategyInfo.head= NULL;
  dataHandler->strategyInfo.tail= NULL;



  /**Initialize the Stratergy Info :: First In First Out / CLOCK **/
   bm->strategy= strategy;
  dataHandler->strategyInfo.trailingFrame= -1;
      dataHandler->strategyInfo.currFrame=-1;

    initializeBufferPoolData(dataHandler,numPages);
    bm->dataHandler= dataHandler;
    }
  return rc;
}


void initializeBufferPoolData(PoolManager *dataHandler, const int numPages){
    int pageNo;
    dataHandler->bm_pool_arr = ((BM_Pool_Page_Details*) malloc (sizeof(BM_Pool_Page_Details) * numPages));
    for (pageNo=0; pageNo<numPages; pageNo++){
    insertFrameAtEnd(&dataHandler->strategyInfo, &dataHandler->bm_pool_arr[pageNo]);
    dataHandler->bm_pool_arr[pageNo].isReplaceable= TRUE;
    dataHandler->bm_pool_arr[pageNo].pfCount= 0;
    dataHandler->bm_pool_arr[pageNo].pn= -1;
    dataHandler->bm_pool_arr[pageNo].isDirty= FALSE;

  }
}
RC shutdownBufferPool(BM_BufferPool *const bm){
  int frameNumber;
  BM_Pool_Page_Details *pageFrame;
  PoolManager *dataHandler= bm->dataHandler;
  RC rc= forceFlushPool(bm);
  pageFrame= &dataHandler->bm_pool_arr[0];
  for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
        if (pageFrame->pn != -1)
        removeFrame(&dataHandler->pageFrame_pointer, pageFrame->pn,1);
        if (pageFrame->pfCount){
                //pthread_mutex_unlock(&dataHandler->bm_mutex);
                return(RC_POOL_ALREADY_HAVE_PINNED_PAGE);
    }
    pageFrame++;
  }
  rc= closePageFile(&dataHandler->fhandler);
  if (rc != RC_OK){
    return(rc);
  }

  removeLinkEntry(&dataHandler->strategyInfo);


    free(bm->pageFile);
    free(dataHandler->bm_pool_arr);
    free(dataHandler);

  return(RC_OK);
}
RC forceFlushPool(BM_BufferPool *const bm){

  PoolManager *dataHandler;
  int frameNumber;
  BM_Pool_Page_Details *pageFrame;
  dataHandler= bm->dataHandler;
  RC rc= 0;

  pageFrame= &dataHandler->bm_pool_arr[0];
  for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
    rc= updatePage(bm, pageFrame);
    if (rc!=RC_OK)
      break;
    pageFrame++;
  }

  return(rc);
}

// Buffer Manager Interface Access Pages
// ***************************************
static RC updatePage(BM_BufferPool *const bm, BM_Pool_Page_Details *pageFrame)
{
  RC rc;
  PoolManager *dataHandler= bm->dataHandler;

  if (pageFrame->isDirty && pageFrame->pfCount==0)
  {
    rc= writeBlock(pageFrame->pn, &dataHandler->fhandler, (SM_PageHandle) &pageFrame->data);
    if (rc!=RC_OK){

      return(rc);
    }
    dataHandler->writeCount++;
    pageFrame->isDirty= FALSE;
    removeFrame(&dataHandler->pageFrame_pointer, pageFrame->pn,1);
  }

  return(RC_OK);
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
  BM_Pool_Page_Details *pageFrame;
  PoolManager *dataHandler= bm->dataHandler;

  pageFrame= getFrameForPage(&dataHandler->pageFrame_pointer, page->pageNum,1);
  if (pageFrame){
        pageFrame->isDirty= TRUE;

        return(RC_OK);
  }

  return(RC_PAGE_PINNED_ERROR);
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
  BM_Pool_Page_Details *pageFrame;
  PoolManager *dataHandler= bm->dataHandler;
 // //pthread_mutex_lock(&dataHandler->bm_mutex);
  pageFrame= getFrameForPage(&dataHandler->pageFrame_pointer, page->pageNum,1);
  if (!pageFrame){
        //pthread_mutex_unlock(&dataHandler->bm_mutex);
    return(RC_PAGE_PINNED_ERROR);
  }

  pageFrame->pfCount--;
  if(pageFrame->pfCount == 0 && bm->strategy == RS_LRU)
	insertFrameAtEnd(&dataHandler->strategyInfo, pageFrame);

  //pthread_mutex_lock(&dataHandler->bm_mutex);
return(RC_OK);
}


RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  RC rc= RC_PAGE_PINNED_ERROR ;
  BM_Pool_Page_Details *pageFrame;
  PoolManager *dataHandler= bm->dataHandler;
  //pthread_mutex_lock(&dataHandler->bm_mutex);
  pageFrame= getFrameForPage(&dataHandler->pageFrame_pointer, page->pageNum,1);
  if (pageFrame)
    rc= updatePage(bm, pageFrame);

//pthread_mutex_unlock(&dataHandler->bm_mutex);
  return(rc);
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum){
 /** RC rc;
  BM_Pool_Page_Details *pageFrame;
  PoolManager *dataHandler= bm->dataHandler;
  //pthread_mutex_lock(&dataHandler->bm_mutex);
  pageFrame= getFrameForPage(&dataHandler->pageFrame_pointer, pageNum,1);
  if (pageFrame){
    if(bm->strategy == RS_LRU ){
            if(pageFrame->pfCount==0 )
                getFromPool(&dataHandler->strategyInfo, pageFrame);
    }
    if (bm->strategy == RS_CLOCK){
       pageFrame->isReplaceable = FALSE;
    }
    pageFrame->pfCount++;
    page->pageNum= pageNum;
    page->data= (char*)&pageFrame->data;
    //pthread_mutex_unlock(&dataHandler->bm_mutex);
    return(RC_OK);
  }
  pageFrame= getUseableFrame(bm);
  if (pageFrame==NULL){
        //pthread_mutex_unlock(&dataHandler->bm_mutex);
    return(RC_POOL_FULL);
  }
  if (pageNum >= dataHandler->fhandler.totalNumPages){
    rc= ensureCapacity(pageNum+1, &dataHandler->fhandler);
    if (rc!=RC_OK){
        //pthread_mutex_unlock(&dataHandler->bm_mutex);
      return rc;
    }
  }
  rc= readBlock(pageNum, &dataHandler->fhandler, &pageFrame->data[0]);
  if (rc!=RC_OK){
      //pthread_mutex_unlock(&dataHandler->bm_mutex);
    return rc;
  }
    page->data= &pageFrame->data[0];
  dataHandler->readCount =dataHandler->readCount+1;
  pageFrame->pfCount= pageFrame->pfCount+1;
  pageFrame->pn= page->pageNum= pageNum;
  setFrame(&dataHandler->pageFrame_pointer, pageNum, pageFrame,1);

    if (bm->strategy == RS_CLOCK){
     pageFrame->isReplaceable = FALSE;
    }

    //pthread_mutex_unlock(&dataHandler->bm_mutex);
  return(RC_OK);**/




  RC rc;
  BM_Pool_Page_Details *pf;
  PoolManager *mgmtData= bm->dataHandler;
 // BM_LOCK();

  // Check if we already have a frame assigned to this page
  pf= getFrameForPage(&mgmtData->pageFrame_pointer, pageNum,1);
  if (pf)
  {
    // If fixCount==0, then remove it from LRU
    // Representing that frame is no more free
    if(pf->pfCount==0 && bm->strategy == RS_LRU)
      getFromPool(&mgmtData->strategyInfo, pf);

    pf->pfCount++;
    page->pageNum= pageNum;
    page->data= (char*)&pf->data;
    if (bm->strategy == RS_CLOCK)
    {
       pf->isReplaceable = FALSE;
    }
    return(RC_OK);
  }

  // Get free frame from pool
  pf= getUseableFrame(bm);
  if (pf==NULL)
  {
    return (RC_POOL_FULL);
  }

  if (pageNum >= mgmtData->fhandler.totalNumPages)
  {
    rc= ensureCapacity(pageNum+1, &mgmtData->fhandler);
    if (rc!=RC_OK)
    {
      return rc;
    }
  }
  rc= readBlock(pageNum, &mgmtData->fhandler, &pf->data[0]);
  if (rc!=RC_OK)
  {

    return rc;
  }
  mgmtData->readCount++;

  // Mark page frame as used
  pf->pfCount++;
  pf->pn= page->pageNum= pageNum;
  page->data= &pf->data[0];

  // Map page number to frame;
  setFrame(&mgmtData->pageFrame_pointer ,pageNum, pf,1);

   //Set the flag for the flag as false, which will prevent any replacement of this frame
   if (bm->strategy== RS_CLOCK)
     pf->isReplaceable = FALSE;

 // BM_UNLOCK();
  return (RC_OK);

}
PageNumber *getFrameContents (BM_BufferPool *const bm){
  int frameCount;
  PoolManager *dataHandler= bm->dataHandler;
  BM_Pool_Page_Details *pageFrame= &dataHandler->bm_pool_arr[0];// initialize to the first bm_pool_arr and increment
  int numOfPages =bm->numPages;
  //pthread_mutex_lock(&dataHandler->bm_mutex);
  PageNumber *pageNumber=(PageNumber*) malloc(sizeof(PageNumber)*numOfPages);
  for (frameCount=0; frameCount < numOfPages; frameCount++){
    pageNumber[frameCount]= pageFrame->pn;
    pageFrame++;
  }
  //pthread_mutex_unlock(&dataHandler->bm_mutex);

  return pageNumber;
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    int frameNumber;
    bool *dirty_flag_array= (bool*) malloc(bm->numPages*sizeof(bool));
    PoolManager *dataHandler= bm->dataHandler;
    BM_Pool_Page_Details *pageFrame= &dataHandler->bm_pool_arr[0];
            for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
                if (pageFrame->isDirty)
                    dirty_flag_array[frameNumber]= TRUE;
                else
                    dirty_flag_array[frameNumber]= FALSE;

            ++pageFrame;
  }
  return dirty_flag_array;
}
int *getFixCounts (BM_BufferPool *const bm)
{
  int *page_frame_array= (int*) malloc(bm->numPages*sizeof(int));
  PoolManager *dataHandler= bm->dataHandler;
  int frameNumber;
  BM_Pool_Page_Details *pageFrame= &dataHandler->bm_pool_arr[0];
  for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
    page_frame_array[frameNumber]= pageFrame->pfCount;
    pageFrame++;
  }
  return page_frame_array;
}
int getNumReadIO (BM_BufferPool *const bm){
    int readCount=0;
  PoolManager *dataHandler= bm->dataHandler;
  readCount = dataHandler->readCount;
  return readCount;
}
int getNumWriteIO (BM_BufferPool *const bm)
{
   int writeCount=0;
  PoolManager *dataHandler= bm->dataHandler;
  writeCount = dataHandler->writeCount;
  return writeCount;
}
static BM_Pool_Page_Details* getUseableFrame(BM_BufferPool *bm){
        RC rc;
        BM_Pool_Page_Details *pageFrame;
        PoolManager *dataHandler= dataHandler= bm->dataHandler;
//pthread_mutex_lock(&dataHandler->bm_mutex);
            if(bm->strategy == RS_FIFO){
                int frameNumber,newFrame= dataHandler->strategyInfo.trailingFrame+1;
                    for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
                            newFrame= newFrame % bm->numPages;// This is to keep the frame number within the buffer page limit
                                pageFrame= &dataHandler->bm_pool_arr[newFrame];
                                if (pageFrame->pfCount==0){
                                    if (pageFrame->isDirty){
                                        rc= updatePage(bm, pageFrame);
                                            if (rc!=RC_OK){
                                                    //pthread_mutex_unlock(&dataHandler->bm_mutex);
                                                return NULL;
                                            }
                                            }
                                            else if (pageFrame->pn != -1){
                                                removeFrame(&dataHandler->pageFrame_pointer, pageFrame->pn,1);
                                            }
                                        dataHandler->strategyInfo.trailingFrame= newFrame;
                                        //pthread_mutex_unlock(&dataHandler->bm_mutex);
                                        return pageFrame;
                                }
                                    newFrame++;
                    }
                        //pthread_mutex_unlock(&dataHandler->bm_mutex);
                        return NULL;
        }else if(bm->strategy == RS_LRU){
           pageFrame= getFrame(&dataHandler->strategyInfo);
                if (!pageFrame)
                    return NULL; // All frames pinned
                        if (pageFrame->isDirty){
                            rc= updatePage(bm, pageFrame);
                                if (rc!=RC_OK){
                                    //pthread_mutex_unlock(&dataHandler->bm_mutex);
                                    return NULL;
                                }
                    }else if (pageFrame->pn != -1){
                            removeFrame(&dataHandler->pageFrame_pointer, pageFrame->pn,1);
                    }
                    //pthread_mutex_unlock(&dataHandler->bm_mutex);
                return pageFrame;
        }else if(bm->strategy == RS_CLOCK){
            int frameNumber,newFrame= dataHandler->strategyInfo.currFrame+1;
                    int clockPage=(bm->numPages+bm->numPages);
                    for (frameNumber=0; frameNumber < bm->numPages; frameNumber++){
                            newFrame= newFrame % bm->numPages;// This is to keep the frame number within the buffer page limit
                                pageFrame= &dataHandler->bm_pool_arr[newFrame];
                                if(pageFrame->isReplaceable == TRUE){
                                if (pageFrame->pfCount==0){
                                    if (pageFrame->isDirty){
                                        rc= updatePage(bm, pageFrame);
                                            if (rc!=RC_OK){
                                                    //pthread_mutex_unlock(&dataHandler->bm_mutex);
                                                return NULL;
                                            }
                                            }
                                            else if (pageFrame->pn != -1){
                                                removeFrame(&dataHandler->pageFrame_pointer, pageFrame->pn,1);
                                            }
                                        dataHandler->strategyInfo.trailingFrame= newFrame;
                                        //pthread_mutex_unlock(&dataHandler->bm_mutex);
                                        return pageFrame;
                                }
                                }else{
                                    pageFrame->isReplaceable = TRUE;
                                }
                                    newFrame++;
                    }
                        //pthread_mutex_unlock(&dataHandler->bm_mutex);
                        return NULL;
        }
}


void removeFrame(BM_Pool_VPTable *pt, PageNumber pageNumber, int step){
    // The concept used here is same as the concept use for mapping pages in Virtual Memory in OS
    // for binding the pageHandlerysical memory to virtual memory
    //Reference::http://www.cs.cmu.edu/~gkesden/412-18/fall01/ln/lecture11.html
    //Referenence::http://www.cs.columbia.edu/~junfeng/10sp-w4118/lectures/l21-page.pdf
    //Reference ::http://www.bottomupcs.com/virtual_addresses.html
    short offset= (pageNumber>>(ADDRESS_LEVEL)*(4-step)) & 0x000000FF;
    BM_Pool_VPTable* vPointer= pt->entry[offset];

    if (vPointer==NULL)
    return;
    if (step == 4){
            if (vPointer!=NULL){
                pt->entry[offset]= NULL;
                pt->deleteFlag--;
                return;
            }
        }
  removeFrame(vPointer, pageNumber, step+1);
//Once the farme is found decouple it and reduce the reference count
  if (vPointer->deleteFlag == 0){
      free (vPointer);
      pt->entry[offset]= NULL;
      pt->deleteFlag--;
  }
  return;
}


BM_Pool_Page_Details* getFrameForPage(BM_Pool_VPTable *pt, PageNumber pageNumber, int step){
  PageNumber offset= (pageNumber>>((4-step)*ADDRESS_LEVEL)) & 0x000000FF;
  BM_Pool_VPTable* vPointer= pt->entry[offset];
  if (vPointer==NULL)
    return NULL;
  if (step==4)
    return (BM_Pool_Page_Details*) vPointer;

  return getFrameForPage(vPointer, pageNumber, step+1);
}

void setFrame(BM_Pool_VPTable *pt, PageNumber pageNumber,
                  BM_Pool_Page_Details *frame, int step){
  PageNumber offset= (pageNumber>>((4-step)*ADDRESS_LEVEL)) & 0x000000FF ;
  BM_Pool_VPTable* vPointer= pt->entry[offset];

  if (step==4){
    if (vPointer==NULL){
      pt->entry[offset]= (void*) frame;
      pt->deleteFlag++;
    }
    return;
  }
   if (vPointer==NULL){
    vPointer= (BM_Pool_VPTable *) malloc (sizeof(BM_Pool_VPTable));
    initializeVirtualTable(vPointer);
    pt->entry[offset]= vPointer;
    pt->deleteFlag= pt->deleteFlag+1;
  }

  setFrame(vPointer, pageNumber, frame, step+1);

}






