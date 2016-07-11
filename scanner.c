#include "scanner.h"

/**
Implementation of scanner.h
**/
void update(int pageno,Schema *sch,TableManager *tableManager, Pager *dp){
    int i;
    int slotId =-1;
    int _rSize= getRecordSize(sch)+1;
    char *slotAddr= ((char*) &dp->data);
    int totalSlots= ( ((int)(PAGE_SIZE - ((&((Pager*)0)->data) - ((char*)0)) )/(getRecordSize(sch)+1) ));
    for (i=0; i<totalSlots; i++){
        if (!(*(char*)slotAddr)>0){
            slotId = i;
            break;
        }
        slotAddr= _rSize+slotAddr;
    }
    if (slotId <= 0){
          pop(tableManager, dp, pageno);}
    else{
      push(tableManager, dp, pageno);
    }

}

void removeFromHead(TableManager *tableManager, Pager *dp, int pageno){
    BM_PageHandle pageHandler;
     Pager *pagerData;
    if (dp->next == 0){
            dp->prev= 0;
            tableManager->pageNo= 0;
        }
        else{
            pinPage(&tableManager->pool, &pageHandler, (PageNumber)dp->next);
            pagerData= (Pager*) pageHandler.data;
            markDirty(&tableManager->pool, &pageHandler);
            pagerData->prev= 0;
            unpinPage(&tableManager->pool, &pageHandler);
            tableManager->pageNo= dp->next;
            dp->next=dp->prev= 0;
        }
}

void removeFromTail(TableManager *tableManager, Pager *dp, int pageno){
      BM_PageHandle pageHandler;
        Pager *pagerData;

        pinPage(&tableManager->pool, &pageHandler, (PageNumber)dp->prev);
        pagerData= (Pager*) pageHandler.data;
        markDirty(&tableManager->pool, &pageHandler);
        pagerData->next= 0;
        unpinPage(&tableManager->pool, &pageHandler);
        dp->next=dp->prev= 0;

}


void removeFromMiddle(TableManager *tableManager, Pager *dp, int pageno){
        BM_PageHandle pageHandler2;
         BM_PageHandle pageHandler;
        Pager *pagerData;
        Pager *tmp_dp2;
        pinPage(&tableManager->pool, &pageHandler, (PageNumber)dp->prev);
        pinPage(&tableManager->pool, &pageHandler2, (PageNumber)dp->next);
        pagerData= (Pager*) pageHandler.data;
        tmp_dp2= (Pager*) pageHandler2.data;
        markDirty(&tableManager->pool, &pageHandler);
        markDirty(&tableManager->pool, &pageHandler2);
        pagerData->next= dp->next;
        tmp_dp2->prev= dp->prev;
        unpinPage(&tableManager->pool, &pageHandler);
        unpinPage(&tableManager->pool, &pageHandler2);
        dp->next=dp->prev= 0;

}

void pop(TableManager *tableManager, Pager *dp, int pageno)
{

    if (dp->prev==0 && dp->next==0)
        return;
    if (pageno == tableManager->pageNo){
      removeFromHead(tableManager, dp, pageno);
    }
    else if (dp->next == 0){
        removeFromTail(tableManager, dp, pageno);
    }
    else if (dp->next != 0 && dp->prev != 0){
        removeFromMiddle(tableManager, dp, pageno);
    }
}

void push(TableManager *tableManager, Pager *dp, int pageno){
    if (dp->next!=0)
        return;

    if (tableManager->pageNo == 0){
        dp->next=dp->prev= pageno;
        tableManager->pageNo= pageno;
    }
    else{
        BM_PageHandle pageHandler;
        Pager *head_page;
        pinPage(&tableManager->pool, &pageHandler, (PageNumber)tableManager->pageNo);
        head_page= (Pager*) pageHandler.data;
        markDirty(&tableManager->pool, &pageHandler);
        head_page->prev= pageno;
        unpinPage(&tableManager->pool, &pageHandler);
        dp->next= tableManager->pageNo;
        tableManager->pageNo= pageno;
        dp->prev= 0;
    }
}
