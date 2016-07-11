#include "bm_link_list_impl.h"
#include <stdlib.h>
#include "buffer_mgr.h"




LinkEntry* createLinkEntry(){
    LinkEntry *tempLinkEntry =((LinkEntry*) malloc (sizeof(LinkEntry)));
    tempLinkEntry->next = NULL;
    tempLinkEntry->prev = NULL;
    return tempLinkEntry;
}

void insertFrameAtEnd(BM_Pool_Replace_Data *data, BM_Pool_Page_Details *pageFrame)
{
 LinkEntry * tempLinkEntry=createLinkEntry();

  tempLinkEntry->frame= pageFrame;
  pageFrame->linkEntry= tempLinkEntry;// Assigning it to the LinkEntry in Frame
  if(data->head == NULL)
    data->head= data->tail= tempLinkEntry;
  else if(data->head == data->tail)
  {
    data->tail= tempLinkEntry;
    data->head->next= data->tail;
    data->tail->prev= data->head;
  } else
  {
    data->tail->next= tempLinkEntry;
    tempLinkEntry= data->tail->next;
    tempLinkEntry->prev= data->tail;
    data->tail= tempLinkEntry;
  }
}
BM_Pool_Page_Details* getFrame(BM_Pool_Replace_Data *data)
{
  LinkEntry *temp;
  BM_Pool_Page_Details *pageFrame;

  if(data->head == NULL)
    return NULL;

  
  pageFrame= data->head->frame;
  pageFrame->linkEntry= NULL;
  temp= data->head->next;
  free(data->head);
  if (temp)
    temp->prev = NULL;
  else
    data->tail= NULL;
  data->head = temp;

  return pageFrame;
}

void getFromPool(BM_Pool_Replace_Data *data, BM_Pool_Page_Details *pageFrame)
{
  LinkEntry *node;
  LinkEntry *temp;
  node= pageFrame->linkEntry;
  pageFrame->linkEntry= NULL;

  if (node == data->head && node == data->tail)
  {
    free(node);
    data->head= NULL;
    data->tail=NULL;
  } else if(node == data->head)
  {
    data->head= data->head->next;
    data->head->prev= NULL;
  } else if(node == data->tail)
  {
    data->tail= node->prev;
    data->tail->next= NULL;
  } else
  {
    temp= node->prev;
    temp->next= node->next;
    (temp->next)->prev= temp;
  }

  free(node);
}

// Remove all nodes
void removeLinkEntry(BM_Pool_Replace_Data *data)
{
  LinkEntry *temp;
  while (data->head != NULL)
  {
    temp= data->head->next;
    free(data->head);
    data->head= temp;
  }
  data->head= data->tail= NULL;
}
