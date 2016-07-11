//Including standard and user-defined header files
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "storage_mgr.h"
#include "constants.h"

static Storage_Manager storageManager;
//START
/**  This function initializes the storage
        manager if not already initialized
**/
void initStorageManager (void){
        storageManager.initializer= 1;
        printf("\n********************Storage Manager Initialized Successfully***************************");
}
//END


static int checkForFileHandler(SM_FileHandle *fHandle)
{
    int i;
    int handleFound=0;
    int openFileCount= storageManager.openFileCount;
    for(i=0; i<openFileCount; i++){
        if (storageManager.handlers[i] != 0)
                if (storageManager.handlers[i] == fHandle){
                        handleFound = 1;// this is an indicator that file is found to be opened
                        break; //this will terminate the loop for unneccessary looping
        }
    }
    return handleFound;
}

RC createPageFile (char *fileName)
{
    printf("\n************Inside createPageFile()****************************");
    FILE *fp;
    if (storageManager.initializer != 1){
            printf("\n****************Storage Manager was not initialized !!!********************");
            return RC_STORAGE_NOT_INTIALIZED;// NEW DEFINED returnED CODE
    }
    fp=fopen(fileName, "w+");
    if(NULL != fp ){
        fseek(fp, PAGE_SIZE, SEEK_SET);
        fputc('\n', fp);
        fclose(fp);
        return RC_OK;
    }
    return RC_FILE_CREATION_FAILED;
}

/* Open the page file and register it with Storage Engine */
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    printf("\n*******************Inside openPageFile()********************");
     int desc;
    int fileNameLen;
    int index;
    struct stat stbuf;// POSIX COMMAND USED TO FIND THE FILE SIZE ,
    if (storageManager.initializer == 0){
            printf("\n****************Storage Manager not initialized*******************");
        return RC_STORAGE_NOT_INTIALIZED;
    }

    if (checkForFileHandler(fHandle) == 1){
            printf("\n******************File Is Already Opened*************************");
        return RC_FILE_AlREADY_OPEN;
    }


    if ((desc= open(fileName, O_RDWR, S_IRWXU)) > 0){
        int isFileRegistered=0;
        stat(fileName, &stbuf);
        fHandle->totalNumPages= stbuf.st_size/PAGE_SIZE;
        printf("\n\n*************Total Number of Pages : : %i", fHandle->totalNumPages);
        //Initialize the Handler with the default
        //value once the open function is called by the client
        fHandle->curPagePos= 0;
        fHandle->mgmtInfo = desc;//desc;// This will be used while reading the file , "Book Keeping "
        fileNameLen=strlen(fileName)+1;
        fHandle->fileName= (char*) malloc(fileNameLen);
        fHandle->fileName=fileName;

        /**https://www.securecoding.cert.org/confluence/display/seccode/FIO19-C.
        +Do+not+use+fseek%28%29+and+ftell%28%29+to+compute+the+size+of+a+regular+file
        **/

    /**This block of code will basically handle the registration
     of the file with the Storage Manager**/

    for(index=0; index<MAX_FILES ; index++){
        //TO DO: Extract this into method to make it more modular
         if (storageManager.handlers[index] == 0){
            storageManager.handlers[index]= fHandle;
            storageManager.openFileCount++;
            isFileRegistered=1;
           break;
        }
    }
        if(isFileRegistered == 1)return RC_OK;
        else{
                printf("\n\n*********openPageFile():: ERROR: MAXIMUM FILE HANDLER COUNT REACHED**************");
        return RC_TO_MANY_HANDLERS_OPENED;
        }
    }
    printf("\n\n*********openPageFile():: ERROR: FILE NOT FOUND**************");
    return RC_FILE_NOT_FOUND;
}


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    printf("\n****************Inside readBlock()********************************* ");
    if (storageManager.initializer == 0){
            printf("****************Storage Manager was not initialized !!!********************\n ");
            return RC_STORAGE_NOT_INTIALIZED;// NEW DEFINED returnED CODE
    }
    if (checkForFileHandler(fHandle) == 0){
            printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if(pageNum < 0 || pageNum >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        int pageReadSize;
        lseek(fHandle->mgmtInfo,PAGE_SIZE*(pageNum),SEEK_SET);
        //The value stored in mgmtInfo is the file descriptor
        //Courtsey :: www.cprogrammming.com
        pageReadSize = read(fHandle->mgmtInfo,memPage,PAGE_SIZE);// read the content from the fileHandler into the memory page
        if((pageReadSize) >= 4095){
            fHandle->curPagePos=pageNum;//Update the current Position of the cursor
            printf("\n**Exiting read");
            return RC_OK;
        }
        else{
                printf("\n*************Inside readBlock():: Error Reading File ::File Descriptor is 0***********");
            return RC_READ_ERROR;
        }
    }
}

int getBlockPos(SM_FileHandle *fHandle){
    printf("\n***********************Inside getBlockPos()*********************");
return fHandle->curPagePos;//return the current page position
}


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

printf("\n*******************Inside readFirstBlock**********************");
int pageReadSize=0;
int page=0;
if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

     if (checkForFileHandler(fHandle) == 0){
              printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    page=0;
 if(page < 0 || page >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }


    lseek(fHandle->mgmtInfo,0,SEEK_SET);
    pageReadSize=read(fHandle->mgmtInfo,memPage,PAGE_SIZE);

     if(pageReadSize >= PAGE_SIZE){
            fHandle->curPagePos=0;//Update the current Position of the cursor
            return RC_OK;
    }
    else{
            printf("\n\n*************Inside readFirstBlock():: Error Reading File ::File Descriptor is 0***********");
                return RC_READ_ERROR;
    }
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

printf("\n\n*******************Inside readFirstBlock()**********************");
int pageReadSize=0;
int page=0;
if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

     if (checkForFileHandler(fHandle) == 0){
              printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    page =(fHandle->curPagePos)-1;

     if(page < 0 || page >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }

    lseek(fHandle->mgmtInfo,page*PAGE_SIZE,SEEK_SET);
    pageReadSize=read(fHandle->mgmtInfo,memPage,PAGE_SIZE);

     if(pageReadSize >= PAGE_SIZE){
            fHandle->curPagePos=page;//Update the current Position of the cursor
            return RC_OK;}
    else{
            printf("\n*************Inside readPrevoisBlock():: Error Reading File ::File Descriptor is 0***********");
            return RC_READ_ERROR;
    }
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    printf("\n\n*******************Inside readCurrentBlock()**********************");
int pageReadSize=0;
int currentPos=0;
if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

    if (checkForFileHandler(fHandle) == 0){
              printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    currentPos=fHandle->curPagePos;
    if(currentPos < 0 || currentPos >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }
    lseek(fHandle->mgmtInfo,PAGE_SIZE*currentPos,SEEK_SET);
    pageReadSize=read(fHandle->mgmtInfo,memPage,PAGE_SIZE);

     if(pageReadSize >= PAGE_SIZE){
            fHandle->curPagePos=currentPos;//Update the current Position of the cursor
            return RC_OK;
    }
    else{
            printf("\n\n*************Inside readBlock():: Error Reading File ::File Descriptor is 0***********");
                return RC_READ_ERROR;
    }
}
RC shutdownStorageManager()
{
    if (storageManager.openFileCount>0)
        return (RC_FILE_AlREADY_OPEN);

    storageManager.initializer= 0;

    return (RC_OK);
}


RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    printf("\n\n*******************Inside readLastBlock()**********************");
int pageReadSize=0;
int page=0;
if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

     if (checkForFileHandler(fHandle) == 0){
            printf("\nError : %s NO INITIALIZED",fHandle->fileName);
        return RC_FILE_HANDLE_NOT_INIT;
    }
 page=fHandle->totalNumPages-1;
    if(page < 0 || page >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }

    lseek(fHandle->mgmtInfo,(PAGE_SIZE*page),SEEK_SET);
    pageReadSize=read(fHandle->mgmtInfo,memPage,PAGE_SIZE);

     if(pageReadSize >= PAGE_SIZE){
            fHandle->curPagePos=fHandle->totalNumPages-1;//Update the current Position of the cursor
            return RC_OK;
    }
    else{
            printf("\n\n*************Inside readLastBlock():: Error Reading File ::File Descriptor is 0***********");
                return RC_READ_ERROR;
    }
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
printf("\n*******************Inside readLastBlock()**********************");
int pageReadSize=0;
int page=0;
if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

    if (checkForFileHandler(fHandle) == 0){
              printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    page =fHandle->curPagePos+1;
     if(page < 0 || page >= fHandle -> totalNumPages || (fHandle -> mgmtInfo) == NULL ){
      return RC_READ_NON_EXISTING_PAGE;
    }
    lseek(fHandle->mgmtInfo,page*PAGE_SIZE,SEEK_SET);
    pageReadSize=read(fHandle->mgmtInfo,memPage,PAGE_SIZE);

     if(pageReadSize >= PAGE_SIZE){
            fHandle->curPagePos=page;//Update the current Position of the cursor
            return RC_OK;}
    else{
            printf("\n*************Inside readBlock():: Error Reading File ::File Descriptor is 0***********");
            return RC_READ_ERROR;
    }
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle,SM_PageHandle memPage){

       if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

    if (checkForFileHandler(fHandle) == 0){
             printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if(pageNum < 0 || (fHandle -> mgmtInfo) == NULL ){
      return RC_WRITE_NON_EXISTING_PAGE;
    }
    int writtenSize=0;
    lseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);
    int totalPages=fHandle->totalNumPages;
    writtenSize = write(fHandle->mgmtInfo, memPage,PAGE_SIZE);

    if(writtenSize>=PAGE_SIZE){
          if(pageNum>=totalPages)fHandle->totalNumPages=++pageNum;
          return RC_OK;
    }else{
    return RC_WRITE_FAILED;
    }
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
int pageNum=0;
   if (storageManager.initializer == 0)
        return RC_STORAGE_NOT_INTIALIZED;

    if (checkForFileHandler(fHandle) == 0){
             printf("\n*******************FILE HANDLER NOT INITIALIZED**************************");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    pageNum=fHandle->curPagePos;
    if(pageNum < 0 || (fHandle -> mgmtInfo) == NULL ){
      return RC_WRITE_NON_EXISTING_PAGE;
    }

    int writtenSize=0;
    lseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);
    int totalPages=fHandle->totalNumPages;
    writtenSize = write(fHandle->mgmtInfo,memPage,PAGE_SIZE);

    if(writtenSize>=PAGE_SIZE){
          if(pageNum>=totalPages)fHandle->totalNumPages=++pageNum;
          return RC_OK;
    }else{
        return RC_WRITE_FAILED;
    }
}

/**
@param: char* fileName
This method will destroy the file from the storage manager.
It returns the error code RC_UNABLE_TO_DELETE in case of the error or else RC_OK incase of Successful deletion
**/
RC destroyPageFile (char *fileName){
    printf("\n**********************Inside destoryPageFile()*********************");

   if(unlink(fileName)){
        printf("\n unlink() failed -Permission Denied");
        return RC_UNABLE_TO_DELETE;
    }
    else{
        printf("\n********************File deleted successfully************************");
        return RC_OK;
    }
}
RC closePageFile (SM_FileHandle *fHandle){
    int status ;
    status = close(fHandle -> mgmtInfo);
    if(close < 0)
        return RC_ERROR_IN_FILE_CLOSE;
       else{

        fHandle->fileName= NULL;
        fHandle->mgmtInfo= NULL;
        int currentHandleIndex;
        int openHandle= storageManager.openFileCount;
            // this block of code will decouple the file form the Storage Manager
        for(currentHandleIndex=0; currentHandleIndex< openHandle; currentHandleIndex++){
        if (storageManager.handlers[currentHandleIndex] != 0)
            if (storageManager.handlers[currentHandleIndex] == fHandle){
                    storageManager.handlers[currentHandleIndex] = 0; //decouple it once the match is found
                        break;
        }
    }
            return RC_OK;
       }
}

RC appendEmptyBlock(SM_FileHandle *fHandle){

       char emptyPage[PAGE_SIZE];
    memset(emptyPage, 0, PAGE_SIZE);
	return writeBytes (fHandle->totalNumPages, fHandle, (SM_PageHandle) &emptyPage);

}

RC ensureCapacity(int numberOfPages,SM_FileHandle *fHandle){
  /**  int totalPages,writeSize,extendPageSize=0;
    totalPages=fHandle->totalNumPages;
    char * writeBuffer;

    if(pageNumber>=totalPages){
        extendPageSize=(pageNumber-totalPages)*PAGE_SIZE;
        writeBuffer=(char*)malloc(extendPageSize*sizeof(char));
        memset(writeBuffer,0,extendPageSize);
        writeSize = write(fHandle->mgmtInfo,writeBuffer,extendPageSize);
        if(writeSize>=extendPageSize){
                fHandle->totalNumPages=pageNumber+1;
                  printf("\n*********************Ensure Capacity Successful : TotalPages --->:: %d",fHandle->totalNumPages);

            return RC_OK;
        }
        else
            return RC_WRITE_FAILED;
    }
    return RC_OK;**/

    char dummyPage[PAGE_SIZE];

    if (numberOfPages > fHandle->totalNumPages)
    {
        memset(dummyPage, 0, PAGE_SIZE);
        return writeBytes (numberOfPages-1, fHandle, (SM_PageHandle) &dummyPage);
    }
    return(RC_OK);
}

RC writeBytes(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(pageNum > 0){
    lseek(fHandle->mgmtInfo, (pageNum * PAGE_SIZE), SEEK_SET);
    if (pageNum >= fHandle->totalNumPages)
        fHandle->totalNumPages= pageNum+1;
    if (write(fHandle->mgmtInfo, memPage, (int)PAGE_SIZE) < (int)PAGE_SIZE)
        return(RC_WRITE_FAILED);
    return(RC_OK);
    }
   return(RC_READ_NON_EXISTING_PAGE);
}
