#include "string.h"
#include "assert.h"
#include "scanner.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"

RC initRecordManager (void *mgmtData){
    initStorageManager();
    return(RC_OK);
}
RC shutdownRecordManager (){
    printf("Shutting down Record Manager !!!!\n");
    return(RC_OK);
}

RC freeSchema(Schema *schema){

    printf("Freeing up the schema !!!!!\n");
    free(schema->typeLength);
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->attrNames);
    free(schema);
    printf("Schema Memory Freed !!!!!");

    return (RC_OK);
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes,
                      int *typeLength, int keySize, int *keys){

        Schema *schema;
        schema= (Schema*) malloc(sizeof(Schema));
        schema->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttr);
        schema->typeLength= (int*) malloc( sizeof(int)*numAttr);
        schema->numAttr= numAttr;
        schema->attrNames= (char**) malloc( sizeof(char*)*numAttr);
        schema->keyAttrs= (int*) malloc( sizeof(int)*keySize);
        schema->keySize= keySize;

    int index;
    for(index=0; index<numAttr; index++){
       schema->attrNames[index]= (char*) malloc(64);
    }

    for(index=0; index<schema->numAttr; index++){
        if (index<keySize){
         schema->keyAttrs[index]= keys[index];
        }
        strncpy(schema->attrNames[index], attrNames[index], 64);
        schema->dataTypes[index]= dataTypes[index];

         if(schema->dataTypes[index] == DT_INT){
           schema->typeLength[index]= sizeof(int);
         }else if(schema->dataTypes[index] == DT_STRING){
             schema->typeLength[index]= typeLength[index];
         } else if(schema->dataTypes[index] == DT_FLOAT){
               schema->typeLength[index]= sizeof(float);
         }else if(schema->dataTypes[index] == DT_BOOL ){
             schema->typeLength[index]= sizeof(bool);
         }
    }

    return schema;
}
RC createTable (char *name, Schema *schema)
{
    SM_FileHandle fh;
    char data[PAGE_SIZE];
    char *indent= data;
    int recLen,index;
    RC rc;
    recLen= (4 * sizeof(int));
    recLen= recLen+(schema->numAttr * (76));
    if (recLen > PAGE_SIZE){
        return(RC_CANNOT_HANDLE_LARGE_SCHEMA);
    }
    recLen= getRecordSize(schema)+1;
    if (recLen > (int)(PAGE_SIZE - ((&((Pager*)0)->data) - ((char*)0)) )){
        return(RC_CANNOT_HANDLE_LARGE_RECORD);
    }

    memset(indent, 0, PAGE_SIZE);
    *(int*)indent = 0;
    indent+= sizeof(int);
    *(int*)indent = 0;
    indent+= sizeof(int);
    *(int*)indent = schema->numAttr;
    indent+= sizeof(int);
    *(int*)indent = schema->keySize;
    indent+= sizeof(int);

    for(index=0; index<schema->numAttr; index++){
       strncpy(indent, schema->attrNames[index], 64);
       indent+=64;

       *(int*)indent= (int) schema->dataTypes[index];
       indent+=4;

       *(int*)indent= (int) schema->typeLength[index];
       indent+=4;

       if (index<schema->keySize)
         *(int*)indent= (int) schema->keyAttrs[index];
       indent+=4;
    }
    rc=createPageFile(name);

    if (rc!= RC_OK)
        return rc;

    rc=openPageFile(name, &fh);

    if (rc != RC_OK)
        return rc;

    if ((rc=writeBlock(0, &fh, data)) != RC_OK)
        return rc;

    if ((rc=closePageFile(&fh)) != RC_OK)
        return rc;

    return(RC_OK);
}

RC openTable (RM_TableData *rel, char *name)
{
    char *indent;

    int numAttrs, keySize, index;
    RC rc;
    TableManager *tableManager;
    tableManager= (TableManager*) malloc( sizeof(TableManager) );
    rel->mgmtData= tableManager;
    rel->name= strdup(name);

    rc=initBufferPool(&tableManager->pool, rel->name, 20000,
                            RS_FIFO, NULL);

    if ( rc == RC_OK){
        rc=pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)0);
            if (rc == RC_OK){
                Schema *schema;
                schema= (Schema*) malloc(sizeof(Schema));

        /**
        Creating the proper indents/offset for the schema attribute
        **/
                indent= (char*) tableManager->pageHandler.data;
                tableManager->numTuples= *(int*)indent;
                indent+= sizeof(int);
                tableManager->pageNo= *(int*)indent;
                indent+= sizeof(int);
                numAttrs= *(int*)indent;
                indent+= sizeof(int);
                keySize= *(int*)indent;
                indent+= sizeof(int);
        /**
        Initializing the Schema Attribute
        **/
                schema->numAttr= numAttrs;
                schema->keySize= keySize;
                schema->attrNames= (char**) malloc( sizeof(char*)*numAttrs);
                schema->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttrs);
                schema->typeLength= (int*) malloc( sizeof(int)*numAttrs);
                schema->keyAttrs= (int*) malloc( sizeof(int)*keySize);
                    for(index=0; index<numAttrs; index++)
                        schema->attrNames[index]= (char*) malloc(64);
                        rel->schema= schema;
                            for(index=0; index<rel->schema->numAttr; index++){
                                strncpy(rel->schema->attrNames[index], indent, 64);
                                indent+=64;
                                rel->schema->dataTypes[index]= *(int*)indent;
                                indent+=4;
                                rel->schema->typeLength[index]= *(int*)indent;
                                indent+=4;
                                    if (index<keySize)
                                        rel->schema->keyAttrs[index]= *(int*)indent;
                                        indent+=4;
                                }
                unpinPage(&tableManager->pool, &tableManager->pageHandler);
                return(RC_OK);
        }else{
                return rc;
    }
}else{
    return rc ;
    }
}

RC closeTable (RM_TableData *rel)
{
    TableManager *tableManager;
    char *indent;
    RC rc;

    tableManager= rel->mgmtData;

    // Read page and prepare schema
    if ((rc=pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)0)) != RC_OK)
        return rc;
    indent= (char*) tableManager->pageHandler.data;

    markDirty(&tableManager->pool, &tableManager->pageHandler);
    *(int*)indent= tableManager->numTuples;
    unpinPage(&tableManager->pool, &tableManager->pageHandler);

    // CloseBM
    shutdownBufferPool(&tableManager->pool);

    // Deallocate schema
    free(rel->name);
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->typeLength);
    free(rel->schema->keyAttrs);
    free(rel->schema);
    rel->schema= NULL;
    free(rel->mgmtData);
    rel->mgmtData= NULL;

    return(RC_OK);
}
RC deleteTable (char *name){
    RC rc;
    if ((rc=destroyPageFile(name)) != RC_OK)
        return rc;

    return(RC_OK);
}
int getNumTuples (RM_TableData *rel){

    return(((TableManager*)rel->mgmtData)->numTuples);
}
RC insertRecord (RM_TableData *rel, Record *record){
    TableManager *tableManager= rel->mgmtData;
    PoolManager *pmd= tableManager->pool.dataHandler;
    RID *rowID= &record->id;
    Pager *dp;
    char *slotAddr;
    int _rSize=0;
    if (tableManager->pageNo == 0){
        if (appendEmptyBlock(&pmd->fhandler) != RC_OK){
            return(RC_INSERT_ERROR);
        }
        rowID->page= pmd->fhandler.totalNumPages-1;
        pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)rowID->page);
        dp= (Pager*) tableManager->pageHandler.data;
        rowID->slot= 0;
    }
    else {
        rowID->page= tableManager->pageNo;
        pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)rowID->page);
        dp= (Pager*) tableManager->pageHandler.data;
        int index;
    char *slotAddr= (char*) &dp->data;
    int totalSlots= ((int)(PAGE_SIZE - ((&((Pager*)0)->data) - ((char*)0)) )/(getRecordSize(rel->schema)+1));
    _rSize= getRecordSize(rel->schema)+1;
    rowID->slot= -1;
    for (index=0; index<totalSlots; index++){
        if (!(*(char*)slotAddr)){
            rowID->slot = index;
            break;
        }
        slotAddr = _rSize+slotAddr;
    }

        if (rowID->slot==-1){
            unpinPage(&tableManager->pool, &tableManager->pageHandler);
            if (appendEmptyBlock(&pmd->fhandler) != RC_OK)
                return(RC_INSERT_ERROR);
            rowID->page= pmd->fhandler.totalNumPages-1;
            pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)rowID->page);
            dp= (Pager*) tableManager->pageHandler.data;
            rowID->slot= 0;
        }
    }
    markDirty(&tableManager->pool, &tableManager->pageHandler);
    slotAddr= ((getRecordSize(rel->schema)+1)* rowID->slot)+ ((char*) &dp->data) ;
    memcpy(slotAddr+1, record->data, getRecordSize(rel->schema));
    /**Set the record marker **/
    (*(char*)slotAddr)=1;
    update( rowID->page,rel->schema,tableManager,dp);
    unpinPage(&tableManager->pool, &tableManager->pageHandler);
    tableManager->numTuples =tableManager->numTuples+1;

    return(RC_OK);
}

RC deleteRecord (RM_TableData *rel, RID id){
    char *slotAddr;
    TableManager *tableManager= rel->mgmtData;
    Pager *dp;
    if (id.page == -1 || id.slot == -1)
        return(RC_DELETE_ERROR);

    pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)id.page);
    dp= (Pager*) tableManager->pageHandler.data;
    slotAddr= ((getRecordSize(rel->schema)+1)* id.slot)+ ((char*) &dp->data) ;

    markDirty(&tableManager->pool, &tableManager->pageHandler);
    (*(char*)slotAddr)= -1;
    push(tableManager, dp, id.page);
    unpinPage(&tableManager->pool, &tableManager->pageHandler);
    tableManager->numTuples = tableManager->numTuples-1;
    return(RC_OK);
}

RC updateRecord (RM_TableData *rel, Record *record)
{
    RID *rowID= &record->id;
     if (rowID->page == -1 || rowID->slot == -1)
        return(RC_UPDATE_ERROR);


    TableManager *tableManager= rel->mgmtData;
    Pager *dp;
    char *slotAddr;
    pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)rowID->page);
    dp= (Pager*) tableManager->pageHandler.data;
    slotAddr= ((getRecordSize(rel->schema)+1)* rowID->slot)+ ((char*) &dp->data);

    markDirty(&tableManager->pool, &tableManager->pageHandler);
     memcpy(slotAddr+1, record->data, getRecordSize(rel->schema));
    unpinPage(&tableManager->pool, &tableManager->pageHandler);

    return(RC_OK);
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    TableManager *tableManager= rel->mgmtData;
    Pager *dp;
    char *slotAddr;
    if (id.page == -1 || id.slot == -1)
        return(RC_UPDATE_ERROR);

    pinPage(&tableManager->pool, &tableManager->pageHandler, (PageNumber)id.page);
    record->id= id;
    dp= (Pager*) tableManager->pageHandler.data;
    slotAddr= ((getRecordSize(rel->schema)+1)* id.slot)+ ((char*) &dp->data);
    memcpy( record->data,slotAddr+1,getRecordSize(rel->schema));
    unpinPage(&tableManager->pool, &tableManager->pageHandler);
    return(RC_OK);
}

int getRecordSize (Schema *schema){
    int _rSize= 0;
    int iterator;
        for (iterator=0; iterator<schema->numAttr; iterator++)
            _rSize+=schema->typeLength[iterator];
        return (_rSize);
}



RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    Scanner *scanner;
    scanner = (Scanner*) malloc( sizeof(Scanner) );
    scan->mgmtData = scanner;
    scanner->rowID.page= -1;
    scanner->rowID.slot= -1;
    scanner->counter= 0;
    scanner->expr= cond;
    scan->rel= rel;

    return(RC_OK);
}
RC next (RM_ScanHandle *scan, Record *record)
{
    Scanner *scanner= (Scanner*) scan->mgmtData;
    TableManager *tableManager= (TableManager*) scan->rel->mgmtData;
    char *slotAddr;
    Value *result = (Value *) malloc(sizeof(Value));	\
    result->v.boolV = TRUE;
    if (tableManager->numTuples == 0)
        return(RC_RM_NO_MORE_TUPLES);

/** Scan atleast once **/
   do
    {
        if (scanner->counter == 0){
            scanner->rowID.slot= 0;
            scanner->rowID.page= 1;
            pinPage(&tableManager->pool, &scanner->pageHandler, (PageNumber)scanner->rowID.page);
            scanner->dp= (Pager*) scanner->pageHandler.data;
        }
        else if (scanner->counter == tableManager->numTuples ){
            unpinPage(&tableManager->pool, &scanner->pageHandler);
            scanner->rowID.page= -1;
            scanner->counter = 0;
            scanner->dp= NULL;
            scanner->rowID.slot= -1;
            return(RC_RM_NO_MORE_TUPLES);
        }
        else{
            int totSlots= ((int)(PAGE_SIZE - ((&((Pager*)0)->data) - ((char*)0)) )/(getRecordSize(scan->rel->schema)+1) );
            scanner->rowID.slot++;
            if (scanner->rowID.slot== totSlots){
                scanner->rowID.slot= 0;
                 scanner->rowID.page++;
                pinPage(&tableManager->pool, &scanner->pageHandler, (PageNumber)scanner->rowID.page);
                scanner->dp= (Pager*) scanner->pageHandler.data;
            }
        }


        slotAddr= ((getRecordSize(scan->rel->schema)+1)* scanner->rowID.slot)+ ((char*) &(scanner->dp)->data);
        memcpy( record->data,slotAddr+1,getRecordSize(scan->rel->schema));
        record->id.page=scanner->rowID.page;
        record->id.slot=scanner->rowID.slot;
        scanner->counter++;


        if (scanner->expr != NULL)
            evalExpr(record, (scan->rel)->schema, scanner->expr, &result);
    } while (!result->v.boolV && scanner->expr != NULL);
    return(RC_OK);
}

RC closeScan (RM_ScanHandle *scan){
    Scanner *scanner= (Scanner*) scan->mgmtData;
    TableManager *tableManager= (TableManager*) scan->rel->mgmtData;
    if (scanner->counter > 0){
        unpinPage(&tableManager->pool, &scanner->pageHandler);
    }
    free(scan->mgmtData);
    scan->mgmtData= NULL;
    return(RC_OK);
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
     int index;
    void *offset= record->data;
    for (index=0; index<schema->numAttr; index++){
        if (index == attrNum){ // Check for the attribute number
            if(schema->dataTypes[attrNum] == DT_STRING){
                memcpy(offset, value->v.stringV, schema->typeLength[index]);
            } else{
                memcpy(offset, &value->v, schema->typeLength[index]);
            }
        }
        offset= offset+schema->typeLength[index];
    }
    return(RC_OK);
}


RC createRecord (Record **record, Schema *schema){
    int recordSize;
    Record *rec;
    rec= (Record*) malloc( sizeof(Record) );
    recordSize= getRecordSize(schema);
    rec->data= (char*) malloc(recordSize);
    memset(rec->data, 0, recordSize);
    /**resetting the record slot to -1**/
    rec->id.page= -1;
    rec->id.slot= -1;
    *record= rec;

    return (RC_OK);
}
RC freeRecord (Record *record){
    free(record->data);
    free(record);
    return(RC_OK);
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

    Value *val= (Value*) malloc(sizeof(Value));
    void *offset= record->data;
    int index;
    for (index=0; index<schema->numAttr; index++){
        if (index==attrNum){
            val->dt= schema->dataTypes[attrNum];
             if(schema->dataTypes[attrNum] == DT_STRING){
               val->v.stringV= (char*) malloc(schema->typeLength[index]+1);
                    memcpy(val->v.stringV, offset, schema->typeLength[index]);
                    val->v.stringV[schema->typeLength[index]]='\0';
            } else{
                     memcpy(&val->v, offset, schema->typeLength[index]);
            }
            break;
        }
        offset= schema->typeLength[index]+offset;
    }
    *value= val;
    return RC_OK;
}
