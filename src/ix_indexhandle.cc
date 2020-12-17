#include "ix.h"
#include<iostream>
#include<cstring>
#include"ix_internal.h"
#include<string>
using namespace std;
IX_IndexHandle::IX_IndexHandle(){
    isOpen = FALSE;
    headerModified = FALSE;

    indexHeader.rootPage = IX_NO_PAGE;

}

IX_IndexHandle::~IX_IndexHandle(){

}


RC IX_IndexHandle::InsertEntry(void* pData, const RID &rid){
    if(!isOpen){
        return IX_INDEX_CLOSED;
    }

    if(pData == NULL){
        return IX_NULL_ENTRY;
    }

    RC rc;

    //從index header獲得root node
    PageNum rootPage = indexHeader.rootPage;
    int degree = indexHeader.degree;
    int attrLength = indexHeader.attrLength;
    AttrType attrType = indexHeader.attrType;

    //如果root不存在
    if(rootPage == IX_NO_PAGE){
        PF_PageHandle pfPH;
        if((rc = pfFH.AllocatePage(pfPH))){
            return rc;
    
        }

        char * pageData;
        if((rc = pfPH.GetData(pageData))){
            return rc;
        }

        PageNum pageNum;
        if((rc = pfPH.GetPageNum(pageNum))){
            return pageNum;
        }

        if((rc = pfFH.MarkDirty(pageNum))){
            return rc;
        }

        IX_NodeHeader *  rootHeader  = new IX_NodeHeader;
        rootHeader->numberKeys = 1;
        rootHeader->keyCapacity = degree;
        rootHeader->type = ROOT_LEAF;
        rootHeader->parent = IX_NO_PAGE;
        rootHeader->parent = IX_NO_PAGE;

        memcpy(pageData,(char*)rootHeader,sizeof(IX_NodeHeader));
        delete rootHeader;


        //申請nodeValue array給node節點
        IX_NodeValue* valueArray = new IX_NodeValue[degree+1];
        for(int i = 0;i <= degree;i++){
            valueArray[i] = dummyNodeValue;
        }

        valueArray[0].state = RID_FILED;
        valueArray[0].rid = dummyRID;
        valueArray[0].page = IX_NO_PAGE;

        int valueOffeset = sizeof(IX_NodeHeader)+degree*attrLength;
        memcpy(pageData + valueOffeset,(char*)valueArray,sizeof(IX_NodeValue)*(degree+1));

        //申請key array 給node節點
        char * keyData = pageData +sizeof(IX_NodeHeader);
        if(attrType == INT){
            int *keyArray = new int[degree];
            for(int i = 0;i < degree;i++){
                keyArray[i] = -1;
            }

            int givenKey = *static_cast<int*>(pData);
            keyArray[0] = givenKey;
            memcpy(keyData, keyArray, attrLength*degree);
            delete[] keyArray;
        }
        else if(attrType == FLOAT){
            float *keyArray = new float[degree];
            for(int i = 0;i < degree;i++){
                keyArray[i] = -1;
            }

            float givenKey = *static_cast<float*>(pData);
            keyArray[0] = givenKey;
            memcpy(keyData, keyArray, attrLength*degree);
            delete[] keyArray;
            else
            {
                char* keyArray = new char[degree*attrLength];
                for(int i = 0;i < attrLength*degree;i++){
                    keyArray[i] = ' ';
                }
                char * givenKeyChar = static_cast<char*>(pData);
                strcpy(keyArray,givenKeyChar);
                memcpy(keyData,keyArray,attrLength*degree);
                delete[] keyArray;
            }

            headerModified = TRUE;
            indexHeader.rootPage = pageNum;

            if((rc = pfFH.UnpinPage(pageNum))){
                return rc;
            }
            return OK_RC;
        }//如果root node存在
        else{
            PF_PageHandle pfPH;
            char* pageData;
            if((rc = pfFH.GetThisPage(rootPage,pfPH))){
                return rc;
            }

            if((rc = pfPH.GetData(pageData))){
                return rc;
            }

            if((rc = pfFH.MarkDirty(rootPage))){
                return rc;
            }


            IX_NodeHeader* nodeHeader = (IX_NodeHeader*)pageData;
            IX_NodeType type = nodeHeader->type;
            int numberKeys = nodeHeader->numberKeys;
            int keyCapacity = nodeHeader->keyCapacity;
            char* keyData = pageData +sizeof(IX_NodeHeader);
            char *valueData = keyData +attrLength*degree;
            IX_NodeValue* valueArray = (IX_NodeValue*)valueData;

            if(type == ROOT_LEAF){
                int index = -1;
                if(attrType == INT){
                    int* keyArray = (int*)keyData;
                    int givenKey = *static_cast<int*>(pData);
                    for(int i = 0;i <numberKeys;i++){
                        if(keyArray[i] == givenKey){
                            index = i;
                            break;
                        }
                    }
                }
                else if(type == FLOAT){
                    float* keyArray = (float*)keyData;
                    float givenKey = *static_cast<float*>(pData);
                    for(int i = 0;i < numberKeys;i++){
                        if(keyArray[i] == givenKey){
                            index = i;
                            break;
                        }
                    }
                }
                else{
                    char* keyArray = (char*)keyData;
                    char* givenKeyChar = static_cast<char*>(pData);
                    string givenKey(givenKeyChar);
                    for(int  i= 0;i <numberKeys;i++){
                        string currentKey(keyArray +i*attrLength);
                        if(currentKey  == givenKey){
                            index = i;
                            break;
                        }
                    }
                }

                //key存在，檢查RID是否相同
                if(index != -1){
                    IX_NodeValue value = valueArray[index];
                    if(compareRIDs(value.rid, rid)){
                        return IX_ENTRY_EXISTS;
                    }
                    else{
                        //先獲得bucketPage
                        PageNum bucketPage = value.page;
                        if(bucketPage == IX_NO_PAGE){
                            //新建page
                            PF_PageHandle bucketPFPH;
                            if((rc = pfFH.AllocatePage(bucketPFPH))){
                                return rc;
                            }

                            if((rc = bucketPFPH.GetPageNum(bucketPage))){
                                return rc;
                            }

                            valueArray[index].page = bucketPage;
                            memcpy(valueData,(char*)valueArray,sizeof(IX_NodeValue)*(degree +1));

                            //初始化和復制bucket header
                            IX_BucketPageHeader* bucketHeader = new IX_BucketPageHeader;
                            bucketHeader->numberRecords = 1;
                            int recordCapacity = (PF_PAGE_SIZE - sizeof(IX_BucketPageHeader)) / sizeof(RID);
                            bucketHeader->recordCapacity = recordCapacity;
                            bucketHeader->parentNode = rootPage;

                            char* bucketData;
                            if((rc = bucketPFPH.GetData(bucketData))){
                                return rc;
                            }

                            if((rc = pfFH.MarkDirty(bucketPage))){
                                return rc;
                            }

                            memcpy(bucketData, (char*)bucketHeader, sizeof(IX_BucketPageHeader));
                            delete bucketHeader;

                            //復制RID to bucket
                            RID* ridList = new RID[recordCapacity];
                            for(int i = 0;i < recordCapacity;i++){
                                ridList[i] = dummyRID;
                            }
                            ridList[0] = rid;
                            memcpy(bucketData +sizeof(IX_BucketPageHeader), ridList, sizeof(RID)*recordCapacity);
                            delete[] ridList;

                            if((rc = pfFH.UnpinPage(bucketPage))){
                                return rc;
                            }
                        }
                        else{
                            //從bucketpage中獲得數據
                            PF_PageHandle bucketPFPH;
                            char * bucketData;
                            if((rc = pfFH.GetThisPage(bucketPage,bucketPFPH))){
                                return rc;
                            }
                            if((rc = bucketPFPH.GetData(bucketData))){
                                return rc;
                            }

                            if((rc = pfFH.MarkDirty(bucketPage))){
                                return rc;
                            }

                            //獲取bucket page header
                            IX_BucketPageHeader* bucketHeader = (IX_BucketPageHeader*)bucketData;
                            int numberRecords = bucketHeader->numberRecords;
                            int recordCapacity = bucketHeader->recordCapacity;

                            char* ridData = bucketData +sizeof(IX_BucketPageHeader);
                            RID* ridList = (RID*) ridData;
                            for(int i = 0;i <numberRecords;i++){
                                if(compareRIDs(ridList[i],rid)){
                                    return IX_ENTRY_EXISTS;
                                }
                            }

                            if(numberRecords == recordCapacity){
                                return IX_BUCKET_FULL;
                            }

                            ridList[numberRecords] = rid;
                            memcpy(ridData, (char*)ridList,sizeof(RID)*recordCapacity);

                            bucketHeader->numberRecords++;
                            memcpy(bucketData,(char*)bucketHeader,sizeof(IX_BucketPageHeader));

                            if((rc = pfFH.UnpinPage(bucketPage))){
                                return rc;
                            }
                        }
                    }
                }//當key不在當前node中
                else{
                    //當前node節點還未滿
                    if (numberKeys < keyCapacity) {
                    int position = numberKeys;
                    if (attrType == INT) {
                        int* keyArray = (int*) keyData;
                        int givenKey = *static_cast<int*>(pData);
                        for (int i=0; i<numberKeys; i++) {
                            if (givenKey < keyArray[i]) {
                                position = i;
                                break;
                            }
                        }

                        // 往後移動其他節點，插入節點
                        for (int i=numberKeys; i>position; i--) {
                            keyArray[i] = keyArray[i-1];
                            valueArray[i] = valueArray[i-1];
                        }
                        keyArray[position] = givenKey;
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                    }
                    else if (attrType == FLOAT) {
                        float* keyArray = (float*) keyData;
                        float givenKey = *static_cast<float*>(pData);
                        for (int i=0; i<numberKeys; i++) {
                            if (givenKey < keyArray[i]) {
                                position = i;
                                break;
                            }
                        }

                        for (int i=numberKeys; i>position; i--) {
                            keyArray[i] = keyArray[i-1];
                            valueArray[i] = valueArray[i-1];
                        }
                        keyArray[position] = givenKey;
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                    }
                    else {
                        char* keyArray = (char*) keyData;
                        char* givenKeyChar = static_cast<char*>(pData);
                        string givenKey(givenKeyChar);
                        for (int i=0; i<numberKeys; i++) {
                            string currentKey(keyArray + i*attrLength);
                            if (givenKey < currentKey) {
                                position = i;
                                break;
                            }
                        }

                        for (int i=numberKeys; i>position; i--) {
                            for (int j=0; j<attrLength; j++) {
                                keyArray[i*attrLength + j] = keyArray[(i-1)*attrLength + j];
                            }
                            valueArray[i] = valueArray[i-1];
                        }
                        strcpy(keyArray + position*attrLength, givenKey.c_str());
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                    }

                    valueArray[position].state = RID_FILLED;
                    valueArray[position].rid = rid;
                    valueArray[position].page = IX_NO_PAGE;
                    nodeHeader->numberKeys++;

                    memcpy(pageData, (char*) nodeHeader, sizeof(IX_NodeHeader));
                    memcpy(valueData, (char*) valueArray, sizeof(IX_NodeValue)*(degree+1));
                }

                else{
                    // 新建node page
                    PF_PageHandle newPFPH;
                    char* newPageData;
                    PageNum newPageNumber;
                    if ((rc = pfFH.AllocatePage(newPFPH))) {
                        return rc;
                    }
                    if ((rc = newPFPH.GetData(newPageData))) {
                        return rc;
                    }
                    if ((rc = newPFPH.GetPageNum(newPageNumber))) {
                        return rc;
                    }
                    if ((rc = pfFH.MarkDirty(newPageNumber))) {
                        return rc;
                    }

                    //將一半的key復制到新節點
                    if (attrType == INT) {
                        int* keyArray = (int*) keyData;
                        int givenKey = *static_cast<int*>(pData);
                        IX_NodeHeader* newNodeHeader = new IX_NodeHeader;
                        int* newKeyArray = new int[degree];
                        for (int i=0; i<degree; i++) {
                            newKeyArray[i] = -1;
                        }
                        IX_NodeValue* newValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newValueArray[i] = dummyNodeValue;
                        }
                        for (int i=numberKeys/2; i<numberKeys; i++) {
                            newKeyArray[i-numberKeys/2] = keyArray[i];
                            newValueArray[i-numberKeys/2] = valueArray[i];
                        }

                        // 更新 node headers
                        nodeHeader->numberKeys = numberKeys/2;
                        nodeHeader->type = LEAF;
                        newNodeHeader->numberKeys = numberKeys - numberKeys/2;
                        newNodeHeader->keyCapacity = keyCapacity;
                        newNodeHeader->type = LEAF;

                        // 更新 左節點的 last pointer 
                        valueArray[keyCapacity].state = PAGE_ONLY;
                        valueArray[keyCapacity].page = newPageNumber;
                        valueArray[keyCapacity].rid = dummyRID;

                        // 插入新key
                        if (givenKey < newKeyArray[0]) {
                            // 插入左節點
                            int position = numberKeys/2;
                            for (int i=0; i<numberKeys/2; i++) {
                                if (givenKey < keyArray[i]) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys/2; i>position; i--) {
                                keyArray[i] = keyArray[i-1];
                                valueArray[i] = valueArray[i-1];
                            }
                            keyArray[position] = givenKey;
                            valueArray[position].state = RID_FILLED;
                            valueArray[position].rid = rid;
                            valueArray[position].page = IX_NO_PAGE;

                            nodeHeader->numberKeys++;
                        }
                        else {
                            // 插入右節點
                            int position = numberKeys - numberKeys/2;
                            for (int i=0; i< numberKeys - numberKeys/2; i++) {
                                if (givenKey < newKeyArray[i]) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys-numberKeys/2; i>position; i--) {
                                newKeyArray[i] = newKeyArray[i-1];
                                newValueArray[i] = newValueArray[i-1];
                            }
                            newKeyArray[position] = givenKey;
                            newValueArray[position].state = RID_FILLED;
                            newValueArray[position].rid = rid;
                            newValueArray[position].page = IX_NO_PAGE;

                            newNodeHeader->numberKeys++;
                        }

                        // 新建root page
                        PF_PageHandle newRootPFPH;
                        char* newRootPageData;
                        PageNum newRootPage;
                        if ((rc = pfFH.AllocatePage(newRootPFPH))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetData(newRootPageData))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetPageNum(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.MarkDirty(newRootPage))) {
                            return rc;
                        }

                        // 設置 keys 和 values
                        IX_NodeHeader* newRootNodeHeader = new IX_NodeHeader;
                        int* newRootKeyArray = new int[degree];
                        for (int i=0; i<degree; i++) {
                            newRootKeyArray[i] = -1;
                        }
                        IX_NodeValue* newRootValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newRootValueArray[i] = dummyNodeValue;
                        }
                        newRootNodeHeader->numberKeys = 1;
                        newRootNodeHeader->keyCapacity = keyCapacity;
                        newRootNodeHeader->type = ROOT;
                        newRootNodeHeader->parent = IX_NO_PAGE;
                        newRootNodeHeader->left = IX_NO_PAGE;

                        newRootKeyArray[0] = newKeyArray[0];
                        newRootValueArray[0].state = PAGE_ONLY;
                        newRootValueArray[0].page = rootPage;
                        newRootValueArray[0].rid = dummyRID;
                        newRootValueArray[1].state = PAGE_ONLY;
                        newRootValueArray[1].page = newPageNumber;
                        newRootValueArray[1].rid = dummyRID;

                        // 復制data 到 the root page
                        memcpy(newRootPageData, (char*) newRootNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newRootPageData+sizeof(IX_NodeHeader), (char*) newRootKeyArray, attrLength*degree);
                        memcpy(newRootPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newRootValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newRootNodeHeader;
                        delete[] newRootKeyArray;
                        delete[] newRootValueArray;

                        // 更新父節點指針
                        nodeHeader->parent = newRootPage;
                        newNodeHeader->parent = newRootPage;
                        newNodeHeader->left = rootPage;

                        // 復制 data 到the pages
                        memcpy(newPageData, (char*) newNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newPageData+sizeof(IX_NodeHeader), (char*) newKeyArray, attrLength*degree);
                        memcpy(newPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newNodeHeader;
                        delete[] newKeyArray;
                        delete[] newValueArray;

                        memcpy(pageData, (char*) nodeHeader, sizeof(IX_NodeHeader));
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                        memcpy(valueData, (char*) valueArray, sizeof(IX_NodeValue)*(degree+1));

                        //更新index header中的rootPage
                        indexHeader.rootPage = newRootPage;
                        headerModified = TRUE;
                        if ((rc = pfFH.UnpinPage(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.UnpinPage(newPageNumber))) {
                            return rc;
                        }
                    }

                    else if (attrType == FLOAT) {
                        float* keyArray = (float*) keyData;
                        float givenKey = *static_cast<float*>(pData);
                        IX_NodeHeader* newNodeHeader = new IX_NodeHeader;
                        float* newKeyArray = new float[degree];
                        for (int i=0; i<degree; i++) {
                            newKeyArray[i] = (float) -1;
                        }
                        IX_NodeValue* newValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newValueArray[i] = dummyNodeValue;
                        }
                        for (int i=numberKeys/2; i<numberKeys; i++) {
                            newKeyArray[i-numberKeys/2] = keyArray[i];
                            newValueArray[i-numberKeys/2] = valueArray[i];
                        }

                        nodeHeader->numberKeys = numberKeys/2;
                        nodeHeader->type = LEAF;
                        newNodeHeader->numberKeys = numberKeys - numberKeys/2;
                        newNodeHeader->keyCapacity = keyCapacity;
                        newNodeHeader->type = LEAF;

                        valueArray[keyCapacity].state = PAGE_ONLY;
                        valueArray[keyCapacity].page = newPageNumber;
                        valueArray[keyCapacity].rid = dummyRID;

                        if (givenKey < newKeyArray[0]) {
                            int position = numberKeys/2;
                            for (int i=0; i<numberKeys/2; i++) {
                                if (givenKey < keyArray[i]) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys/2; i>position; i--) {
                                keyArray[i] = keyArray[i-1];
                                valueArray[i] = valueArray[i-1];
                            }
                            keyArray[position] = givenKey;
                            valueArray[position].state = RID_FILLED;
                            valueArray[position].rid = rid;
                            valueArray[position].page = IX_NO_PAGE;

                            nodeHeader->numberKeys++;
                        }
                        else {
                            int position = numberKeys - numberKeys/2;
                            for (int i=0; i< numberKeys - numberKeys/2; i++) {
                                if (givenKey < newKeyArray[i]) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys-numberKeys/2; i>position; i--) {
                                newKeyArray[i] = newKeyArray[i-1];
                                newValueArray[i] = newValueArray[i-1];
                            }
                            newKeyArray[position] = givenKey;
                            newValueArray[position].state = RID_FILLED;
                            newValueArray[position].rid = rid;
                            newValueArray[position].page = IX_NO_PAGE;

                            newNodeHeader->numberKeys++;
                        }

                        PF_PageHandle newRootPFPH;
                        char* newRootPageData;
                        PageNum newRootPage;
                        if ((rc = pfFH.AllocatePage(newRootPFPH))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetData(newRootPageData))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetPageNum(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.MarkDirty(newRootPage))) {
                            return rc;
                        }

                        IX_NodeHeader* newRootNodeHeader = new IX_NodeHeader;
                        float* newRootKeyArray = new float[degree];
                        for (int i=0; i<degree; i++) {
                            newRootKeyArray[i] = (float) -1;
                        }
                        IX_NodeValue* newRootValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newRootValueArray[i] = dummyNodeValue;
                        }
                        newRootNodeHeader->numberKeys = 1;
                        newRootNodeHeader->keyCapacity = keyCapacity;
                        newRootNodeHeader->type = ROOT;
                        newRootNodeHeader->parent = IX_NO_PAGE;
                        newRootNodeHeader->left = IX_NO_PAGE;

                        newRootKeyArray[0] = newKeyArray[0];
                        newRootValueArray[0].state = PAGE_ONLY;
                        newRootValueArray[0].page = rootPage;
                        newRootValueArray[0].rid = dummyRID;
                        newRootValueArray[1].state = PAGE_ONLY;
                        newRootValueArray[1].page = newPageNumber;
                        newRootValueArray[1].rid = dummyRID;

                        memcpy(newRootPageData, (char*) newRootNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newRootPageData+sizeof(IX_NodeHeader), (char*) newRootKeyArray, attrLength*degree);
                        memcpy(newRootPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newRootValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newRootNodeHeader;
                        delete[] newRootKeyArray;
                        delete[] newRootValueArray;

                        nodeHeader->parent = newRootPage;
                        newNodeHeader->parent = newRootPage;
                        newNodeHeader->left = rootPage;

                        memcpy(newPageData, (char*) newNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newPageData+sizeof(IX_NodeHeader), (char*) newKeyArray, attrLength*degree);
                        memcpy(newPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newNodeHeader;
                        delete[] newKeyArray;
                        delete[] newValueArray;

                        memcpy(pageData, (char*) nodeHeader, sizeof(IX_NodeHeader));
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                        memcpy(valueData, (char*) valueArray, sizeof(IX_NodeValue)*(degree+1));

                        indexHeader.rootPage = newRootPage;
                        headerModified = TRUE;
                        if ((rc = pfFH.UnpinPage(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.UnpinPage(newPageNumber))) {
                            return rc;
                        }
                    }

                    else {
                        char* keyArray = (char*) keyData;
                        char* givenKeyChar = static_cast<char*>(pData);
                        string givenKey(givenKeyChar);
                        IX_NodeHeader* newNodeHeader = new IX_NodeHeader;
                        char* newKeyArray = new char[attrLength*degree];
                        for (int i=0; i<attrLength*degree; i++) {
                            newKeyArray[i] = ' ';
                        }
                        IX_NodeValue* newValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newValueArray[i] = dummyNodeValue;
                        }
                        for (int i=numberKeys/2; i<numberKeys; i++) {
                            for (int j=0; j <attrLength; j++) {
                                newKeyArray[(i-numberKeys/2)*attrLength + j] = keyArray[i*attrLength + j];
                            }
                            newValueArray[i-numberKeys/2] = valueArray[i];
                        }

                        nodeHeader->numberKeys = numberKeys/2;
                        nodeHeader->type = LEAF;
                        newNodeHeader->numberKeys = numberKeys - numberKeys/2;
                        newNodeHeader->keyCapacity = keyCapacity;
                        newNodeHeader->type = LEAF;

                        valueArray[keyCapacity].state = PAGE_ONLY;
                        valueArray[keyCapacity].page = newPageNumber;
                        valueArray[keyCapacity].rid = dummyRID;

                        string firstKey(newKeyArray);
                        if (givenKey < firstKey) {
                            int position = numberKeys/2;
                            for (int i=0; i<numberKeys/2; i++) {
                                string currentKey(keyArray + i*attrLength);
                                if (givenKey < currentKey) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys/2; i>position; i--) {
                                for (int j=0; j<attrLength; j++) {
                                    keyArray[i*attrLength + j] = keyArray[(i-1)*attrLength + j];
                                }
                                valueArray[i] = valueArray[i-1];
                            }

                            strcpy(keyArray + position*attrLength, givenKey.c_str());
                            valueArray[position].state = RID_FILLED;
                            valueArray[position].rid = rid;
                            valueArray[position].page = IX_NO_PAGE;

                            nodeHeader->numberKeys++;
                        }
                        else {
                            int position = numberKeys - numberKeys/2;
                            for (int i=0; i< numberKeys - numberKeys/2; i++) {
                                string currentKey(newKeyArray + i*attrLength);
                                if (givenKey < currentKey) {
                                    position = i;
                                    break;
                                }
                            }
                            for (int i=numberKeys-numberKeys/2; i>position; i--) {
                                for (int j=0; j<attrLength; j++) {
                                    newKeyArray[i*attrLength + j] = newKeyArray[(i-1)*attrLength + j];
                                }
                                newValueArray[i] = newValueArray[i-1];
                            }

                            strcpy(newKeyArray + position*attrLength, givenKey.c_str());
                            newValueArray[position].state = RID_FILLED;
                            newValueArray[position].rid = rid;
                            newValueArray[position].page = IX_NO_PAGE;

                            newNodeHeader->numberKeys++;
                        }

                        PF_PageHandle newRootPFPH;
                        char* newRootPageData;
                        PageNum newRootPage;
                        if ((rc = pfFH.AllocatePage(newRootPFPH))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetData(newRootPageData))) {
                            return rc;
                        }
                        if ((rc = newRootPFPH.GetPageNum(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.MarkDirty(newRootPage))) {
                            return rc;
                        }

                        IX_NodeHeader* newRootNodeHeader = new IX_NodeHeader;
                        char* newRootKeyArray = new char[attrLength*degree];
                        for (int i=0; i<attrLength*degree; i++) {
                            newRootKeyArray[i] = ' ';
                        }
                        IX_NodeValue* newRootValueArray = new IX_NodeValue[degree+1];
                        for (int i=0; i<=degree; i++) {
                            newRootValueArray[i] = dummyNodeValue;
                        }
                        newRootNodeHeader->numberKeys = 1;
                        newRootNodeHeader->keyCapacity = keyCapacity;
                        newRootNodeHeader->type = ROOT;
                        newRootNodeHeader->parent = IX_NO_PAGE;
                        newRootNodeHeader->left = IX_NO_PAGE;

                        for (int i=0; i<attrLength; i++) {
                            newRootKeyArray[i] = newKeyArray[i];
                        }
                        newRootValueArray[0].state = PAGE_ONLY;
                        newRootValueArray[0].page = rootPage;
                        newRootValueArray[0].rid = dummyRID;
                        newRootValueArray[1].state = PAGE_ONLY;
                        newRootValueArray[1].page = newPageNumber;
                        newRootValueArray[1].rid = dummyRID;

                        memcpy(newRootPageData, (char*) newRootNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newRootPageData+sizeof(IX_NodeHeader), (char*) newRootKeyArray, attrLength*degree);
                        memcpy(newRootPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newRootValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newRootNodeHeader;
                        delete[] newRootKeyArray;
                        delete[] newRootValueArray;

                        nodeHeader->parent = newRootPage;
                        newNodeHeader->parent = newRootPage;
                        newNodeHeader->left = rootPage;

                        memcpy(newPageData, (char*) newNodeHeader, sizeof(IX_NodeHeader));
                        memcpy(newPageData+sizeof(IX_NodeHeader), (char*) newKeyArray, attrLength*degree);
                        memcpy(newPageData+sizeof(IX_NodeHeader)+attrLength*degree, (char*) newValueArray, sizeof(IX_NodeValue)*(degree+1));
                        delete newNodeHeader;
                        delete[] newKeyArray;
                        delete[] newValueArray;

                        memcpy(pageData, (char*) nodeHeader, sizeof(IX_NodeHeader));
                        memcpy(keyData, (char*) keyArray, attrLength*degree);
                        memcpy(valueData, (char*) valueArray, sizeof(IX_NodeValue)*(degree+1));

                        indexHeader.rootPage = newRootPage;
                        headerModified = TRUE;
                        if ((rc = pfFH.UnpinPage(newRootPage))) {
                            return rc;
                        }
                        if ((rc = pfFH.UnpinPage(newPageNumber))) {
                            return rc;
                        }
                    }
                }
            }
        }

        else {
            if ((rc = InsertEntryRecursive(pData, rid, rootPage))) {
                return rc;
            }
        }

        if ((rc = pfFH.UnpinPage(rootPage))) {
            return rc;
        }

        return OK_RC;
    }
}
