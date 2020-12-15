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
                }
                else{
                    
                }

            }





        }





    }




}
