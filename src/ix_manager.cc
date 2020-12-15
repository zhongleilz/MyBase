#include "ix.h"
#include "iostream"
#include <string>
#include<cstring>
#include<sstream>
#include<iostream>

using namespace std;

IX_Manager::IX_Manager(PF_Manager & pfm)
{
    this->pfManager = &pfm;
}

IX_Manager::~IX_Manager(){
    
}


RC IX_Manager::CreateIndex(const char *fileName, int indexNo, AttrType attrType, int attrLength)
{
    //檢查index和filename
    if(indexNo < 0)
        return IX_NEGATIVE_INDEX;
    
    if(fileName == NULL)
        return IX_NULL_FILENAME;
    
    RC rc;

    string filenameString = generateIndexFileName(fileName,indexNo);
    const char * filename = filenameString.c_str();

    //檢查屬性和屬性長度
    if(attrType != INT && attrType != FLOAT &&attrType != STRING)
        return IX_INVALID_ATTRIBUTE;
    
    if((attrType == INT ||attrType == FLOAT) && attrLength != 4)
        return IX_INCONSISTENT_ATTRIBUTE;
    
    if(attrType == STRING && (attrLength <= 0 ||attrLength > MAXSTRINGLEN))
        return IX_INCONSISTENT_ATTRIBUTE;
    
    if((rc = pfManager->CreateFile(filename))){
        return rc;
    }

    //創建index文件
    PF_FileHandle pfFH;
    if((rc = pfManager->OpenFile(filename, pfFH))){
        return rc;
    }

    //申請page
    PF_PageHandle pfPH;
    if((rc = pfFH.AllocatePage(pfPH)))
    {
        return rc;
    }

    char * pData;
    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    PageNum pNum;
    if((rc = pfPH.GetPageNum(pNum))){
        return rc;
    }

    if((rc = pfFH.MarkDirty(pNum)))
    {
        return rc;
    }

    IX_IndexHeader * indexHeader = new IX_IndexHeader();

    //設置index頭文件
    indexHeader->attrType = attrType;
    indexHeader->attrLength = attrLength;
    indexHeader->rootPage = IX_NO_PAGE;
    indexHeader->degree = findDegreeOfNode(attrLength);

    //復制index header 到header page
    char * filedata =  (char*)indexHeader;
    memcpy(pData, filedata,sizeof(IX_IndexHeader));

    delete indexHeader;

    //Unpin. flush Page
    if((rc = pfFH.UnpinPage(pNum))){
        return rc;
    }

    if((rc = pfFH.ForcePages(pNum)))
    {
        return rc;
    }

    if((rc = pfManager->CloseFile(pfFH))){
        return rc;
    }

    return OK_RC;
}

RC IX_Manager::DestroyIndex(const char *filename, int indexNo){
    if(indexNo < 0){
        return IX_NEGATIVE_INDEX;
    }

    if(filename == NULL){
        return IX_INVALIDNAME;
    }

    RC rc;

    //獲取文件名
    string newname = generateIndexFileName(filename, indexNo);
    const char * indexfilename = newname.c_str();

    if((rc = pfManager->DestroyFile(indexfilename))){
        return rc;
    }

    return OK_RC;
}



RC IX_Manager::OpenIndex(const char* filename, int indexNo, IX_IndexHandle& indexHandle){
    if(!indexHandle.isOpen){
        return IX_INDEX_CLOSED;
    }
    if(filename == NULL){
        return IX_INVALIDNAME;
    }

    if(indexNo < 0){
        return IX_NEGATIVE_INDEX;
    }

    string newname = generateIndexFileName(filename, indexNo);
    const char* indexFileName = newname.c_str();
    
    PF_FileHandle pfFH;

    RC rc;
    if((rc = pfManager->OpenFile(indexFileName,pfFH))){
        return rc;
    }

    //更新index handle
    indexHandle.pfFH = pfFH;
    indexHandle.headerModified = FALSE;

    indexHandle.lastDeletedEntry.keyValue = NULL;
    indexHandle.lastDeletedEntry.rid = dummyRID;

    PF_PageHandle pfPH;
    PageNum headPage;

    if((rc = pfFH.GetFirstPage(pfPH))){
        return rc;
    }

    if((rc = pfPH.GetPageNum(headPage))){
        return rc;
    }

    char * pData;
    if((rc  = pfPH.GetData(pData))){
        return rc;
    }

    if((rc = pfFH.MarkDirty(headPage))){
        return rc;
    }

    //復制該index所在page的頭部給indexHandle
    IX_IndexHeader* iH = (IX_IndexHeader*)pData;
    memcpy(&indexHandle.indexHeader,iH,sizeof(IX_IndexHeader));

    if((rc = pfFH.UnpinPage(headPage))){
        return rc;
    }

    indexHandle.isOpen = TRUE;
    return OK_RC;
}

//1. 檢查index handle是否打開
//2. 將index handle的信息更新到對應的index header page中
//3. 關閉index file
RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle){
    if(!indexHandle.isOpen){
        return IX_INDEX_CLOSED;
    }

    RC rc;

    PF_FileHandle pfFH = indexHandle.pfFH;
    PF_PageHandle pfPH;
    PageNum headpageNum;

    if(indexHandle.headerModified){
        if((rc = pfFH.GetFirstPage(pfPH))){
            return rc;
        }

        if((rc = pfPH.GetPageNum(headpageNum))){
            return rc;
        }

        char * pData;
        if((rc = pfPH.GetData(pData))){
            return rc;
        }

        char * newData = (char*)&indexHandle.indexHeader;
        memcpy(pData,newData,sizeof(IX_IndexHeader));

        if((rc = pfFH.UnpinPage(headpageNum))){
            return rc;
        }

        if((rc = pfFH.ForcePages(headpageNum)))
        {
            return rc;
        }
    }

    if((rc = pfManager->CloseFile(pfFH))){
        return rc;
    }
    //TO DO: 刪除IX_IndexHandle對應的Entry

    indexHandle.isOpen = FALSE;
    indexHandle.headerModified = FALSE;

    return OK_RC;
}

string IX_Manager::generateIndexFileName(const char* filename, int indexNo){
    stringstream convert;
    convert<<filename<<"."<<indexNo;
    return convert.str();
}


int IX_Manager::findDegreeOfNode(int attrLength){
    int headerSize = sizeof(IX_NodeHeader);
    int n = 1;
    while(true)
    {
        int size = headerSize +n*attrLength +(n-1)*sizeof(IX_NodeValue);
        if(size >PF_PAGE_SIZE) break;
        n++;
    }
    return (n--);
}