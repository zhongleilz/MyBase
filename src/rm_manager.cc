//
// Created by zhong on 2020/11/4.
//

#include <cstring>
#include "rm.h"
using namespace std;

//Constructor
RM_Manager::RM_Manager(PF_Manager &pfm) {
    //
    this->pfManager = pfm;
}

RM_Manager::~RM_Manager() {

}

//This function creates the file headers associated with this file.
RC RM_Manager::CreateFile(const char *fileName, int recordSize) {

    if(recordSize <= 0)
        return RM_SMALL_RECORD;

    if(recordSize > PF_PAGE_SIZE)
        return RM_LARAGE_RECOED;

    if(fileName == NULL)
        return RM_INVALID_FILENAME;

    RC rc;
    if((rc = pfManager->CreateFile(fileName))){
        return rc;
    }

    PF_FileHandle pfFH;
    if((rc = pfManager->OpenFile(fileName,pfFH))){
        return rc;
    }

    PF_PageHandle pfPH;

    if((rc = pfFH.AllocatePage(pfPH))){
        return rc;
    }

    char* pData;
    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    PageNum pNum;
    if((rc = pfPH.GetPageNum(pNum))){
        return rc;
    }

    if((rc = pfFH.MarkDirty(pNum))){
        return rc;
    }

    RM_FileHeaderPage* fileHeaer = new RM_FileHeaderPage;

    fileHeaer->recordSize = recordSize;
    fileHeaer->numberPages = 0;
    fileHeaer->firstFreePage = -1;
    fileHeaer->numberRecordsOnPage = findNumberRecords(recordSize);

    char *fileData = (char*) fileHeaer;
    memcpy(pData,fileData, sizeof(RM_FileHeaderPage))

    delete fileData;

    //Always unpion the page, and close the file before existing.
    if((rc = pfFH.UnpinPage(pNum))){
        return rc;
    }

    if((rc = pfFH.FlushPages(pNum))){
        return rc;
    }

    if((rc = pfManager->CloseFile(pfFH))){
        return rc;
    }

    return OK_RC;
}

//Destroy the file with the given filename.
RC RM_Manager::DestroyFile(const char *fileName) {
    RC rc;

    if(fileName == NULL)
        return RM_INVALID_FILENAME;

    if((rc = pfManager->DestroyFile(fileName))){
        return rc;
    }

    return OK_RC;
}



RC RM_Manager::OpenFile(const char *fileName, RM_FileHandle &fileHandle) {

    if(fileName == NULL)
        return RM_INVALID_FILENAME;

    if(fileHandle.isOpen){
        return RM_FILE_OPEN;
    }

    RC rc;

    PF_FileHandle pfFH;

    if((rc = pfManager->OpenFile(fileName,pfFH))){
        return rc;
    }

    fileHandle.pfFH = pfFH;

    fileHandle.isOpen = TRUE;

    fileHandle.isHeaderModified = FALSE;

    PF_PageHandle pfPH;
    if((rc = pfFH.GetFirstPage(pfPH))){
        return rc;
    }

    char* pData;
    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    RM_FileHeaderPage* fH = (RM_FileHeaderPage*)pData;
    memcpy(&fileHandle.fileHeader,&fH, sizeof(RM_FileHeaderPage));

    PageNum headerPageNum;
    if((rc = pfPH.GetPageNum(headerPageNum))){
        return rc;
    }

    if((rc = pfFH.UnpinPage(headerPageNum))){
        return rc;
    }

    return OK_RC;
}

RC RM_Manager::CloseFile(RM_FileHandle &fileHandle) {
    if(!fileHandle.isOpen){
        return RM_FILE_CLOSED;
    }

    PageNum pNum;
    PF_PageHandle pfPH;
    PF_FileHandle pfFH = fileHandle.pfFH;

    if(fileHandle.isHeaderModified){
        if((rc = pfFH.GetFirstPage(pfPH))){
            return rc;
        }

        if((rc = pfPH.GetPageNum(pNum))){
            return rc;
        }

        char* pData;
        if((rc = pfPH.GetData(pData))){
            return rc;
        }

        char *temp = (char*) &fileHandle.fileHeader;
        memcpy(pData,temp, sizeof(RM_FileHeaderPage));

        if((rc = pfFH.ForcePages(pNum))){
            return rc;
        }

        if((rc = pfFH.UnpinPage(pNum))){
            return rc;
        }
    }

    if((rc = pfManager->CloseFile(pfFH))){
        return rc;
    }

    fileHandle.isHeaderModified = FALSE;
    fileHandle.isOpen = FALSE;

    return OK_RC;
}

int RM_Manager::findNumberRecords(int recordSize) {
    int headerSize = sizeof(PageNum);
    int n = 1;
    while(true){
        int bitmapSize = n / 8;
        if(n%8 != 0) bitmapSize++;
        int size = headerSize + bitmapSize + n*recordSize;
        if(size ? PF_PAGE_SIZE) break;
        n++;
    }

    return (n-1);

}