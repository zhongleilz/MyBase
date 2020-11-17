//
// Created by zhong on 2020/11/9.
//
#include <cstring>
#include <string>
#include "rm.h"
#include "rm_internal.h"

using namespace std;

RM_FileHandle::RM_FileHandle() {

    isOpen = FALSE;
    isHeaderModified = FALSE;

    fileHeader.firstFreePage = RM_NO_FREE_PAGE ;
    fileHeader.numPages = 0;
}

RM_FileHandle::~RM_FileHandle() {
    
}

RM_FileHandle::RM_FileHandle(const RM_FileHandle &fileHandle) {
    this->isOpen = fileHandle.isOpen;
    this->isHeaderModified = fileHandle.isHeaderModified;
    this->pfFH = fileHandle.pfFH;
    this->fileHeader = fileHandle.fileHeader;
}

RM_FileHandle& RM_FileHandle::operator=(const RM_FileHandle &fileHandle) {
    if(this != &fileHandle){
        this->isOpen = fileHandle.isOpen;
        this->isHeaderModified = fileHandle.isHeaderModified;
        this->pfFH = fileHandle.pfFH;
        this->fileHeader = fileHandle.fileHeader;
    }

    return *(this);
}

RC RM_FileHandle::GetPageAndSlot(const RID &rid, PageNum &pageNum, SlotNum &slotNum) const {
    RC rc;
    if((rc = rid.isValidRID()))
        return rc;

    if((rc = rid.GetPageNum(pageNum))){
        return rc;
    }

    if((rc = rid.GetSlotNum(slotNum))){
        return rc;
    }

    return OK_RC;
}

RC RM_FileHandle::GetRec(const RID &rid, RM_Record &rec) const {
    if(!isOpen){
        return RM_FILE_CLOSED;
    }

    if(rec.isValid ){
        rec.isValid = FALSE;
        delete [] rec.rData;
    }

    RC rc;
    PageNum pageNum;
    SlotNum slotNum;
    if((rc = GetPageAndSlot(rid,pageNum,slotNum))){
        return rc;
    }

    PF_PageHandle pfPH;
    if((rc = pfFH.GetThisPage(pageNum,pfPH))){
        return rc;
    }

    char *pData;
    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    int numRecords = fileHeader.numRecordsPerPage;
    if(slotNum < 1 || slotNum > numRecords){
        return RM_INVALID_SLOW_NUMBER;
    }

    int recordOffset = GetRecordOffset(slotNum);

    rec.isValid = TRUE;

    int recordSize = fileHeader.recordSize;
    char *data = pData + recordOffset;
    char *newData = new char[recordSize];
    memcpy(newData,data,recordSize);
    rec.rData = newData;

    rec.rid = rid;
    rec.recordSize = recordSize;

    if((rc = pfFH.UnpinPage(pageNum))){
        return rc;
    }

    return OK_RC;
}

RC RM_FileHandle::GetRecordOffset(int slowNum) const{
    // Page的头文件 + bitmp + slot占的空间
    int numRecords = fileHeader.numRecordsPerPage;
    int bitmapSize = numRecords / 8;

    if(bitmapSize % 8 != 0)
        bitmapSize++;

    int recordSize = fileHeader.recordSize;
    int recordOffset = sizeof(RM_PageHeader) + bitmapSize + (slowNum - 1)*recordSize;
    return recordOffset;
}

RC RM_FileHandle::InsertRec(const char *pData, RID &rid){
    if(pData == NULL){
        return RM_NULL_RECORD;
    }

    if(!isOpen){
        return RM_FILE_CLOSED;
    }

    RC rc;

    int numRecords = fileHeader.numRecordsPerPage;
    int bitmapSize = fileHeader.bitmapSize;
    PageNum freePage = fileHeader.firstFreePage;
    PF_PageHandle pfPH;

    if(freePage == RM_NO_FREE_PAGE){
        //申请新的page
        if((rc = pfFH.AllocatePage(pfPH))){
            return rc;
        }

        //获得新page的数据
        char* pHData;
        if((rc = pfPH.GetData(pHData))){
            return rc;
        }

        //初始化PageHeader和bitmap
        RM_PageHeader* pageHeader = new RM_PageHeader;
        pageHeader->nextPage = RM_NO_FREE_PAGE;

        char* bitmap = new char[bitmapSize];
        for(int i = 0; i < bitmapSize;i++){
            bitmap[i] = 0x00;
        }

        //将PageHeader和bitmap copy到phdata中
        memcpy(pHData, pageHeader,sizeof(RM_PageHeader));
        char* offest = pHData + sizeof(RM_PageHeader);
        memcpy(offest, bitmap,bitmapSize);

        //更改fileheader中的信息
        if((rc = pfPH.GetPageNum(freePage))){
            return rc;
        }

        fileHeader.numPages++;
        isHeaderModified = TRUE;
        fileHeader.firstFreePage = freePage;
        
        //回收
        delete pageHeader;
        delete [] bitmap;

        if((rc = pfFH.UnpinPage(freePage))){
            return rc;
        }
    }

    if((rc = pfFH.GetThisPage(freePage,pfPH))){
        return rc;
    }

    char* freePageData;
    if((rc = pfPH.GetData(freePageData))){
        return rc;
    }

    char* bitmap = freePageData + fileHeader.bitmapOffset;

    int slot;
    if((rc = GetFirstZeroBit(bitmap, bitmapSize, slot))){
        return rc
    }

    if((rc = pfFH.MarkDirty(freePage))){
        return rc;
    }

    int recordOffeset = getRecordOffset(slot);

    memcpy(freePageData + recordOffeset,pData,fileHeader.recordSize);

    if((rc = SetBit(slot,bitmap))){
        return rc;
    }

    if(isBitmapFull(bitmap, numRecords)){
        RM_PageHeader *pH = (RM_PageHeader*)freePageData;//?
        PageNum nextFreePageNum = pH->nextPage;

        fileHeader.firstFreePage = nextFreePageNum;
        isHeaderModified = TRUE;

        pH->nextPage = RM_NO_FREE_PAGE;
    }

    if((rc = pfFH.UnpinPage(freePage))){
        return rc;
    }

    RID newRid(freePage,slot);
    rid = newRid;

    return OK_RC;
}

RC GetFirstZeroBit(char *bitmap, int bitmapSize, int &location){
    for(int i = 0; i < bitmapSize;i++)
    {
        char currentByte = bitmap[i];

        for(int j = 0;j < 8;j++)
        {
            if((currentByte | (0x80 >> j)) == currentByte){
                location = i*8 + j +1;
                return OK_RC
            }
        }
    }

    return RM_INCONSISTENT_BITMAP;
}



bool RM_FileHandle::isBitmapFull(char * bitmap,int numRecords){
    int bitmapSize = numRecords / 8;
    if(numRecords  % 8 != 0){
        bitmapSize++;
    }

    for(int i = 0;i < bitmapSize;i++){
        char currentByte = bitmap[i];

        int end = 8;
        if(i == bitmapSize - 1)
        {
            int bitOffeset = numRecords - (i-1)*8;
            end = min(end, bitOffeset);
        }

        for(int j = 0;j < end;j++){
            //this byte is 0
            if((currentByte | (0x80 >> j)) != currentByte){
                return false;
            }
        }
    }

    return TRUE;
}

bool RM_FileHandle::isBitmapEmpty(char * bitmap,int numRecords){
    int bitmapSize = numRecords / 8;
    if(numRecords  % 8 != 0){
        bitmapSize++;
    }

    for(int i = 0;i < bitmapSize;i++){
        char currentByte = bitmap[i];

        int end = 8;
        if(i == bitmapSize - 1)
        {
            int bitOffeset = numRecords - (i-1)*8;
            end = min(end, bitOffeset);
        }

        for(int j = 0;j < end;j++){
            //this byte is 0
            if((currentByte | (0x80 >> j)) != currentByte){
                return false;
            }
        }
    }

    return TRUE;
}

//使用RID获得page号和slot号
//Mark page为dirty page
//set bitmap为0
//如果page满的话，将the first page 设置为该page
//unpain the page
//如果page空，删掉该bitmap

RC RM_FileHandle::DeleteRec  (const RID &rid){
    if(!isOpen){
        return RM_FILE_CLOSED;
    }

    RC rc;
    int pageNum,slotNum;
    if((rc = rid.GetPageNum(pageNum))){
        return rc;
    }

    if((rc = rid.GetSlotNum(pageNum))){
        return rc;
    }

    if(pageNum <= 0){
        return RM_INVALID_PAGE_NUMBER;
    }

    int numRecords = fileHeader.numRecordsPerPage;
    if(slotNum <= 0 || slotNum >= numRecords){
        return RM_INVALID_SLOW_NUMBER;
    }

    PF_PageHandle pfPH;
    char* pageData ;
    if((rc = pfFH.GetThisPage(pageNum,pfPH))){
        return rc;
    }

    if((rc = pfPH.GetData(pageData))){
        return rc;
    }

    if((rc = pfFH.MarkDirty(pageNum))){
        return rc;
    }

    //检查bitmap是否full
    int bitmapSize = numRecords / 8;
    if(numRecords % 8 != 0){
        bitmapSize++;
    }

    char *bitmap = pageData + fileHeader.bitmapOffset;
    bool pageFull  = isBitmapFull(bitmap,numRecords);

    if((rc = UnsetBit(slotNum,bitmap))){
        return rc;
    }

    //如果之前page满，则把该page加到free page列表中
    if(pageFull){
        //将原本的firstfreepage 加到本page后面
        PageNum firstFreePage = fileHeader.firstFreePage;
        
        RM_PageHeader* pH = (RM_PageHeader*) pageData;//?
        pH->nextPage = firstFreePage;

        //将firstfree page设置为本页
        fileHeader.firstFreePage = pageNum;
        isHeaderModified = TRUE;
    }

    if((rc = pfFH.UnpinPage(pageNum))){
        return rc;
    }

    return OK_RC;
} 

RC RM_FileHandle::UnsetBit(int slowNum,char* bitmap){
    int bitNum = slowNum--;
    int byteNum = slowNum / 8;
    char currentByte = bitmap[byteNum];

    int bitOffeset = bitNum - (byteNum*8);

    //this bit is 0
    if((currentByte | (0x80 >> bitOffeset) != currentByte)){
        return RM_INCONSISTENT_BITMAP;
    }

    currentByte &= ~(0x80 >> bitOffeset);
    bitmap[byteNum] = currentByte;

    return OK_RC;
}


//Force a page from the buffer to disk. Default force all pages.
RC RM_FileHandle::ForcePages(PageNum pageNum){
    int rc;

    if((rc = pfFH.ForcePages(pageNum))){
        return rc;
    }

    return OK_RC;
}


RC RM_FileHandle::UpdateRec  (const RM_Record &rec){
    if(!isOpen){
        return RM_FILE_CLOSED;
    }

    RC rc;
    int slotNum;
    int pageNum;

    RID rid;
    if((rc  = rec.GetRid(rid))){
        return rc;
    }

    if((rc = rid.GetPageNum(pageNum))){
        return rc;
    }

    if((rc = rid.GetSlotNum(slotNum))){
        return rc;
    }

    if(pageNum  <= 0){
        return RM_INVALID_PAGE_NUMBER;
    }

    int numRecord = fileHeader.numRecordsPerPage;
    if(slotNum <= 0 || slotNum > numRecord){
        return RM_INVALID_SLOW_NUMBER;
    }

    PF_PageHandle pfPH;
    if((rc = pfFH.GetThisPage(pageNum,pfPH))){
        return rc;
    }

    char* pData;
    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    if((rc = pfFH.MarkDirty(pageNum))){
        return rc;
    }

    int recordOffesrt = getRecordOffset(slotNum);
    char* recordData = recordOffesrt + pData;

    char *newData;
    if((rc = rec.GetData(newData))){
        return rc;
    }

    memcpy(recordData,newData,fileHeader.recordSize);

    if((rc = pfFH.UnpinPage(pageNum))){
        return rc;
    }

    return OK_RC;
}


RC RM_FileHandle::SetBit(int bitNumber, char * bitmap){
    int bitNum = bitNumber--;
    int byteNum = bitNumber / 8;
    char currentByte = bitmap[byteNum];

    int bitOffeset = bitNum - (byteNum*8);

    //this bit is 1
    if((currentByte | (0x80 >> bitOffeset) == currentByte)){
        return RM_INCONSISTENT_BITMAP;
    }

    currentByte |= (0x80 >> bitOffeset);
    bitmap[byteNum] = currentByte;

    return OK_RC;
}

