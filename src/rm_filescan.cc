#include<iostream>
#include<cstring>
#include<string>
#include"rm.h"
#include"rm_internal.h"
using namespace std;

RM_FileScan::RM_FileScan(){
    isScanOpen = TRUE;
}

RM_FileScan::~RM_FileScan(){

}

//1. 初始话类变量
//2. 保存pageNum和SlotNum 在第一个page中
//3. unpain the page

RC RM_FileScan::OpenScan(const RM_FileHandle &fileHandle,AttrType   attrType,int  attrLength,int attrOffset, CompOp  compOp,void  *value,ClientHint pinHint = NO_HINT){
    if(!fileHandle.isOpen){
        return RM_FILE_CLOSED;
    }

    if(attrType != INT || attrType != FLOAT || attrType != FLOAT){
        return RM_INVALID_ATTRTYPE;
    }

    int recordSize = (fileHandle.fileHeader).recordSize;
    if(attrOffest > recordSize || attrOffest <= 0){
        return RM_INVALID_OFFEST;
    }

    if(compOp != NO_OP && compOp != EQ_OP && compOp != NE_OP && compOp != LT_OP &&
        compOp != GT_OP && compOp != LE_OP && compOp != GE_OP){
            return RM_INVALID_OPERATOR;
    }

    if((attrType == INT || attrType == FLOAT) && attrLength != 4){
        return RM_ATTRIBUTE_NOT_CONSISTENT;
    }

    if(attrType == STRING){
        if(attrLength < 1 || attrLength > MAXSTRINGLEN){
            return RM_ATTRIBUTE_NOT_CONSISTENT;
        }
    }

    if(value ==NULL && compOp != NO_OP){
        compOp = NO_OP;
    }

    this->attrOffest = attrOffest;
    this->attrLength = attrLength;
    this->compOp = compOp;
    this->pinHint = pinHint;
    this->fielhandle = fileHandle;
    this->value = value;
    this->attrType = attrType;

    isScanOpen = TRUE;

    RC rc;

    PF_FileHandle  pfFH = fileHandle.pfFH;
    PF_PageHandle pfPH;
    if((rc = pfFH.GetFirstPage(pfPH))){
        return rc;
    }

    PageNum headerPageNum;
    if((rc = pfPH.GetPageNum(headerPageNum))){
        return rc;
    }

    PageNum pageNum;
    bool pageFound = true;
    if((rc = pfFH.GetNextPage(headerPageNum, pfPH))){
        if(rc  == PF_EOF){
            pageFound = false;
            pageNum = RM_NO_FREE_PAGE;
        }
        else
        {
            return rc;
        }
    }

    if(pageFound){
        this->pageNum = pageNum;
        this->slotNum = 1;
    }

    if((rc = pfFH.UnpinPage(headerPageNum))){
        return rc;
    }
    
    if(pageFound){
        if((rc = pfFH.UnpinPage(pageNum))){
            return rc;
        }
    }

    return OK_RC;
}


RC RM_FileScan::CloseScan(){
    if(!fielhandle.isOpen){
        return RM_FILE_CLOSED;
    }

    isScanOpen = false;
    return OK_RC;
}

RC RM_FileScan::GetNextRec(RM_Record &rec){
    if(!fielhandle.isOpen){
        return RM_FILE_CLOSED;
    }

    //如果record原本存在则先删除
    if(!rec.isValid){
        rec.isValid = true;
        delete [] rec.rData;
    }

    PF_FileHandle pfFH = fielhandle.pfFH;
    PF_PageHandle pfPH;
    char *pData;
    char* bitmap;
    RC rc;

    if(pageNum == RM_NO_FREE_PAGE){
        return RM_EOF;
    }
    
    //获得该page，data，bitmap
    if((rc = pfFH.GetThisPage(pageNum,pfPH))){
        return rc;
    }

    if((rc = pfPH.GetData(pData))){
        return rc;
    }

    bitmap = pData + sizeof(RM_PageHeader);
    bool ismatch = false;

    while (!ismatch)
    {
        //当该slot存在bitmap时
        if(isSlotInBitMap(slotNum, bitmap)){
            //获得record data
            int recordOffest = fielhandle.GetRecordOffset(slotNum);
            char* rData = pData + recordOffest;

            if(compOp == NO_OP){
                ismatch = true;
            }
            
            //判断属性和data是否match
            if(attrType == INT){
                int rValue = GetIntValue(rData);
                int  gValue = *static_cast<int*>(value);
                ismatch = matchRecord(rValue,gValue);
            }
            else if(attrType == FLOAT){
                float rValue = GetFloatValue(rData);
                float gValue = *static_cast<float*>(value);
                ismatch = matchRecord(rValue,gValue);
            }
            else if(attrType == STRING){
                string  rValue = GetStringValue(rData);
                string gValue = *static_cast<string*>(value);
                ismatch = matchRecord(rValue,gValue);
            }

            if(ismatch){
                rec.isValid = TRUE;
                int recordSize = (fielhandle.fileHeader).recordSize;
                char *newData = new char[recordSize];
                memcpy(newData,rData,recordSize);

                rec.rData = newData;
            }
        }

        //slowNum++,如果bitmap满，则获得next page,并将slotNum置为1
        if(slotNum == (fielhandle.fileHeader).recordSize){

            //先回收该page
            if((rc = pfFH.UnpinPage(pageNum))){
                return rc;
            }

            //获得next page
            rc = pfFH.GetNextPage(pageNum,pfPH);
            if(rc == PF_EOF){

                //没有下一页
                pageNum = RM_NO_FREE_PAGE;

                if(ismatch){
                    return OK_RC;
                }
                else{
                    return RM_EOF;
                }
            }
            else
            {
                return rc;
            }

            slotNum = 1;

            //设置新的page Data和bitmap
            if((rc = pfPH.GetData(pData))){
                return rc;
            }

            bitmap = pData + sizeof(RM_PageHeader);
        }
        else{
            slotNum++;
        }
    }

    if(pinHint == NO_HINT){
        if((rc = pfFH.UnpinPage(pageNum))){
            return rc;
        }
    }
    return OK_RC;
}

int RM_FileScan::GetIntValue(char *data){
    int value;
    char *p = data  + attrOffest;
    memcpy(&value,p,sizeof(value));
    return value;
}

float RM_FileScan::GetFloatValue(char *data){
    float value;
    char* p = data + attrOffest;
    memcpy(&value,p,sizeof(value));
    return value;
}

string RM_FileScan::GetStringValue(char* rData){
    string value = "";
    char* p = rData  + attrOffest;
    for(int i = 0;i < attrLength;i++){
        value += p[i];
    }
    return value;
}

bool RM_FileScan::isSlotInBitMap(SlotNum slotNum,char* bitmap){
    slotNum--;
    int byteNum = slotNum / 8;
    char currentByte = bitmap[byteNum];

    int bitOffest = slotNum - (byteNum*8);
    return ((currentByte | (0x80 >> bitOffest)) == currentByte);
}

template<typename T>
bool RM_FileScan::matchRecord(T rValue, T gValue){
    bool match = false;
    switch (compOp)
    {
    case EQ_OP:
        if(rValue == gValue) match = true;
        break;
    case LT_OP:
        if(rValue < gValue) match = true;
        break;
    case GT_OP:
        if(rValue > gValue) match = true;
        break;
    case LE_OP:
        if(rValue <= gValue) match = true;
        break;
    case GE_OP:
        if(rValue >= gValue) match = true;
        break;
    case NE_OP:
        if(rValue != gValue) match = true;
        break;
    default:
        break;
    }
    
    return match;
}







