#include<string>
#include<cstring>
#include"rm.h"
#include"rm_rid.h"

using namespace std;

RID::RID(){
    isValid = FALSE;
}

RID::~RID(){

}

RID::RID(PageNum pageNum,SlotNum slotNum){
    this->pageNum = pageNum;
    this->slotNum = slotNum;
    this->isValid = TRUE;
}

RID& RID::operator=(RID &rid){
    if(this != &rid){
        this->pageNum = pageNum;
        this->slotNum = slotNum;
        this->isValid = TRUE;

    }

    return (*this);
}

RC RID::GetPageNum(PageNum &pageNum) const{
    if(isValid){
        pageNum = this->pageNum;
    }
    else{
        return RID_NOT_VIABLE;
    }

    return OK_RC;
}

RC RID::GetSlotNum(SlotNum &slotNum) const{
    if(isValid){
        slotNum = this->slotNum;
    }
    else{
        return RID_NOT_VIABLE;
    }

    return OK_RC;
}