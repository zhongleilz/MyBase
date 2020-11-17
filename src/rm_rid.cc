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

