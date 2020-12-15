#include<string>
#include<cstring>
#include"rm_internal.h"
#include"rm_rid.h"
#include"rm.h"
using namespace std;

RM_Record::RM_Record(){
    this->isValid = FALSE;
}

RM_Record::~RM_Record()
{
    if(isValid){
        isValid = FALSE;
        delete [] rData;
    }
}

RC RM_Record::GetData(char *&pData) const{
    if(!isValid){
        return RM_RECORD_NOT_VALID;
    }

    pData = this->rData;
    return OK_RC;
}

RC RM_Record::GetRid(RID &rid) const{
    if(!isValid){
        return RM_RECORD_NOT_VALID;
    }

    rid = this->rid;
    return OK_RC;
}

RM_Record::RM_Record(const RM_Record &rec){

    memcpy(this->rData,rec.rData,rec.recordSize);

    this->rid = rec.rid;
    this->isValid = rec.isValid;
    this->recordSize = rec.recordSize;
}

RM_Record& RM_Record::operator=(const RM_Record& rec){
    if(this !=  &rec){
        memcpy(this->rData,rec.rData,rec.recordSize);

        this->rid = rec.rid;
        this->isValid = rec.isValid;
        this->recordSize = rec.recordSize;
    }

    return *(this);
}

