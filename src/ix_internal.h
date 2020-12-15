#ifndef IX_INTERNAL_H
#define IX_INTERNAL_H

#include <string>
#include "ix.h"

#define IX_NULL_POINTER -1
#define IX_NO_PAGE -1

 RID dummyRID(-1, -1);

enum IX_NodeType{
    ROOT,
    NODE,
    LEAF,
    ROOT_LEAF
};

enum IX_ValueType{
    EMPTY,
    PAGE_ONLY,
    RID_FILED
};

struct IX_NodeValue {
    IX_ValueType state;
    RID rid;
    PageNum page;

    IX_NodeValue() {
        this->state = EMPTY;
        this->rid = dummyRID;
        this->page = IX_NO_PAGE;
    }
};

IX_NodeValue dummyNodeValue;


struct IX_NodeHeader
{
    int numberKeys;
    int keyCapacity;
    IX_NodeType type;
    PageNum parent;
    PageNum left;
};

struct IX_BucketPageHeader{
    int numberRecords;
    int recordCapacity;
    PageNum parentNode;
};


#endif