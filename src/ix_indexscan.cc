#include "ix_internal.h"
#include "ix.h"
#include <iostream>
#include <cstring>
using namespace std;

IX_IndexScan::IX_IndexScan(){
    scanOpen = FALSE;
}


IX_IndexScan::~IX_IndexScan(){
    
}
/*
Step 1: 檢查input是否合法
Step 2: 初始化class類別 -存儲attrType，attrLength， compOp，value， pinHint，degree
Step 3: 獲得第一個滿足條件的key
*/

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp,
                          void *value, ClientHint  pinHint) {
    if (!indexHandle.isOpen) {
        return IX_INDEX_CLOSED;
    }

    if (compOp != NO_OP && compOp != EQ_OP && compOp != LT_OP &&
        compOp != GT_OP && compOp != LE_OP && compOp != GE_OP) {
        return IX_INVALID_OPERATOR;
    }

    if (compOp != NO_OP && value == NULL) {
        return IX_INVALID_OPERATOR;
    }

    this->indexHandle = &indexHandle;
    this->attrType = (indexHandle.indexHeader).attrType;
    this->attrLength = (indexHandle.indexHeader).attrLength;
    this->compOp = compOp;
    this->value = value;
    this->pinHint = pinHint;
    this->degree = (indexHandle.indexHeader).degree;
    this->inBucket = FALSE;
    this->bucketPosition = 0;
    (this->lastScannedEntry).keyValue = NULL;
    (this->lastScannedEntry).rid = dummyRID;

    int rc;

    scanOpen = TRUE;

    PageNum rootPage = (indexHandle.indexHeader).rootPage;

    if (rootPage == IX_NO_PAGE) {
        this->pageNumber = IX_NO_PAGE;
        this->keyPosition = -1;
    }

    else {
        PageNum firstPage;
        int position;
        if ((rc = SearchEntry(rootPage, firstPage, position))) {
            return rc;
        }

        this->pageNumber = firstPage;
        this->keyPosition = position;
    }

    return OK_RC;
}

RC IX_IndexScan::CloseScan() {
    if (!scanOpen) {
        return IX_SCAN_CLOSED;
    }

    scanOpen = FALSE;

    return OK_RC;
}

bool IX_IndexScan::compareRIDs(const RID &rid1, const RID &rid2) {
    PageNum pageNum1 = -1, pageNum2 = -1;
    SlotNum slotNum1 = -1, slotNum2 = -1;

    rid1.GetPageNum(pageNum1);
    rid1.GetSlotNum(slotNum1);
    rid2.GetPageNum(pageNum2);
    rid2.GetSlotNum(slotNum2);

    return (pageNum1 == pageNum2 && slotNum1 == slotNum2);
}

bool IX_IndexScan::compareEntries(const IX_Entry &e1, const IX_Entry &e2) {
    bool keyMatch = false;
    if (e1.keyValue == NULL || e1.keyValue == NULL) {
        return false;
    }

    if (attrType == INT) {
        int key1 = *static_cast<int*> (e1.keyValue);
        int key2 = *static_cast<int*> (e2.keyValue);
        if (key1 == key2) keyMatch = true;
    }
    else if (attrType == FLOAT) {
        float key1 = *(float*) (e1.keyValue);
        float key2 = *(float*) (e2.keyValue);
        if (key1 == key2) keyMatch = true;
    }
    else {
        char* key1Char = static_cast<char*> (e1.keyValue);
        char* key2Char = static_cast<char*> (e2.keyValue);
        string key1(key1Char);
        string key2(key2Char);
        if (key1 == key2) keyMatch = true;
    }
    return keyMatch && compareRIDs(e1.rid, e2.rid);
}

/* 
    Step 1: 檢查當前Page是否存在
    Step 2: 檢查last scanned entry是否被刪除
    Step 3: 檢查rid是否位於bucket中
    Step 4: 檢查是否滿足條件
*/
RC IX_IndexScan::GetNextEntry(RID &rid) {
    if (!scanOpen) {
        return IX_SCAN_CLOSED;
    }

    if (pageNumber == IX_NO_PAGE) {
        // Free the last scanned entry array
        char* temp = static_cast<char*> (lastScannedEntry.keyValue);
        delete[] temp;

        return IX_EOF;
    }

    int rc;

    PF_FileHandle pfFH = indexHandle->pfFH;
    PF_PageHandle pfPH;
    char* pageData;
    if ((rc = pfFH.GetThisPage(pageNumber, pfPH))) {
        return rc;
    }
    if ((rc = pfPH.GetData(pageData))) {
        return rc;
    }

    // 如果last scanned entry 存在
    if (!compareRIDs(lastScannedEntry.rid, dummyRID)) {
        // 檢查是否被刪除
        if (!compareRIDs((indexHandle->lastDeletedEntry).rid, dummyRID) && compareEntries(lastScannedEntry, indexHandle->lastDeletedEntry)) {
            if ((rc = pfFH.UnpinPage(pageNumber))) {
                return rc;
            }
        }

        // 如果不是 first entry scanned, 更新variables
        else {
            if (inBucket) {
                IX_BucketPageHeader* bucketHeader = (IX_BucketPageHeader*) pageData;
                int numberRecords = bucketHeader->numberRecords;

                bucketPosition++;

                if ((rc = pfFH.UnpinPage(pageNumber))) {
                    return rc;
                }

                // bucket結尾
                if (bucketPosition == numberRecords) {
                    inBucket = FALSE;
                    bucketPosition = 0;

                    // 設置new page number和key position
                    pageNumber = bucketHeader->parentNode;
                    keyPosition++;


                    if ((rc = pfFH.GetThisPage(pageNumber, pfPH))) {
                        return rc;
                    }
                    if ((rc = pfPH.GetData(pageData))) {
                        return rc;
                    }

                    IX_NodeHeader* nodeHeader = (IX_NodeHeader*) pageData;
                    int numberKeys = nodeHeader->numberKeys;
                    char* valueData = pageData + sizeof(IX_NodeHeader) + attrLength*degree;
                    IX_NodeValue* valueArray = (IX_NodeValue*) valueData;

                    if ((rc = pfFH.UnpinPage(pageNumber))) {
                        return rc;
                    }

                    // node結尾
                    if (keyPosition == numberKeys) {
                        pageNumber = valueArray[degree].page;
                        keyPosition = 0;
                    }
                }
            }
            else {
                IX_NodeHeader* nodeHeader = (IX_NodeHeader*) pageData;
                int numberKeys = nodeHeader->numberKeys;
                char* keyData = pageData + sizeof(IX_NodeHeader);
                char* valueData = keyData + attrLength*degree;
                IX_NodeValue* valueArray = (IX_NodeValue*) valueData;

                if ((rc = pfFH.UnpinPage(pageNumber))) {
                    return rc;
                }

                
                if (valueArray[keyPosition].page == IX_NO_PAGE) {
                    keyPosition++;

                    // key位於node結尾
                    if (keyPosition == numberKeys) {
                        pageNumber = valueArray[degree].page;
                        keyPosition = 0;
                    }
                }
                else {
                    pageNumber = valueArray[keyPosition].page;
                    inBucket = TRUE;
                    bucketPosition = 0;
                }
            }
        }

        if (pageNumber == IX_NO_PAGE) {
            char* temp = static_cast<char*> (lastScannedEntry.keyValue);
            delete[] temp;

            return IX_EOF;
        }

        if ((rc = pfFH.GetThisPage(pageNumber, pfPH))) {
            return rc;
        }
        if ((rc = pfPH.GetData(pageData))) {
            return rc;
        }
    }
    //位於bucket
    if (inBucket) {
        RID* ridList = (RID*) (pageData + sizeof(IX_BucketPageHeader));
        rid = ridList[bucketPosition];

        if ((rc = pfFH.UnpinPage(pageNumber))) {
            return rc;
        }
    }

    // 不在bucket
    else {
        IX_NodeHeader* nodeHeader = (IX_NodeHeader*) pageData;
        int numberKeys = nodeHeader->numberKeys;
        char* keyData = pageData + sizeof(IX_NodeHeader);
        char* valueData = keyData + attrLength*degree;
        IX_NodeValue* valueArray = (IX_NodeValue*) valueData;

        if ((rc = pfFH.UnpinPage(pageNumber))) {
            return rc;
        }
        bool unpinned = true;

        // 檢查current key是否滿足條件
        if (keyPosition == numberKeys) {
            pageNumber = valueArray[degree].page;
            keyPosition = 0;

            if (pageNumber == IX_NO_PAGE) {
                char* temp = static_cast<char*> (lastScannedEntry.keyValue);
                delete[] temp;

                return IX_EOF;
            }
        }
        while (true) {
            if (attrType == INT) {
                int* keyArray = (int*) keyData;
                int givenValue = *static_cast<int*>(value);
                if(satisfiesCondition(keyArray[keyPosition], givenValue))
                    break;
            }
            else if (attrType == FLOAT) {
                float* keyArray = (float*) keyData;
                float givenValue = *static_cast<float*>(value);
                if (satisfiesCondition(keyArray[keyPosition], givenValue))
                    break;
            }
            else {
                char* keyArray = (char*) keyData;
                char* givenValueChar = static_cast<char*>(value);
                string givenValue(givenValueChar);
                string currentKey(keyArray + keyPosition*attrLength);
                if (satisfiesCondition(currentKey, givenValue))
                    break;
            }

            keyPosition++;

            //node結尾
            if (keyPosition == numberKeys) {
                PageNum previousPage = pageNumber;
                pageNumber = valueArray[degree].page;
                keyPosition = 0;

                if (!unpinned) {
                    if ((rc = pfFH.UnpinPage(previousPage))) {
                       return rc;
                    }
                }

                if (pageNumber == IX_NO_PAGE) {
                    char* temp = static_cast<char*> (lastScannedEntry.keyValue);
                    delete[] temp;

                    return IX_EOF;
                }
                else {
                    if ((rc = pfFH.GetThisPage(pageNumber, pfPH))) {
                        return rc;
                    }
                    if ((rc = pfPH.GetData(pageData))) {
                        return rc;
                    }
                    unpinned = false;

                    nodeHeader = (IX_NodeHeader*) pageData;
                    numberKeys = nodeHeader->numberKeys;
                    keyData = pageData + sizeof(IX_NodeHeader);
                    valueData = keyData + attrLength*degree;
                    valueArray = (IX_NodeValue*) valueData;
                }
            }
        }

        rid = valueArray[keyPosition].rid;
    }

    // 更新 last scanned entry
    if (lastScannedEntry.keyValue == NULL) {
        lastScannedEntry.keyValue = new char[attrLength];
    }
    memcpy(lastScannedEntry.keyValue, value, attrLength);
    lastScannedEntry.rid = rid;

    return OK_RC;
}

/*
    Step 1: 從Page中獲得data
    Step 2: 檢查node的類型
    Step 3: 如果node爲葉節點:沿着索引找到value，獲得相應位置
    Step 4: 如果node爲root/中間節點：遞歸找下一屆點
     
*/
RC IX_IndexScan::SearchEntry(PageNum node, PageNum &pageNumber, int &keyPosition) {
    int rc;

    if (node == IX_NO_PAGE) {
        pageNumber = IX_NO_PAGE;
        keyPosition = -1;
        return OK_RC;
    }

    PF_FileHandle pfFH = indexHandle->pfFH;
    PF_PageHandle pfPH;
    if ((rc = pfFH.GetThisPage(node, pfPH))) {
        return rc;
    }
    char* nodeData;
    if ((rc = pfPH.GetData(nodeData))) {
        return rc;
    }

    IX_NodeHeader* nodeHeader = (IX_NodeHeader*) nodeData;
    IX_NodeType nodeType = nodeHeader->type;
    int numberKeys = nodeHeader->numberKeys;
    char* keyData = nodeData + sizeof(IX_NodeHeader);
    char* valueData = keyData + attrLength*degree;
    IX_NodeValue* valueArray = (IX_NodeValue*) valueData;

    // 葉節點
    if (nodeType == LEAF || nodeType == ROOT_LEAF) {
        bool found = false;
        if (attrType == INT) {
            int* keyArray = (int*) keyData;
            int intValue = *static_cast<int*>(value);
            for (int i=0; i<numberKeys; i++) {
                if (satisfiesCondition(keyArray[i], intValue)) {
                    pageNumber = node;
                    keyPosition = i;
                    found = true;
                    break;
                }
            }
        }
        else if (attrType == FLOAT) {
            float* keyArray = (float*) keyData;
            float floatValue = *static_cast<float*>(value);
            for (int i=0; i<numberKeys; i++) {
                if (satisfiesCondition(keyArray[i], floatValue)) {
                    pageNumber = node;
                    keyPosition = i;
                    found = true;
                    break;
                }
            }
        }
        else {
            char* keyArray = (char*) keyData;
            char* charValue = static_cast<char*>(value);
            string stringValue(charValue);
            for (int i=0; i<numberKeys; i++) {
                string currentKey(keyArray + i*attrLength);
                if (satisfiesCondition(currentKey, stringValue)) {
                    pageNumber = node;
                    keyPosition = i;
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            pageNumber = IX_NO_PAGE;
            keyPosition = -1;
        }

        if ((rc = pfFH.UnpinPage(node))) {
            return rc;
        }
    }

    // root/中間節點
    else if (nodeType == NODE || nodeType == ROOT) {
        PageNum nextPage = IX_NO_PAGE;
        if (compOp == LT_OP || compOp == LE_OP) {
            nextPage = valueArray[0].page;
        }
        else {
            if (attrType == INT) {
                int* keyArray = (int*) keyData;
                int intValue = *static_cast<int*>(value);
                if (intValue < keyArray[0]) {
                    nextPage = valueArray[0].page;
                }
                else if (intValue >= keyArray[numberKeys-1]) {
                    nextPage = valueArray[numberKeys].page;
                }
                else {
                    bool found;
                    for (int i=1; i<numberKeys; i++) {
                        if (satisfiesInterval(keyArray[i-1], keyArray[i], intValue)) {
                            nextPage = valueArray[i].page;
                            found = true;
                            break;
                        }
                    }
                    if (!found) nextPage = valueArray[numberKeys].page;
                }
            }
            else if (attrType == FLOAT) {
                float* keyArray = (float*) keyData;
                float floatValue = *static_cast<float*>(value);
                if (floatValue < keyArray[0]) {
                    nextPage = valueArray[0].page;
                }
                else if (floatValue >= keyArray[numberKeys-1]) {
                    nextPage = valueArray[numberKeys].page;
                }
                else {
                    bool found;
                    for (int i=1; i<numberKeys; i++) {
                        if (satisfiesInterval(keyArray[i-1], keyArray[i], floatValue)) {
                            nextPage = valueArray[i].page;
                            found = true;
                            break;
                        }
                    }
                    if (!found) nextPage = valueArray[numberKeys].page;
                }
            }
            else {
                char* keyArray = (char*) keyData;
                char* charValue = static_cast<char*>(value);
                string stringValue(charValue);
                string firstKey(keyArray);
                string lastKey(keyArray + (numberKeys-1)*attrLength);
                if (stringValue < firstKey) {
                    nextPage = valueArray[0].page;
                }
                else if (stringValue >= lastKey) {
                    nextPage = valueArray[numberKeys].page;
                }
                else {
                    bool found;
                    for (int i=1; i<numberKeys; i++) {
                        string currentKey(keyArray + i*attrLength);
                        string previousKey(keyArray + (i-1)*attrLength);
                        if (satisfiesInterval(previousKey, currentKey, stringValue)) {
                            nextPage = valueArray[i].page;
                            found = true;
                            break;
                        }
                    }
                    if (!found) nextPage = valueArray[numberKeys].page;
                }
            }
        }

        if ((rc = pfFH.UnpinPage(node))) {
            return rc;
        }

        if ((rc = SearchEntry(nextPage, pageNumber, keyPosition))) {
            return rc;
        }
    }

    // Return OK
    return OK_RC;
}

template<typename T>
bool IX_IndexScan::satisfiesCondition(T key, T value) {
    bool match = false;
    switch(compOp) {
        case EQ_OP:
            if (key == value) match = true;
            break;
        case LT_OP:
            if (key < value) match = true;
            break;
        case GT_OP:
            if (key > value) match = true;
            break;
        case LE_OP:
            if (key <= value) match = true;
            break;
        case GE_OP:
            if (key >= value) match = true;
            break;
        default:
            break;
    }
    return match;
}

template<typename T>
bool IX_IndexScan::satisfiesInterval(T key1, T key2, T value) {
    return (value >= key1 && value < key2);
}

bool IX_IndexScan::compareRIDs(const RID &rid1, const RID &rid2) {
    PageNum pageNum1 = -1, pageNum2 = -1;
    SlotNum slotNum1 = -1, slotNum2 = -1;

    rid1.GetPageNum(pageNum1);
    rid1.GetSlotNum(slotNum1);
    rid2.GetPageNum(pageNum2);
    rid2.GetSlotNum(slotNum2);

    return (pageNum1 == pageNum2 && slotNum1 == slotNum2);
}

bool IX_IndexScan::compareEntries(const IX_Entry &e1, const IX_Entry &e2) {
    bool keyMatch = false;
    if (e1.keyValue == NULL || e1.keyValue == NULL) {
        return false;
    }

    if (attrType == INT) {
        int key1 = *static_cast<int*> (e1.keyValue);
        int key2 = *static_cast<int*> (e2.keyValue);
        if (key1 == key2) keyMatch = true;
    }
    else if (attrType == FLOAT) {
        float key1 = *(float*) (e1.keyValue);
        float key2 = *(float*) (e2.keyValue);
        if (key1 == key2) keyMatch = true;
    }
    else {
        char* key1Char = static_cast<char*> (e1.keyValue);
        char* key2Char = static_cast<char*> (e2.keyValue);
        string key1(key1Char);
        string key2(key2Char);
        if (key1 == key2) keyMatch = true;
    }
    return keyMatch && compareRIDs(e1.rid, e2.rid);
}