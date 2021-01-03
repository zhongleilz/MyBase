#include <cstdio>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include "redbase.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"
#include "ex.h"
#include "printer.h"
#include "parser.h"
using namespace std;

SM_Manager::SM_Manager(IX_Manager & ixm, RM_Manager &rmm){
    this->rmManager = &rmm;
    this->ixManager = &ixm;

    isOpen = FALSE;

    printCommands = FALSE;
    optimizeQuery = TRUE;
    partitionedPrint = FALSE;
}

SM_Manager::~SM_Manager() {
}
/*
Step 1. 檢查database是否打開
Step 2.  改變database的目錄
Step3. 打開系統的catalogs
Step 4. 更新flag
*/
RC SM_Manager::OpenDb(const char *dbName) {
    if (isOpen) {
        return SM_DATABASE_OPEN;
    }

    if (dbName == NULL) {
        return SM_INVALID_DATABASE_NAME;
    }

    if (chdir(dbName) == -1) {
        return SM_INVALID_DATABASE_NAME;
    }

    int rc;
    RM_FileHandle dbInfoFH;
    RM_FileScan dbInfoFS;
    RM_Record rec;
    char* recordData;

    if ((rc = rmManager->OpenFile("dbinfo", dbInfoFH))) {
        return rc;
    }
    if ((rc = dbInfoFS.OpenScan(dbInfoFH, INT, 4, 0, NO_OP, NULL))) {
        return rc;
    }
    if ((rc = dbInfoFS.GetNextRec(rec))) {
        return rc;
    }
    if ((rc = rec.GetData(recordData))) {
        return rc;
    }
    EX_DBInfo* dbInfo = (EX_DBInfo*) recordData;
    this->distributed = dbInfo->distributed;
    this->numberNodes = dbInfo->numberNodes;

    if ((rc = dbInfoFS.CloseScan())) {
        return rc;
    }
    if ((rc = rmManager->CloseFile(dbInfoFH))) {
        return rc;
    }

    if ((rc = rmManager->OpenFile("relcat", relcatFH))) {
        return rc;
    }
    if ((rc = rmManager->OpenFile("attrcat", attrcatFH))) {
        return rc;
    }

    isOpen = TRUE;

    return OK_RC;
}

RC SM_Manager::CloseDb() {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    int rc;
    if ((rc = rmManager->CloseFile(relcatFH))) {
        return rc;
    }
    if ((rc = rmManager->CloseFile(attrcatFH))) {
        return rc;
    }

    if (chdir("../") == -1) {
        return SM_INVALID_DATABASE_CLOSE;
    }

    isOpen = FALSE;

    return OK_RC;
}


/*
Step 1. 檢查database是否打開
Step 2. 檢查table是否存在
Step 3. 更新system catalogs
Step 4. 爲relation創建RM files
Step 5. Flush system catalogs
*/
RC SM_Manager::CreateTable(const char *relName, int attrCount, AttrInfo *attributes,
                           // EX
                           int isDistributed, const char* partitionAttrName, int nValues, const Value values[]) {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (relName == NULL) {
        return SM_INVALID_NAME;
    }
    if (attrCount < 1 || attrCount > MAXATTRS) {
        return SM_INCORRECT_ATTRIBUTE_COUNT;
    }
    if (attributes == NULL) {
        return SM_NULL_ATTRIBUTES;
    }

    int distributedRelation = isDistributed;
    AttrType partitionAttrType = (AttrType) 0;
    if (distributedRelation) {
        // Check that the attribute is not null
        if (partitionAttrName == NULL) {
            return EX_INVALID_ATTRIBUTE;
        }

        if (nValues != numberNodes-1) {
            return EX_INCORRECT_VALUE_COUNT;
        }

        // Check the attribute name
        bool found = false;
        for (int i=0; i<attrCount; i++) {
            if (strcmp(attributes[i].attrName, partitionAttrName) == 0) {
                found = true;
                partitionAttrType = attributes[i].attrType;
                break;
            }
        }
        if (!found) {
            return EX_INVALID_ATTRIBUTE;
        }

        // 檢查value的屬性 
        for (int i=0; i<nValues; i++) {
            if (values[i].type != partitionAttrType) {
                return EX_INVALID_VALUE;
            }
        }
    }

    // 檢查 table是否存在 
    int rc;
    RM_FileScan relcatFS;
    RM_Record rec;
    bool duplicate = false;
    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }
    if ((rc = relcatFS.GetNextRec(rec)) != RM_EOF) {
        duplicate = true;
    }
    if ((rc = relcatFS.CloseScan())) {
        return rc;
    }
    if (duplicate) {
        return SM_TABLE_ALREADY_EXISTS;
    }

    if (printCommands) {
        cout << "CreateTable\n"
             << "   relName     =" << relName << "\n"
             << "   attrCount   =" << attrCount << "\n";
        for (int i = 0; i < attrCount; i++) {
            cout << "   attributes[" << i << "].attrName=" << attributes[i].attrName
                 << "   attrType="
                 << (attributes[i].attrType == INT ? "INT" :
                     attributes[i].attrType == FLOAT ? "FLOAT" : "STRING")
                 << "   attrLength=" << attributes[i].attrLength << "\n";
        }
    }

    int tupleLength = 0;
    int offset[attrCount];
    for (int i=0; i<attrCount; i++) {
        offset[i] = tupleLength;
        tupleLength += attributes[i].attrLength;
    }

    // Update relcat
    RID rid;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    strcpy(rcRecord->relName, relName);
    rcRecord->tupleLength = tupleLength;
    rcRecord->attrCount = attrCount;
    rcRecord->indexCount = 0;
    rcRecord->distributed = distributedRelation;
    if (distributedRelation) {
        strcpy(rcRecord->attrName, partitionAttrName);
    }
    else {
        strcpy(rcRecord->attrName, "NA");
    }
    if ((rc = relcatFH.InsertRec((char*) rcRecord, rid))) {
        return rc;
    }
    delete rcRecord;

    // Update attrcat
    SM_AttrcatRecord* acRecord = new SM_AttrcatRecord;
    memset(acRecord, 0, sizeof(SM_AttrcatRecord));
    strcpy(acRecord->relName, relName);
    for (int i=0; i<attrCount; i++) {
        strcpy(acRecord->attrName, attributes[i].attrName);
        acRecord->offset = offset[i];
        acRecord->attrType = attributes[i].attrType;
        acRecord->attrLength = attributes[i].attrLength;
        acRecord->indexNo = -1;
        if ((rc = attrcatFH.InsertRec((char*) acRecord, rid))) {
            return rc;
        }
    }
    delete acRecord;

    if (!distributedRelation) {
        if ((rc = rmManager->CreateFile(relName, tupleLength))) {
            return rc;
        }
    }

    else {
        char partitionVectorFileName[255];
        strcpy(partitionVectorFileName, relName);
        strcat(partitionVectorFileName, "_partitions_");
        strcat(partitionVectorFileName, partitionAttrName);

        RM_FileHandle partitionVectorFH;
        if (partitionAttrType == INT) {
            if ((rc = rmManager->CreateFile(partitionVectorFileName, sizeof(EX_IntPartitionVectorRecord)))) {
                return rc;
            }
            if ((rc = rmManager->OpenFile(partitionVectorFileName, partitionVectorFH))) {
                return rc;
            }

            EX_IntPartitionVectorRecord* pV = new EX_IntPartitionVectorRecord;
            int previous = MIN_INT;
            for (int i=1; i<=numberNodes; i++) {
                pV->node = i;
                pV->startValue = previous;
                if (i != numberNodes) {
                    int current = *static_cast<int*>(values[i-1].data);
                    pV->endValue = current;
                    previous = current;
                }
                else {
                    pV->endValue = MAX_INT;
                }

                if ((rc = partitionVectorFH.InsertRec((char*) pV, rid))) {
                    return rc;
                }
            }

            if ((rc = rmManager->CloseFile(partitionVectorFH))) {
                return rc;
            }
            delete pV;
        }

        else if (partitionAttrType == FLOAT) {
            if ((rc = rmManager->CreateFile(partitionVectorFileName, sizeof(EX_FloatPartitionVectorRecord)))) {
                return rc;
            }
            if ((rc = rmManager->OpenFile(partitionVectorFileName, partitionVectorFH))) {
                return rc;
            }

            EX_FloatPartitionVectorRecord* pV = new EX_FloatPartitionVectorRecord;
            float previous = MIN_FLOAT;
            for (int i=1; i<=numberNodes; i++) {
                pV->node = i;
                pV->startValue = previous;
                if (i != numberNodes) {
                    float current = *static_cast<float*>(values[i-1].data);
                    pV->endValue = current;
                    previous = current;
                }
                else {
                    pV->endValue = MAX_FLOAT;
                }

                if ((rc = partitionVectorFH.InsertRec((char*) pV, rid))) {
                    return rc;
                }
            }

            if ((rc = rmManager->CloseFile(partitionVectorFH))) {
                return rc;
            }
            delete pV;
        }

        else {
            if ((rc = rmManager->CreateFile(partitionVectorFileName, sizeof(EX_StringPartitionVectorRecord)))) {
                return rc;
            }
            if ((rc = rmManager->OpenFile(partitionVectorFileName, partitionVectorFH))) {
                return rc;
            }

            EX_StringPartitionVectorRecord* pV = new EX_StringPartitionVectorRecord;
            char previous[MAXSTRINGLEN+1] = MIN_STRING;
            for (int i=1; i<=numberNodes; i++) {
                pV->node = i;
                strcpy(pV->startValue, previous);
                if (i != numberNodes) {
                    char* current = static_cast<char*>(values[i-1].data);
                    strcpy(pV->endValue, current);
                    strcpy(previous, current);
                }
                else {
                    strcpy(pV->endValue, MAX_STRING);
                }

                if ((rc = partitionVectorFH.InsertRec((char*) pV, rid))) {
                    return rc;
                }
            }

            if ((rc = rmManager->CloseFile(partitionVectorFH))) {
                return rc;
            }
            delete pV;
        }

        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.CreateTableInDataNode(relName, attrCount, attributes, i))) {
                return rc;
            }
        }
    }

    if ((rc = relcatFH.ForcePages())) {
        return rc;
    }
    if ((rc = attrcatFH.ForcePages())) {
        return rc;
    }

    return OK_RC;
}
/*
Step :
1. 檢查database是否打開
2. print command
3. 刪除 entry
4. 掃描整個attrcat
5. 刪除realation的RM file
6 flush system catalogs

*/
RC SM_Manager::DropTable(const char *relName) {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (relName == NULL) {
        return SM_NULL_RELATION;
    }

    if (strcmp(relName, "relcat") == 0 || strcmp(relName, "attrcat") == 0) {
        return SM_SYSTEM_CATALOG;
    }

    if (printCommands) {
        cout << "DropTable\n   relName=" << relName << "\n";
    }

    RM_FileScan relcatFS;
    RM_Record rec;
    int rc;

    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }
    if ((rc = relcatFS.GetNextRec(rec))) {
        if (rc == RM_EOF) {
            return SM_TABLE_DOES_NOT_EXIST;
        }
        return rc;
    }

    char* recordData;
    if ((rc = rec.GetData(recordData))) {
        return rc;
    }
    SM_RelcatRecord* rcRecord = (SM_RelcatRecord*) recordData;
    int distributedRelation = rcRecord->distributed;
    char partitionAttrName[MAXNAME+1];
    strcpy(partitionAttrName, rcRecord->attrName);

    RID rid;
    if ((rc = rec.GetRid(rid))) {
        return rc;
    }

    if ((rc = relcatFH.DeleteRec(rid))) {
        return rc;
    }
    if ((rc = relcatFS.CloseScan())) {
        return rc;
    }

    RM_FileScan attrcatFS;
    if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }
    while ((rc = attrcatFS.GetNextRec(rec)) != RM_EOF) {
        if (rc) {
            return rc;
        }

        if ((rc = rec.GetRid(rid))) {
            return rc;
        }

        if ((rc = rec.GetData(recordData))) {
            return rc;
        }
        SM_AttrcatRecord* acRecord = (SM_AttrcatRecord*) recordData;
        if (acRecord->indexNo != -1) {
            if ((rc = ixManager->DestroyIndex(relName, acRecord->indexNo))) {
                return rc;
            }
        }

        if ((rc = attrcatFH.DeleteRec(rid))) {
            return rc;
        }
    }

    if ((rc = relcatFH.ForcePages())) {
        return rc;
    }
    if ((rc = attrcatFH.ForcePages())) {
        return rc;
    }

    if (!distributedRelation) {
        if ((rc = rmManager->DestroyFile(relName))) {
            return rc;
        }
    }

    else {
        char partitionVectorFileName[255];
        strcpy(partitionVectorFileName, relName);
        strcat(partitionVectorFileName, "_partitions_");
        strcat(partitionVectorFileName, partitionAttrName);
        if ((rc = rmManager->DestroyFile(partitionVectorFileName))) {
            return rc;
        }

        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.DropTableInDataNode(relName, i))) {
                return rc;
            }
        }
    }

    return OK_RC;
}

/*
Step 
1. 檢查參數
2.  檢查數據庫是否打開
3.  檢查index 是否存在
4. 更新 system catalogs
5.  創建和打開index文件
6.  掃描全部tuples和插入index
7.  關閉index文件
*/
RC SM_Manager::CreateIndex(const char *relName, const char *attrName) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (attrName == NULL) {
        return SM_NULL_ATTRIBUTES;
    }

    if (printCommands) {
        cout << "CreateIndex\n"
             << "   relName =" << relName << "\n"
             << "   attrName=" << attrName << "\n";
    }

    int rc;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    int distributed = rcRecord->distributed;

    SM_AttrcatRecord* attrRecord = new SM_AttrcatRecord;
    memset(attrRecord, 0, sizeof(SM_AttrcatRecord));
    if ((rc = GetAttrInfo(relName, attrName, attrRecord))) {
        return rc;
    }
    if (attrRecord->indexNo != -1) {
        delete attrRecord;
        return SM_INDEX_EXISTS;
    }
    int offset = attrRecord->offset;
    AttrType attrType = attrRecord->attrType;
    int attrLength = attrRecord->attrLength;
    delete rcRecord;
    delete attrRecord;

    if (distributed) {
        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.CreateIndexInDataNode(relName, attrName, i))) {
                return rc;
            }
        }
    }

    else {
        RM_FileScan relcatFS;
        RM_Record rec;
        char relationName[MAXNAME+1];
        strcpy(relationName, relName);
        if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        if ((rc = relcatFS.GetNextRec(rec))) {
            if (rc == RM_EOF) {
                return SM_TABLE_DOES_NOT_EXIST;
            }
            else return rc;
        }
        char* recordData;
        if ((rc = rec.GetData(recordData))) {
            return rc;
        }
        SM_RelcatRecord* rcRecord = (SM_RelcatRecord*) recordData;
        rcRecord->indexCount++;
        if ((rc = relcatFH.UpdateRec(rec))) {
            return rc;
        }
        if ((rc = relcatFS.CloseScan())) {
            return rc;
        }

        RM_FileScan attrcatFS;
        if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        int position = 0;
        while (rc != RM_EOF) {
            rc = attrcatFS.GetNextRec(rec);
            if (rc != 0 && rc != RM_EOF) {
                return rc;
            }

            if (rc != RM_EOF) {
                if ((rc = rec.GetData(recordData))) {
                    return rc;
                }
                SM_AttrcatRecord* acRecord = (SM_AttrcatRecord*) recordData;

                if (strcmp(acRecord->attrName, attrName) == 0) {
                    acRecord->indexNo = position;
                    if ((rc = attrcatFH.UpdateRec(rec))) {
                        return rc;
                    }
                    break;
                }
            }
            position++;
        }
        if ((rc = attrcatFS.CloseScan())) {
            return rc;
        }

        if ((rc = relcatFH.ForcePages())) {
            return rc;
        }
        if ((rc = attrcatFH.ForcePages())) {
            return rc;
        }

        if ((rc = ixManager->CreateIndex(relName, position, attrType, attrLength))) {
            return rc;
        }
        IX_IndexHandle ixIH;
        if ((rc = ixManager->OpenIndex(relName, position, ixIH))) {
            return rc;
        }

        RM_FileHandle rmFH;
        RM_FileScan rmFS;
        RID rid;
        if ((rc = rmManager->OpenFile(relName, rmFH))) {
            return rc;
        }
        if ((rc = rmFS.OpenScan(rmFH, INT, 4, 0, NO_OP, NULL))) {
            return rc;
        }
        while (rc != RM_EOF) {
            rc = rmFS.GetNextRec(rec);
            if (rc != 0 && rc != RM_EOF) {
                return rc;
            }

            if (rc != RM_EOF) {
                if ((rc = rec.GetData(recordData))) {
                    return rc;
                }
                if ((rc = rec.GetRid(rid))) {
                    return rc;
                }

                if (attrType == INT) {
                    int value;
                    memcpy(&value, recordData+offset, sizeof(value));
                    if ((rc = ixIH.InsertEntry(&value, rid))) {
                        return rc;
                    }
                }
                else if (attrType == FLOAT) {
                    float value;
                    memcpy(&value, recordData+offset, sizeof(value));
                    if ((rc = ixIH.InsertEntry(&value, rid))) {
                        return rc;
                    }
                }
                else {
                    char* value = new char[attrLength];
                    strcpy(value, recordData+offset);
                    if ((rc = ixIH.InsertEntry(value, rid))) {
                        return rc;
                    }
                    delete[] value;
                }
            }
        }
        if ((rc = rmFS.CloseScan())) {
            return rc;
        }

        if ((rc = rmManager->CloseFile(rmFH))) {
            return rc;
        }
        if ((rc = ixManager->CloseIndex(ixIH))) {
            return rc;
        }
    }

    return OK_RC;
}

RC SM_Manager::DropIndex(const char *relName, const char *attrName) {
    // Check the parameters
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (attrName == NULL) {
        return SM_NULL_ATTRIBUTES;
    }

    // Print the drop index command
    if (printCommands) {
        cout << "DropIndex\n"
             << "   relName =" << relName << "\n"
             << "   attrName=" << attrName << "\n";
    }

    // Check whether the index exists
    int rc;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    int distributed = rcRecord->distributed;

    SM_AttrcatRecord* attrRecord = new SM_AttrcatRecord;
    memset(attrRecord, 0, sizeof(SM_AttrcatRecord));
    if ((rc = GetAttrInfo(relName, attrName, attrRecord))) {
        return rc;
    }
    if (attrRecord->indexNo == -1) {
        delete attrRecord;
        return SM_INDEX_DOES_NOT_EXIST;
    }
    delete rcRecord;
    delete attrRecord;

    // EX - Distributed relation case
    if (distributed) {
        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.DropIndexInDataNode(relName, attrName, i))) {
                return rc;
            }
        }
    }

    // Non distributed relation case
    else {
        // Update relcat
        RM_FileScan relcatFS;
        RM_Record rec;
        char relationName[MAXNAME+1];
        strcpy(relationName, relName);
        if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        if ((rc = relcatFS.GetNextRec(rec))) {
            if (rc == RM_EOF) {
                return SM_TABLE_DOES_NOT_EXIST;
            }
            else return rc;
        }
        char* recordData;
        if ((rc = rec.GetData(recordData))) {
            return rc;
        }
        SM_RelcatRecord* rcRecord = (SM_RelcatRecord*) recordData;
        rcRecord->indexCount--;
        if ((rc = relcatFH.UpdateRec(rec))) {
            return rc;
        }
        if ((rc = relcatFS.CloseScan())) {
            return rc;
        }

        // Update attrcat
        RM_FileScan attrcatFS;
        int position = -1;
        if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        while (rc != RM_EOF) {
            rc = attrcatFS.GetNextRec(rec);
            if (rc != 0 && rc != RM_EOF) {
                return rc;
            }

            if (rc != RM_EOF) {
                if ((rc = rec.GetData(recordData))) {
                    return rc;
                }
                SM_AttrcatRecord* acRecord = (SM_AttrcatRecord*) recordData;

                if (strcmp(acRecord->attrName, attrName) == 0) {
                    position = acRecord->indexNo;
                    acRecord->indexNo = -1;
                    if ((rc = attrcatFH.UpdateRec(rec))) {
                        return rc;
                    }
                    break;
                }
            }
        }
        if ((rc = attrcatFS.CloseScan())) {
            return rc;
        }

        if ((rc = relcatFH.ForcePages())) {
            return rc;
        }
        if ((rc = attrcatFH.ForcePages())) {
            return rc;
        }

        if ((rc = ixManager->DestroyIndex(relName, position))) {
            return rc;
        }
    }

    return OK_RC;
}

/*
1. 檢查參數
2.  檢查database是否打開
3.  檢查index是否存在
4.  更新system catalogs
5.  destroy index 文件
*/
RC SM_Manager::DropIndex(const char *relName, const char *attrName) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (attrName == NULL) {
        return SM_NULL_ATTRIBUTES;
    }

    if (printCommands) {
        cout << "DropIndex\n"
             << "   relName =" << relName << "\n"
             << "   attrName=" << attrName << "\n";
    }

    int rc;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    int distributed = rcRecord->distributed;

    SM_AttrcatRecord* attrRecord = new SM_AttrcatRecord;
    memset(attrRecord, 0, sizeof(SM_AttrcatRecord));
    if ((rc = GetAttrInfo(relName, attrName, attrRecord))) {
        return rc;
    }
    if (attrRecord->indexNo == -1) {
        delete attrRecord;
        return SM_INDEX_DOES_NOT_EXIST;
    }
    delete rcRecord;
    delete attrRecord;

    if (distributed) {
        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.DropIndexInDataNode(relName, attrName, i))) {
                return rc;
            }
        }
    }

    else {
        RM_FileScan relcatFS;
        RM_Record rec;
        char relationName[MAXNAME+1];
        strcpy(relationName, relName);
        if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        if ((rc = relcatFS.GetNextRec(rec))) {
            if (rc == RM_EOF) {
                return SM_TABLE_DOES_NOT_EXIST;
            }
            else return rc;
        }
        char* recordData;
        if ((rc = rec.GetData(recordData))) {
            return rc;
        }
        SM_RelcatRecord* rcRecord = (SM_RelcatRecord*) recordData;
        rcRecord->indexCount--;
        if ((rc = relcatFH.UpdateRec(rec))) {
            return rc;
        }
        if ((rc = relcatFS.CloseScan())) {
            return rc;
        }

        RM_FileScan attrcatFS;
        int position = -1;
        if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
            return rc;
        }
        while (rc != RM_EOF) {
            rc = attrcatFS.GetNextRec(rec);
            if (rc != 0 && rc != RM_EOF) {
                return rc;
            }

            if (rc != RM_EOF) {
                if ((rc = rec.GetData(recordData))) {
                    return rc;
                }
                SM_AttrcatRecord* acRecord = (SM_AttrcatRecord*) recordData;

                if (strcmp(acRecord->attrName, attrName) == 0) {
                    position = acRecord->indexNo;
                    acRecord->indexNo = -1;
                    if ((rc = attrcatFH.UpdateRec(rec))) {
                        return rc;
                    }
                    break;
                }
            }
        }
        if ((rc = attrcatFS.CloseScan())) {
            return rc;
        }

        if ((rc = relcatFH.ForcePages())) {
            return rc;
        }
        if ((rc = attrcatFH.ForcePages())) {
            return rc;
        }

        if ((rc = ixManager->DestroyIndex(relName, position))) {
            return rc;
        }
    }

    return OK_RC;
}


/*
Load relName from fileName
Steps
1. 檢查parameters
2. 檢查database是否打開
3. 從relation中獲得屬性信息
4. 打開RM file和每個index file
5.  打開data file
6.  從file中讀取tuples
7.  Close files

*/
RC SM_Manager::Load(const char *relName, const char *fileName) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (fileName == NULL) {
        return SM_NULL_FILENAME;
    }

    if (strcmp(relName, "relcat") == 0 || strcmp(relName, "attrcat") == 0) {
        return SM_SYSTEM_CATALOG;
    }

    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (printCommands) {
        cout << "Load\n"
             << "   relName =" << relName << "\n"
             << "   fileName=" << fileName << "\n";
    }

    int rc;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    int tupleLength = rcRecord->tupleLength;
    int attrCount = rcRecord->attrCount;
    int indexCount = rcRecord->indexCount;
    int distributedRelation = rcRecord->distributed;
    char partitionAttrName[MAXNAME+1];
    strcpy(partitionAttrName, rcRecord->attrName);

    DataAttrInfo* attributes = new DataAttrInfo[attrCount];
    if ((rc = GetAttrInfo(relName, attrCount, (char*) attributes))) {
        return rc;
    }
    char* tupleData = new char[tupleLength];
    memset(tupleData, 0, tupleLength);

    ifstream dataFile(fileName);
    if (!dataFile.is_open()) {
        delete rcRecord;
        delete[] attributes;
        delete[] tupleData;
        return SM_INVALID_DATA_FILE;
    }

    if (distributedRelation) {
        vector<string> nodeTuples[numberNodes+1];

        bool found = false;
        int partitionAttrIndex = -1;
        AttrType partitionAttrType;
        for (int i=0; i<attrCount; i++) {
            if (strcmp(attributes[i].attrName, partitionAttrName) == 0) {
                found = true;
                partitionAttrIndex = i;
                partitionAttrType = attributes[i].attrType;
                break;
            }
        }
        if (!found) {
            return EX_INCONSISTENT_PV;
        }

        string line;
        int dataNode = 0;
        while (getline(dataFile, line)) {
            stringstream ss(line);
            vector<string> dataValues;
            string dataValue = "";
            while (getline(ss, dataValue, ',')) {
                dataValues.push_back(dataValue);
            }

            Value key;
            key.type = partitionAttrType;
            if (partitionAttrType == INT) {
                int value = atoi(dataValues[partitionAttrIndex].c_str());
                int* keyValue = new int;
                memcpy(keyValue, &value, sizeof(int));
                key.data = keyValue
                if ((rc = GetDataNodeForTuple(rmManager, key, relName, partitionAttrName, dataNode))) {
                    return rc;
                }
                if (dataNode <= 0 || dataNode > numberNodes) {
                    return EX_INCONSISTENT_PV;
                }
                nodeTuples[dataNode].push_back(line);
                delete keyValue;
            }
            else if (partitionAttrType == FLOAT) {
                float value = atof(dataValues[partitionAttrIndex].c_str());
                float* keyValue = new float;
                memcpy(keyValue, &value, sizeof(float));
                key.data = keyValue;

                if ((rc = GetDataNodeForTuple(rmManager, key, relName, partitionAttrName, dataNode))) {
                    return rc;
                }
                if (dataNode <= 0 || dataNode > numberNodes) {
                    return EX_INCONSISTENT_PV;
                }
                nodeTuples[dataNode].push_back(line);
                delete keyValue;
            }
            else {
                char* keyValue = new char[attributes[partitionAttrIndex].attrLength];
                strcpy(keyValue, dataValues[partitionAttrIndex].c_str());
                key.data = keyValue;

                if ((rc = GetDataNodeForTuple(rmManager, key, relName, partitionAttrName, dataNode))) {
                    return rc;
                }
                if (dataNode <= 0 || dataNode > numberNodes) {
                    return EX_INCONSISTENT_PV;
                }
                nodeTuples[dataNode].push_back(line);
                delete[] keyValue;
            }
        }

        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.LoadInDataNode(relName, nodeTuples[i], i))) {
                return rc;
            }
        }
    }

    else {
        RM_FileHandle rmFH;
        RID rid;
        if ((rc = rmManager->OpenFile(relName, rmFH))) {
            return rc;
        }

        IX_IndexHandle* ixIH = new IX_IndexHandle[attrCount];
        if (indexCount > 0) {
            int currentIndex = 0;
            for (int i=0; i<attrCount; i++) {
                int indexNo = attributes[i].indexNo;
                if (indexNo != -1) {
                    if (currentIndex == indexCount) {
                        return SM_INCORRECT_INDEX_COUNT;
                    }
                    if ((rc = ixManager->OpenIndex(relName, indexNo, ixIH[currentIndex]))) {
                        return rc;
                    }
                    currentIndex++;
                }
            }
        }

        string line;
        while (getline(dataFile, line)) {
            stringstream ss(line);
            vector<string> dataValues;
            string dataValue = "";
            while (getline(ss, dataValue, ',')) {
                dataValues.push_back(dataValue);
            }

            for (int i=0; i<attrCount; i++) {
                if (attributes[i].attrType == INT) {
                    int value = atoi(dataValues[i].c_str());
                    memcpy(tupleData+attributes[i].offset, &value, attributes[i].attrLength);
                }
                else if (attributes[i].attrType == FLOAT) {
                    float value = atof(dataValues[i].c_str());
                    memcpy(tupleData+attributes[i].offset, &value, attributes[i].attrLength);
                }
                else {
                    char value[attributes[i].attrLength];
                    memset(value, 0, attributes[i].attrLength);
                    strcpy(value, dataValues[i].c_str());
                    memcpy(tupleData+attributes[i].offset, value, attributes[i].attrLength);
                }
            }
            if ((rc = rmFH.InsertRec(tupleData, rid))) {
                return rc;
            }

            int currentIndex = 0;
            for (int i=0; i<attrCount; i++) {
                if (attributes[i].indexNo != -1) {
                    if (attributes[i].attrType == INT) {
                        int value = atoi(dataValues[i].c_str());
                        if ((rc = ixIH[currentIndex].InsertEntry(&value, rid))) {
                            return rc;
                        }
                    }
                    else if (attributes[i].attrType == FLOAT) {
                        float value = atof(dataValues[i].c_str());
                        if ((rc = ixIH[currentIndex].InsertEntry(&value, rid))) {
                            return rc;
                        }
                    }
                    else {
                        char* value = (char*) dataValues[i].c_str();
                        if ((rc = ixIH[currentIndex].InsertEntry(value, rid))) {
                            return rc;
                        }
                    }
                    currentIndex++;
                }
            }
        }

        if ((rc = rmManager->CloseFile(rmFH))) {
            return rc;
        }

        if (indexCount > 0) {
            for (int i=0; i<indexCount; i++) {
                if ((rc = ixManager->CloseIndex(ixIH[i]))) {
                    return rc;
                }
            }
        }
        delete[] ixIH;
    }

    dataFile.close();

    delete rcRecord;
    delete[] attributes;
    delete[] tupleData;

    return OK_RC;
}


RC SM_Manager::Help() {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (printCommands) {
        cout << "Help\n";
    }

    int attrCount = SM_RELCAT_ATTR_COUNT;
    RM_Record rec;
    int rc;
    char* recordData;

    DataAttrInfo* attributes = new DataAttrInfo[attrCount];
    if ((rc = GetAttrInfo("relcat", attrCount, (char*) attributes))) {
        return rc;
    }

    Printer p(attributes, attrCount);

    p.PrintHeader(cout);

    RM_FileScan relcatFS;
    if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, NO_OP, NULL))) {
        return rc;
    }

    while (rc != RM_EOF) {
        rc = relcatFS.GetNextRec(rec);

        if (rc != 0 && rc != RM_EOF) {
            return rc;
        }

        if (rc != RM_EOF) {
            rec.GetData(recordData);
            p.Print(cout, recordData);
        }
    }

    p.PrintFooter(cout);

    if ((rc = relcatFS.CloseScan())) {
        return rc;
    }
    delete[] attributes;

    return OK_RC;
}

RC SM_Manager::Help(const char *relName) {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (relName == NULL) {
        return SM_NULL_RELATION;
    }

    if (printCommands) {
        cout << "Help\n"
             << "   relName=" << relName << "\n";
    }

    int rc;
    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    delete rcRecord;

    int attrCount = SM_ATTRCAT_ATTR_COUNT;
    RM_Record rec;
    char* recordData;

    DataAttrInfo* attributes = new DataAttrInfo[attrCount];
    if ((rc = GetAttrInfo("attrcat", attrCount, (char*) attributes))) {
        return rc;
    }

    Printer p(attributes, attrCount);

    p.PrintHeader(cout);

    RM_FileScan attrcatFS;
    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }

    while (rc != RM_EOF) {
        rc = attrcatFS.GetNextRec(rec);

        if (rc != 0 && rc != RM_EOF) {
            return rc;
        }

        if (rc != RM_EOF) {
            rec.GetData(recordData);
            p.Print(cout, recordData);
        }
    }

    p.PrintFooter(cout);

    if ((rc = attrcatFS.CloseScan())) {
        return rc;
    }
    delete[] attributes;

    return OK_RC;
}

RC SM_Manager::Print(const char *relName) {
    if (!isOpen) {
        return SM_DATABASE_CLOSED;
    }

    if (relName == NULL) {
        return SM_NULL_RELATION;
    }

    if (printCommands) {
        cout << "Print\n"
             << "   relName=" << relName << "\n";
    }

    RM_Record rec;
    int rc;
    char* recordData;

    SM_RelcatRecord* rcRecord = new SM_RelcatRecord;
    memset(rcRecord, 0, sizeof(SM_RelcatRecord));
    if ((rc = GetRelInfo(relName, rcRecord))) {
        delete rcRecord;
        return rc;
    }
    int distributedRelation = rcRecord->distributed;

    int attrCount = rcRecord->attrCount;
    DataAttrInfo* attributes = new DataAttrInfo[attrCount];
    if ((rc = GetAttrInfo(relName, attrCount, (char*) attributes))) {
        return rc;
    }

    Printer p(attributes, attrCount);

    p.PrintHeader(cout);

    if (distributedRelation) {
        EX_CommLayer commLayer(rmManager, ixManager);
        for (int i=1; i<=numberNodes; i++) {
            if ((rc = commLayer.PrintInDataNode(p, relName, i))) {
                return rc;
            }

            if (i != numberNodes && partitionedPrint) {
                cout << "......." << endl;
            }
        }
    }

    else {
        RM_FileHandle rmFH;
        if ((rc = rmManager->OpenFile(relName, rmFH))) {
            return SM_TABLE_DOES_NOT_EXIST;
        }

        RM_FileScan rmFS;
        if ((rc = rmFS.OpenScan(rmFH, INT, 4, 0, NO_OP, NULL))) {
            return rc;
        }

        while (rc != RM_EOF) {
            rc = rmFS.GetNextRec(rec);

            if (rc != 0 && rc != RM_EOF) {
                return rc;
            }

            if (rc != RM_EOF) {
                rec.GetData(recordData);
                p.Print(cout, recordData);
            }
        }

        if ((rc = rmFS.CloseScan())) {
            return rc;
        }
        if ((rc = rmManager->CloseFile(rmFH))) {
            return rc;
        }
    }

    p.PrintFooter(cout);

    delete rcRecord;
    delete[] attributes;

    return OK_RC;
}

RC SM_Manager::Set(const char *paramName, const char *value) {
    if (paramName == NULL || value == NULL) {
        return SM_NULL_PARAMETERS;
    }

    if (strcmp(paramName, "printCommands") == 0) {
        if (strcmp(value, "TRUE") == 0) {
            printCommands = TRUE;
        }
        else if (strcmp(value, "FALSE") == 0) {
            printCommands = FALSE;
        }
        else {
            return SM_INVALID_VALUE;
        }
    }
    else if (strcmp(paramName, "optimizeQuery") == 0) {
        if (strcmp(value, "TRUE") == 0) {
            optimizeQuery = TRUE;
        }
        else if (strcmp(value, "FALSE") == 0) {
            optimizeQuery = FALSE;
        }
        else {
            return SM_INVALID_VALUE;
        }
    }
    else if (strcmp(paramName, "partitionedPrint") == 0) {
        if (strcmp(value, "TRUE") == 0) {
            partitionedPrint = TRUE;
        }
        else if (strcmp(value, "FALSE") == 0) {
            partitionedPrint = FALSE;
        }
        else {
            return SM_INVALID_VALUE;
        }
    }
    else if (strcmp(paramName, "bQueryPlans") == 0) {
        if (strcmp(value, "1") == 0) {
            bQueryPlans = 1;
        }
        else if (strcmp(value, "0") == 0) {
            bQueryPlans = 0;
        }
        else {
            return SM_INVALID_VALUE;
        }
    }
     else {
        return SM_INVALID_SYSTEM_PARAMETER;
    }

    if (printCommands) {
        cout << "Set\n"
             << "   paramName=" << paramName << "\n"
             << "   value    =" << value << "\n";
    }

    return OK_RC;
}

RC SM_Manager::GetAttrInfo(const char* relName, int attrCount, char* attributeData) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (attrCount < 0) {
        return SM_INCORRECT_ATTRIBUTE_COUNT;
    }

    int rc;
    RM_FileScan attrcatFS;
    RM_Record rec;
    char* recordData;
    SM_AttrcatRecord* acRecord;
    DataAttrInfo* attributes = (DataAttrInfo*) attributeData;

    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }

    int i = 0;
    while (rc != RM_EOF) {
        rc = attrcatFS.GetNextRec(rec);
        if (rc != 0 && rc != RM_EOF) {
            return rc;
        }

        if (rc != RM_EOF) {
            if (i == attrCount) {
                return SM_INCORRECT_ATTRIBUTE_COUNT;
            }

            if ((rc = rec.GetData(recordData))) {
                return rc;
            }
            acRecord = (SM_AttrcatRecord*) recordData;

            strcpy(attributes[i].relName, acRecord->relName);
            strcpy(attributes[i].attrName, acRecord->attrName);
            attributes[i].offset = acRecord->offset;
            attributes[i].attrType = acRecord->attrType;
            attributes[i].attrLength = acRecord->attrLength;
            attributes[i].indexNo = acRecord->indexNo;
            i++;
        }
    }

    if ((rc = attrcatFS.CloseScan())) {
        return rc;
    }

    return OK_RC;
}

RC SM_Manager::GetAttrInfo(const char* relName, const char* attrName, SM_AttrcatRecord* attributeData) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }
    if (attrName == NULL) {
        return SM_NULL_ATTRIBUTES;
    }

    int rc;
    RM_FileScan attrcatFS;
    RM_Record rec;
    char* recordData;
    SM_AttrcatRecord* acRecord;

    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = attrcatFS.OpenScan(attrcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }

    while (rc != RM_EOF) {
        rc = attrcatFS.GetNextRec(rec);
        if (rc != 0 && rc != RM_EOF) {
            return rc;
        }

        if (rc != RM_EOF) {
            if ((rc = rec.GetData(recordData))) {
                return rc;
            }
            acRecord = (SM_AttrcatRecord*) recordData;

            if (strcmp(acRecord->attrName, attrName) == 0) {
                strcpy(attributeData->relName, acRecord->relName);
                strcpy(attributeData->attrName, acRecord->attrName);
                attributeData->offset = acRecord->offset;
                attributeData->attrType = acRecord->attrType;
                attributeData->attrLength = acRecord->attrLength;
                attributeData->indexNo = acRecord->indexNo;
                break;
            }
        }
    }

    if (rc == RM_EOF) {
        return SM_INVALID_ATTRIBUTE;
    }

    if ((rc = attrcatFS.CloseScan())) {
        return rc;
    }

    return OK_RC;
}

RC SM_Manager::GetRelInfo(const char* relName, SM_RelcatRecord* relationData) {
    if (relName == NULL) {
        return SM_NULL_RELATION;
    }

    int rc;
    RM_FileScan relcatFS;
    RM_Record rec;
    char* recordData;
    SM_RelcatRecord* rcRecord;

    char relationName[MAXNAME+1];
    strcpy(relationName, relName);
    if ((rc = relcatFS.OpenScan(relcatFH, STRING, MAXNAME, 0, EQ_OP, relationName))) {
        return rc;
    }

    if ((rc = relcatFS.GetNextRec(rec))) {
        if (rc == RM_EOF) {
            return SM_TABLE_DOES_NOT_EXIST;
        }
    }
    if ((rc = rec.GetData(recordData))) {
        return rc;
    }
    rcRecord = (SM_RelcatRecord*) recordData;

    strcpy(relationData->relName, rcRecord->relName);
    relationData->tupleLength = rcRecord->tupleLength;
    relationData->attrCount = rcRecord->attrCount;
    relationData->indexCount = rcRecord->indexCount;
    relationData->distributed = rcRecord->distributed;
    strcpy(relationData->attrName, rcRecord->attrName);

    if ((rc = relcatFS.CloseScan())) {
        return rc;
    }

    return OK_RC;
}

int SM_Manager::getPrintFlag() {
    return printCommands;
}

int SM_Manager::getOpenFlag() {
    return isOpen;
}

int SM_Manager::getDistributedFlag() {
    return distributed;
}

int SM_Manager::getNumberNodes() {
    return numberNodes;
}

int SM_Manager::getOptimizeFlag() {
    return optimizeQuery;
}

int SM_Manager::getPartitionedPrintFlag() {
    return partitionedPrint;
}