//
// rm.h
//
//   Record Manager component interface
//
// This file does not include the interface for the RID class.  This is
// found in rm_rid.h
//

#ifndef RM_H
#define RM_H

// Please DO NOT include any files other than redbase.h and pf.h in this
// file.  When you submit your code, the test program will be compiled
// with your rm.h and your redbase.h, along with the standard pf.h that
// was given to you.  Your rm.h, your redbase.h, and the standard pf.h
// should therefore be self-contained (i.e., should not depend upon
// declarations in any other file).

// Do not change the following includes
#include "redbase.h"
#include "rm_rid.h"
#include "pf.h"
#include<string>

struct RM_FileHeaderPage{
    int recordSize;                             //the size of the records in the file
    int numRecordsPerPage;       //the number of records 
    int numPages;                            //the current number of pages int the file
    PageNum firstFreePage;        //the location of pages with free space

    int bitmapOffset;
    int bitmapSize;
};

//
// RM_Record: RM Record interface
//
class RM_Record {
    friend class RM_FileHandle;
    friend class RM_FileScan;
public:
    RM_Record ();
    ~RM_Record();

    // Return the data corresponding to the record.  Sets *pData to the
    // record contents.
    RC GetData(char *&pData) const;
    RM_Record(const RM_Record &rec);
    RM_Record& operator=(const RM_Record& rec);

    // Return the RID associated with the record
    RC GetRid (RID &rid) const;

private:
    bool isValid;                // is valid to store a record
    char* rData;                //record Data.
    int recordSize;             //size of record.
    RID rid;                    //Rid of record.
};

//
// RM_FileHandle: RM File interface
//
class RM_FileHandle {
    friend class RM_FileScan;
    friend class RM_Manager;
public:

    RM_FileHandle ();
    ~RM_FileHandle();

    //Copy constructor
    RM_FileHandle(const RM_FileHandle& fileHandle);
    //Overload =
    RM_FileHandle& operator=(const RM_FileHandle& fileHandle);

    // Given a RID, return the record
    RC GetRec     (const RID &rid, RM_Record &rec) const;

    RC InsertRec  (const char *pData, RID &rid);       // Insert a new record

    RC DeleteRec  (const RID &rid);                    // Delete a record
    RC UpdateRec  (const RM_Record &rec);              // Update a record

    // Forces a page (along with any contents stored in this class)
    // from the buffer pool to disk.  Default value forces all pages.
    RC ForcePages (PageNum pageNum = ALL_PAGES);

    RC GetPageAndSlot(const RID &rid, PageNum &pageNum, SlotNum& slotNum) const;
    RC GetRecordOffset(int slotNum) const;
    RC GetFirstZeroBit(char *bitmap, int bitmapSize, int &location);
    RC SetBit(int bitNumber, char * bitmap);
    RC UnsetBit(int slowNum,char* bitmap);
    bool isBitmapFull(char * bitmap,int numRecords);
    bool isBitmapEmpty(char * bitmap,int numRecords);

private:
    bool isOpen;                                         // File handle open flag
    bool isHeaderModified;                               // Modified flag for the file header
    PF_FileHandle pfFH;                                // PF file handle
    RM_FileHeaderPage fileHeader;                      // File handle information
    int GetRecordOffset(int slowNum) const;

 };

//
// RM_FileScan: condition-based scan of records in the file
//
class RM_FileScan {
public:
    RM_FileScan  ();
    ~RM_FileScan ();

    RC OpenScan  (const RM_FileHandle &fileHandle,
                  AttrType   attrType,
                  int        attrLength,
                  int        attrOffset,
                  CompOp     compOp,
                  void       *value,
                  ClientHint pinHint = NO_HINT); // Initialize a file scan
    RC GetNextRec(RM_Record &rec);               // Get next matching record
    RC CloseScan ();                             // Close the scan

private:
    RM_FileHandle fielhandle;
    PageNum pageNum;
    SlotNum slotNum;
    int attrLength;
    AttrType attrType;
    int attrOffest;
    CompOp compOp;
    void *value;
    ClientHint pinHint;
    bool isScanOpen;

    bool isSlotInBitMap(SlotNum slotNum,char* bitmap);
    int GetIntValue(char* rData);
    float GetFloatValue(char* rData);
    std::string GetStringValue(char* rData);

    template<typename T>
    bool matchRecord(T rValue, T gValue);

};

//
// RM_Manager: provides RM file management
//
class RM_Manager {
public:
    RM_Manager    (PF_Manager &pfm);
    ~RM_Manager   ();

    RC CreateFile (const char *fileName, int recordSize);
    RC DestroyFile(const char *fileName);
    RC OpenFile   (const char *fileName, RM_FileHandle &fileHandle);

    RC CloseFile  (RM_FileHandle &fileHandle);

private:
    PF_Manager* pfManager;
    int findNumberRecords(int recordSize);
};

//
// Print-error function
//
void RM_PrintError(RC rc);


//Warnings
#define RM_SMALL_RECORD             (START_RM_WARN + 0)
#define RM_LARAGE_RECOED            (START_RM_WARN + 1)
#define RM_FILE_OPEN                (START_RM_WARN + 2)
#define RM_FILE_CLOSED              (START_RM_WARN + 3)
#define RM_INVALID_FILENAME         (START_RM_WARN + 9) //Invalid filename
#define RM_INVALID_SLOW_NUMBER      (START_RM_WARN + 10) //Invalid slow number
#define RM_INVALID_PAGE_NUMBER      (START_RM_WARN + 11) //Invalid slow number
#define RM_NULL_RECORD                          (START_RM_WARN + 13) // NULL Record
#define RM_INVALID_ATTRTYPE                 (START_RM_WARN + 14) // NULL Record
#define RM_INVALID_OFFEST                      (START_RM_WARN + 15) // NULL Record
#define RM_INVALID_OPERATOR               (START_RM_WARN + 16) // NULL Record
#define RM_ATTRIBUTE_NOT_CONSISTENT    (START_RM_WARN + 17) // NULL Record
#define RM_EOF                                                             (START_RM_WARN + 18)
//ERRORS
#define RM_INCONSISTENT_BITMAP  (START_RM_ERR - 1) // Inconsistent bitmap in page
#define RM_RECORD_NOT_VALID                     (START_RM_WARN + 19)
#endif
