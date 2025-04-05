#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it
        status = db.createFile(fileName);

        // trouble creating file
        if (status != OK)
            return status;

        // properly initialize all pointers in file
        status = db.openFile(fileName, file);
        if (status != OK)
            return status;

        // allocate empty page by invoking bm->allocPage()
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
            return status;

        // use the pointer returned from allocPage() and cast it to a FileHdrPage*
        hdrPage = (FileHdrPage *)newPage;

        // use the hdrPage pointer to init values in header page
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);

        // call bm->allocPage() again; this will be first data page of file
        status = bufMgr->allocPage(file, newPageNo, newPage);

        if (status != OK)
        {
            return status;
        }

        // use newPage pointer to invoke init method
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1);
        if (status != OK)
            return status;

        // only header page
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;
        // store page number of the data page in firstPage and lastPage attributes of FileHdrPage
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK)
            return status;
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK)
            return status;

        // flush the pages and close the file; need this or we get errors in tests
        status = bufMgr->flushFile(file);
        if (status != OK)
            return (status);
        status = db.closeFile(file);
        if (status != OK)
            return (status);
        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    Page *pagePtr;

    cout << "opening file " << fileName << endl;

    // first open the appropriate file by calling db.openFile()
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // read and pin the header page for the file in the buffer pool; use file->getFirstPage()
        // init headerPageNo
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK)
        {
            cerr << "error getting first pageNo\n";
            returnStatus = status;
        }

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            cerr << "error reading header page\n";
            returnStatus = status;
        }
        // init headerPage and hdrDirtyFlag
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;

        // read and pin the first page of the file into the buffer pool
        // init curPageNo
        curPageNo = headerPage->firstPage;

        // init curPage
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
        {
            cerr << "error reading first data page\n";
            returnStatus = status;
        }

        // init the values of curDirtyFlag appropriately
        curDirtyFlag = false;

        // set curRec to NULLRID
        curRec = NULLRID;
        returnStatus = OK;
        return;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    // if the desired record is on the currently pinned page, invoke curpage->getrecord(rid, rec)
    if (curPage != nullptr && curPageNo == rid.pageNo)
    {
        status = curPage->getRecord(rid, rec);
        if (status == OK)
            curRec = rid; // do the bookkeeping
        return status;
    }
    // otherwise, unpin the currently pinned page (assuming a page is pinned)
    else
    {
        if (curPage != nullptr)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
            // do the bookkeeping
            curPage = nullptr;
        }

        // use the pageNo field of the RID to read the page into the buffer pool
        // do the bookkeeping
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;

        // do the bookkeeping
        curDirtyFlag = false;

        status = curPage->getRecord(rid, rec);
        if (status == OK)
        {
            // do the bookkeeping
            curRec = rid;
        }
        return status;
    }
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    { // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID &outRid)
{
    Status 	status = OK;
	RID		nextRid;
	RID		tmpRid;
	int 	nextPageNo;
	Record	  rec;

	if (curPageNo < 0) {
		return FILEEOF;
	}
	if (curPage == NULL) {
		// get the first page
		curPageNo = headerPage->firstPage;
		if (curPageNo == -1) {
			return FILEEOF;
		}
		// read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		curDirtyFlag = false;
		curRec = NULLRID;
		if (status != OK) {
			return status;
		}
		else {
			// get the first record
			status  = curPage->firstRecord(tmpRid);
			curRec = tmpRid;
			if (status == NORECORDS) {
				if ((status = bufMgr->unPinPage(filePtr, curPageNo, 
												curDirtyFlag)) != OK) {
					return status;
				}
				curPageNo = -1;
				curPage = NULL;
				return FILEEOF;
			}
			if ((status = curPage->getRecord(tmpRid, rec)) != OK) {
				return status;
			}
			if (matchRec(rec) == true) {
				outRid = tmpRid;
				return OK;
			}
		}
	}
	while(true) {
	 	if ((status  = curPage->nextRecord(curRec, nextRid)) == OK) {
			curRec = nextRid;
		}
		while (status == ENDOFPAGE || status == NORECORDS) {
			// get the next page
			curPage->getNextPage(nextPageNo);
			if (nextPageNo == -1) {
				return FILEEOF;
			}
			// unpin current page
			status = bufMgr->unPinPage(filePtr,curPageNo, curDirtyFlag);
			curPage = NULL;
			curPageNo = -1;
			if (status != OK) {
				return status;
			}
			// read next page
			curPageNo = nextPageNo;
			curDirtyFlag = false;
			if ((status = bufMgr->readPage(filePtr,curPageNo,
											curPage)) != OK) {
				return status;
			}
			// get the first record
			status = curPage->firstRecord(curRec);
		}
		if ((status = curPage->getRecord(curRec, rec)) != OK) {
			return status;
		}
		if (matchRec(rec) == true) {
			outRid = curRec;
			return OK;
		}
	}
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
Page *newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
		// make the last page of the file the current page
		curPageNo = headerPage->lastPage;
		if ((status = bufMgr->readPage(filePtr, curPageNo, 
										curPage)) != OK) {
			return status;
		}
	}
	// add record to current page 
	if ((status = curPage->insertRecord(rec, rid)) == OK) {
		headerPage->recCnt++;
		hdrDirtyFlag = true;
		outRid = rid;
		curDirtyFlag = true;
		return status;
	}
	else {
		// allocate & setup new page
		if ((status = bufMgr->allocPage(filePtr, newPageNo, newPage)) != OK) {
			return status;
		}
		newPage->init(newPageNo);
		if ((status = newPage->setNextPage(-1)) != OK) {
			return status;
		}
		headerPage->lastPage = newPageNo;
		headerPage->pageCnt++;
		hdrDirtyFlag = true;
		if ((status = curPage->setNextPage(newPageNo)) != OK) {
			return status;
		}
		if ((status = bufMgr->unPinPage(filePtr, curPageNo, true)) != OK) {
			curPage = NULL;
			curPageNo = -1;
			curDirtyFlag = false;
			unpinstatus = bufMgr->unPinPage(filePtr, newPageNo, true);
			return status;
		}
		curPage = newPage;
		curPageNo = newPageNo;

		// add the record
		if ((status = curPage->insertRecord(rec, rid)) == OK) {
			curDirtyFlag = true;
			headerPage->recCnt++;
			hdrDirtyFlag = true;
			outRid = rid;
			return status;
		}
		else {
			return status;
		}
	}
}
