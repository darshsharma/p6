#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;
	// go through the projection list and look up each in the 
    // attr cat to get an AttrDesc structure (for offset, length, etc)
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }
    // get AttrDesc structure for the attribute
    AttrDesc attrDesc;
    if(attr!=NULL){
        status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     attrDesc);
        if (status != OK){
            return status;
        }                           
    }

    
	int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }
	status = ScanSelect(result,projCnt,attrDescArray,&attrDesc,op,attrValue,reclen);
    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
	HeapFileScan scanObj(projNames[0].relName, status);
	if (status != OK) { return status; }
    int filterInt;
    float filterFloat;
    if(filter==NULL){
        status = scanObj.startScan(0,0,STRING,NULL,EQ);
    }
    else if(attrDesc->attrType==STRING){
        status = scanObj.startScan(attrDesc->attrOffset,attrDesc->attrLen, STRING,filter,op);
    }
    else if(attrDesc->attrType==INTEGER){
        filterInt = atoi(filter);
        status = scanObj.startScan(attrDesc->attrOffset,attrDesc->attrLen, INTEGER,(char*)&filterInt,op);
    }
    else
    if(attrDesc->attrType==FLOAT){
        filterFloat = atof(filter);
        status = scanObj.startScan(attrDesc->attrOffset,attrDesc->attrLen, FLOAT,(char*)&filterFloat,op);
    }
    if(status!=OK){
        return status;
    }
    RID rid;
    Record rec;
    while(scanObj.scanNext(rid)==OK){
            status = scanObj.getRecord(rec);
            if(status!=OK) return status;
            
            // we have a match, copy data into the output record
            int outputOffset = 0;
            for (int i = 0; i < projCnt; i++)
            {
                memcpy(outputData + outputOffset, (char *)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
                outputOffset += projNames[i].attrLen;
            }

            // add the new record to the output relation
            RID outRID;
            status = resultRel.insertRecord(outputRec, outRID);
            if(status!=OK) return status;
    }
    status = scanObj.endScan();
    return status;
}
