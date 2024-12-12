#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
cout << "Doing QU_Insert " << endl;
Status status;
InsertFileScan resultRel(relation, status);
if (status != OK) { return status; }
RelDesc relDesc;
status = relCat->getInfo(relation,relDesc);
if(status!=OK) return status;
if(relDesc.attrCnt!=attrCnt){
	cout << "Error";
	return OK;
}
AttrDesc attrDescArray[attrCnt];
int recLen;
for(int i=0;i<attrCnt;i++){
	AttrDesc *attrDesc;
	status = attrCat->getInfo(relation,attrList[i].attrName,attrDescArray[i]);
	recLen+=attrDescArray[i].attrLen;
	if(status!=OK) return OK;
}
char outputData[recLen];
Record outputRec;
outputRec.data = (void *) outputData;
outputRec.length = recLen;
for(int i=0;i<attrCnt;i++){
	if(attrList[i].attrValue!=NULL){
		if (attrList[i].attrType==INTEGER)
		{
			int a = atoi((char*)attrList[i].attrValue);
			memcpy(outputData + attrDescArray[i].attrOffset,
					(char*)&a,attrDescArray[i].attrLen);
		}
		else if(attrList[i].attrType==FLOAT){
			float f = atof((char*) attrList[i].attrValue);
			memcpy(outputData + attrDescArray[i].attrOffset,
					(char*)&f,attrDescArray[i].attrLen);
		}
		else{
			memcpy(outputData + attrDescArray[i].attrOffset,
					(char *)attrList[i].attrValue,attrDescArray[i].attrLen);
		} 
	}
	else{ cout << "Error";
	return OK;}   
}
RID outRID;
status = resultRel.insertRecord(outputRec,outRID);
if(status!=OK){
	return status;
}
return OK;
}

