#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	cout << "Doing QU_Delete " << endl;
	Status status;
	HeapFileScan hfs(relation,status);
	if(status!=OK){
		return status;
	}
// part 6
if(attrName.empty()){
	//delete everything.
	status = hfs.startScan(0,0,STRING,NULL,EQ);
	if(status!=OK){
		return status;
	}
	RID rid;
	while((status = hfs.scanNext(rid)) == OK){
		status = hfs.deleteRecord();
		if(status!=OK) return status;
	}
	status = hfs.endScan();
	if(status!=OK) return status;
}
else{
	AttrDesc desc;
	status = attrCat->getInfo(relation,attrName,desc);
	if(status!=OK) return status;
	int a;
	float f;
	if(type==INTEGER){
		a = atoi(attrValue);
		status = hfs.startScan(desc.attrOffset,desc.attrLen,type,(char*)&a,op);
	}
	else if(type==FLOAT){
		f = atof(attrValue);
		status = hfs.startScan(desc.attrOffset,desc.attrLen,type,(char*)&f,op);
	}
	else if(type==STRING){
		status = hfs.startScan(desc.attrOffset,desc.attrLen,type,attrValue,op);
	}
	if(status!=OK) return status;
	RID rid;
	while((status = hfs.scanNext(rid)) == OK){
		status = hfs.deleteRecord();
		if(status!=OK) return status;
	}
	status = hfs.endScan();
	if(status!=OK) return status;
}
return OK;
}


