

#ifndef GENERICDBFILE_H_
#define GENERICDBFILE_H_

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
//#include <vector>
#include <string>
//typedef enum {heap, sorted, tree} fType; //Indicates the type in which the files are stored

// stub DBFile header..replace it with your own DBFile.h

class GenericDBFile
{
private:

//void Sort::linearscan(int Pagenum,Record &literal,OrderMaker query)
	//A single instance of the File Class


public:

	//GenericDBFile ();
	virtual int Create (char *fpath, void *startup)=0;
	virtual int Open (char *type)=0;
	virtual int Close ()=0;

	virtual void Load (Schema &myschema, char *loadpath)=0;

	virtual void MoveFirst ()=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
	virtual void AddPage()=0;
};
#endif

 /* GENERICDBFILE_H_ */
