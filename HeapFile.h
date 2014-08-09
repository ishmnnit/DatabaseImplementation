#ifndef HEAPFILE_H
#define HEAPFILE_H
#include "DBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class GenericDBFile;
class HeapFile:public GenericDBFile {
		File *myFile;
		Page *readBuffer;
		Page *writeBuffer;
		int currPage;
		void write_buffer();
	  public:
	HeapFile ();
		int Create (char *fpath, fType file_type, void *startup);
		int Open (char *fpath);
		int Close ();
		void Load (Schema &myschema, char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		void CreateFile(fType f_type);
};
#endif
