#ifndef DBFILE_H
#define DBFILE_H
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <fstream>
#include <cstring>

typedef enum {heap, sorted, tree} fType;

struct SortInfo {
 OrderMaker *myOrder;
 int run_length;
} ;

class GenericDBFile;
class DBFile {
	public:
		struct SortInfo *sort_info;
		GenericDBFile *db_file_ob;
		ifstream input_meta;
		ofstream output_meta;
		int Create (char *fpath, fType file_type, void *startup);
		int Open (char *fpath);
		int Close ();
		void Load (Schema &myschema, char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

class GenericDBFile
{         
	
	    public:
		ifstream input_meta;
		ofstream output_meta;
		virtual int Create (char *fpath, fType file_type, void *startup)=0;
		virtual int Open (char *fpath)=0;
		virtual int Close ()=0;
		virtual void Load (Schema &myschema, char *loadpath)=0;
		virtual void MoveFirst ()=0;
		virtual void Add (Record &addme)=0;
		virtual int GetNext (Record &fetchme)=0;
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;	
};
#endif
