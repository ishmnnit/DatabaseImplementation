#ifndef SORTEDFILE_H
#define SORTEDFILE_H
#include "DBFile.h"
#include "Pipe.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "BigQ.h"
#include <algorithm>

typedef enum {reading ,writing} fMode;
#define MAX_BUFFER 1000

typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker* sort_order;
	int run_len;
} bigq_util;


//class GenericDBFile;
class SortedFile : public virtual GenericDBFile {
	    char* fpath;
	    Pipe *in;
		Pipe *out;
		//bigq_util* util;
		BigQ *bq;
		fMode mode;
		File *myFile;
		File tempFile;
		Page temp_page;
		Page readBuffer;
		Page tempAddpage;
		OrderMaker *sort_info_od;
		//SortInfo *sort_info;
		int currPage;
		pthread_t thread1;
		int runlen;
		bool notCalled;
public:
	//File *sorted_file;
	SortedFile ( );

	void initialiseSortedFile( char *f_path, int file_length);
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int GetNextWrapper(Record &fetchme);
	//virtual int GetNext (Record &fetchme);
	//virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);	
    void AddData(Record &rec,int &);
	void ChangeMode();
	void MergeFile();
	int LinearSearch(Record &fetchme, CNF &cnf, Record &literal);
	int BinarySearch(Record &fetchme, CNF &cnf, Record &literal,OrderMaker &query_order);
	int GetMatch(Record &fetchme, Record &literal, CNF &cnf, OrderMaker &search_order,OrderMaker &literal_order);
	int GetNext_File(Record &fetchme);
};
#endif
