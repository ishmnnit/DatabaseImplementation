#ifndef SORT_H
#define SORT_H
#include <vector>
#include "GenericDBFile.h"
#include "BigQ.h"
#include "Defs.h"
#include <stdio.h>




class Sort: public GenericDBFile{
private:

public:
	Sort ();
	RecPointer CurrentRecord;
	BigQ *bigq;
	OrderMaker *sortorder;
	OrderMaker *cnfsort;
	OrderMaker *query;
	int runlength;
	File *CurrentFile;
	File *TempFile;
	Page *Adder;
	Pipe *in ;
	Pipe *out ;
	off_t start ;
	off_t end ;
	off_t mid ;
	off_t insertoffset;
	bool first;
	char filepath[100];
	string file_path;
	bool DirtyPageAdded;
	char mode;  // R for read and W for write
	int globalcount;
	int count;


	int Create (char *fpath,  void *startup);
	int Open (char *fpath);
	int Close ();
	void merge();
	void Load (Schema &myschema, char *loadpath);
	bool order_based_sort(Record *rec1, Record *rec2, OrderMaker &sortingorder);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void AddPage();
	int binarysearch(OrderMaker *query, Record &fetchme, Record &literal, CNF &cnf);
	int linearscan(off_t Pagenum,Record &literal,OrderMaker *query, Record &fetchme, CNF &cnf);
	int GetCNFSortOrder (OrderMaker &left, CNF &cnf);
};
#endif

