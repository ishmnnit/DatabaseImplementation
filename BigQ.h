#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <queue>
#include <cmath>
#include <cstdlib>
#include <algorithm>
using namespace std;
#define BIGQTEMPFILE "bigqtemp.dat"

class BigQ
{
public:
	pthread_t worker;
	OrderMaker sortOrder;
	Pipe &inputPipe;
	Pipe &outputPipe;
	int runLength;	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

/**
 * Class RecordWrapper for Wrapping records along with OrderMaker. Used to internal sorting list of records.
 */ 
class RecordWrapper
{
	public:
	Record record;
	OrderMaker* sortOrder;
	RecordWrapper(Record *record,OrderMaker* orderMaker);
	static int compareRecords(const void *rw1, const void *rw2);
};
/**
 * ComparisonClass used for Priority queue for comparing objects on queue.
 */
class ComparisonClass
{
	ComparisonEngine* compEngine;
	public:
	ComparisonClass();
	bool operator()(const pair<RecordWrapper*,int> &lhs, const pair<RecordWrapper*,int> &rhs);
};
//Main worker function which invokes various functions to do tasks.
void* workerFunc(void *bigQ);

//Function which sorts list of records specified in record wrapper and add it to end of file. Method also adds run length info in runLengthInfo
void sortAndCopyToFile(vector<RecordWrapper*>& list,File* file,vector<pair<off_t,off_t> >& runLengthInfo);

//Function to create runs each of which reads pages equal to runlen.
void createRuns(int runlen, Pipe& in, File* file,OrderMaker& sortOrder,vector<pair<off_t,off_t> >& runLengthInfo);

//Function merges runs from file specified and reads data from runLengthInfo.
void mergeRunsFromFile(File* file, int runLength,Pipe& out,OrderMaker& orderMaker,vector<pair<off_t,off_t> >& runLengthInfo);

#endif
