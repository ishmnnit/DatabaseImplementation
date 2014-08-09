#ifndef REL_OP_H
#define REL_OP_H
#include "BigQ.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>
#include <cstdio>

void *selFile(void *);
void *selPipe(void*);
void *projectWorker(void*);
void *WriteoutWorker(void*);
void* DupremWorker(void *obj);

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:

	public:
	Record record;
	DBFile *inputFile;
	Pipe *outputPipe;
	CNF selOpr;
	pthread_t selthread;
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {

	public:
	Record record;
	Pipe *inputPipe;
	Pipe *outputPipe;
	CNF selOpr;
	pthread_t selthread;
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) ;
};


class Project : public RelationalOp { 
	public:
	Pipe *inputPipe;
	Pipe *outputPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	pthread_t thread1;
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Join : public RelationalOp {

	private :
		Pipe *inPipeL;
		Pipe *inPipeR;
		Pipe *outPipe;
		CNF  *selOP;
		Record *literal;
		int runlen;
		pthread_t thread;
		static void* workerFunc( void * );
		static void nestedJoin( void * );
		static void sortMergeJoin( void *, OrderMaker &leftO, OrderMaker &rightO );

	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);

};



class DuplicateRemoval : public RelationalOp {
	public:
	int runlen;
	pthread_t thread1;
	Pipe *inputPipe,*outputPipe;
	Schema *mySchema;
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Sum : public RelationalOp {
    public:
	pthread_t thread1;
	Pipe *inputPipe;
	Pipe *outputPipe;
	Function *computeMe;
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp {
	public:
	int runlen;
	pthread_t thread1;
	Pipe *inputPipe;
	Pipe *outputPipe;
	Function *computeMe;
	OrderMaker *groupAtts;
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
	public:
	pthread_t thread1;
	Pipe *inputPipe;
	FILE *outFile;
	Schema *mySchema;
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};

#endif
