#include "RelOp.h"
#include <iomanip>


void* SelFile(void *obj)
{
	SelectFile *selobj=(SelectFile*)obj;
	Record *fetchme=new Record;
	ComparisonEngine compEngine;
	int cnt=0;
	while(selobj->inputFile->GetNext(*fetchme))
	{
		if (compEngine.Compare(fetchme, &(selobj->record), &(selobj->selOpr)))
           {
			  if(fetchme!=NULL)
			    selobj->outputPipe->Insert(fetchme);
			  fetchme=new Record;
           }
	}
    selobj->outputPipe->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	inputFile=&inFile;
	outputPipe=&outPipe;
	selOpr=selOp;
	record=literal;
	pthread_create(&selthread,NULL,SelFile,(void*)this);
	//outputPipe->ShutDown();
}



void SelectFile::WaitUntilDone () {
	pthread_join (selthread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	runlen=10;

}

//################################################
/* Select Pipe Function */
//##################################################

void* SelPipe(void *obj)
{
	SelectPipe *selobj=(SelectPipe*)obj;
	Record *fetchme=new Record;
	ComparisonEngine compEngine;
	int cnt=0;
	while(selobj->inputPipe->Remove(fetchme))
	{
		if (compEngine.Compare(fetchme, &(selobj->record), &(selobj->selOpr)))
           {
			  if(fetchme!=NULL)
			    selobj->outputPipe->Insert(fetchme);
			  fetchme=new Record;
           }
	}
    selobj->outputPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	inputPipe=&inPipe;
	outputPipe=&outPipe;
	selOpr=selOp;
	record=literal;
	pthread_create(&selthread,NULL,SelFile,(void*)this);
}



void SelectPipe::WaitUntilDone () {
	pthread_join (selthread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	runlen=10;
}
//###################################################################
// PROJECT FUNCTION
//#####################################################################
void* projectWorker(void *obj)
{
	Project *pobj=(Project*)obj;
	Record fetchme;
	ComparisonEngine compEngine;
	int cnt=0;
	//cout<<"Reached Here\n";
	while(pobj->inputPipe->Remove(&fetchme))
	{
		Record rec;
		rec.Copy(&fetchme);
		rec.Project(pobj->keepMe,pobj->numAttsOutput,pobj->numAttsInput);
		pobj->outputPipe->Insert(&rec);
	}
    pobj->outputPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	inputPipe=&inPipe;
	outputPipe=&outPipe;
	this->keepMe=keepMe;
	this->numAttsInput=numAttsInput;
	this->numAttsOutput=numAttsOutput;
	pthread_create(&thread1,NULL,projectWorker,(void*)this);
}



void Project::WaitUntilDone () {
	pthread_join (thread1, NULL);
}

void Project::Use_n_Pages (int runlen) {
	runlen=10;
}
//################################################################################
//###############################################################################

void* sumWorker(void *obj)
{
	Sum *sumobj=(Sum*)obj;
	Type datatype;
	Record fetchme;
	int intresult,fintresult;
	double doubleresult,fdoubleresult;
    fintresult=0;
    fdoubleresult=0.0;
    //cout <<setprecision(20);
	while(sumobj->inputPipe->Remove(&fetchme))
	{
	   datatype=sumobj->computeMe->Apply(fetchme,intresult,doubleresult);
	   if(datatype == Int)
		   fintresult+=intresult;
	   else
	   {
           fdoubleresult+=doubleresult;
           //cout<<doubleresult<<endl;
	   }
	}

	char buf[50];
	Attribute attb;
	if(datatype == Int)
	{
		sprintf(buf,"%d|",fintresult);
		attb.myType=Int;
	}
	else
	{
	    attb.myType=Double;
		sprintf(buf,"%.22g|",fdoubleresult);
	}

	attb.name="dummy";

	Schema sch("out_sch",1,&attb);
	Record finalRec;
	finalRec.ComposeRecord(&sch,buf);
	sumobj->outputPipe->Insert(&finalRec);
    sumobj->outputPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	inputPipe=&inPipe;
	outputPipe=&outPipe;
	this->computeMe=&computeMe;
	pthread_create(&thread1,NULL,sumWorker,(void*)this);
}



void Sum::WaitUntilDone () {
	pthread_join (thread1, NULL);
}

void Sum::Use_n_Pages (int runlen) {
	runlen=10;
}
//###################################


void* WriteOutWorker(void *obj)
{
	WriteOut *wrtoutobj=(WriteOut*)obj;
	Record fetchme;
	while(wrtoutobj->inputPipe->Remove(&fetchme))
	{
		    int n = wrtoutobj->mySchema->GetNumAtts();
			Attribute *atts = wrtoutobj->mySchema->GetAtts();
			for (int i = 0; i < n; i++) {
				fputs(atts[i].name,wrtoutobj->outFile);
				fputs(": ",wrtoutobj->outFile);
			    int pointer = ((int *) fetchme.bits)[i + 1];
			    fputs("[",wrtoutobj->outFile);
				if (atts[i].myType == Int) {
					int *myInt = (int *) &(fetchme.bits[pointer]);
					char str[100];
					sprintf(str, "%d", *myInt);
					fputs(str,wrtoutobj->outFile);
				} else if (atts[i].myType == Double) {
					double *myDouble = (double *) &(fetchme.bits[pointer]);
					char str[100];
					sprintf(str,"%g", *myDouble);
					fputs(str,wrtoutobj->outFile);
				} else if (atts[i].myType == String) {
					char *myString = (char *) &(fetchme.bits[pointer]);
					fputs(myString,wrtoutobj->outFile);
				}
				fputs("]",wrtoutobj->outFile);
				if (i != n - 1) {
					fputs(", ",wrtoutobj->outFile);
				}
			}
			fputs("\n",wrtoutobj->outFile);
	}//EndWhile
    fclose(wrtoutobj->outFile);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	inputPipe=&inPipe;
	this->outFile=outFile;
	this->mySchema=&mySchema;
    pthread_create(&thread1,NULL,WriteOutWorker,(void*)this);
}



void WriteOut::WaitUntilDone () {
	pthread_join (thread1, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
	runlen=10;
}

//   Duplicate Removal   //

// ###################################################################################################
// ###################################################################################################


void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	inputPipe=&inPipe;
	outputPipe=&outPipe;
	runlen=10;
	this->mySchema=&mySchema;
    pthread_create(&thread1,NULL,DupremWorker,(void*)this);
}

void* DupremWorker(void *obj)
{
	DuplicateRemoval *drobj=(DuplicateRemoval*)obj;
	Record curRec,prevRec;
	ComparisonEngine compEng;
	Pipe *sortedPipe=new Pipe(1000);
	OrderMaker sortorder(drobj->mySchema);
	BigQ *bq = new BigQ(*(drobj->inputPipe),*(sortedPipe),sortorder, drobj->runlen);
	sortedPipe->Remove(&prevRec);
	while (sortedPipe->Remove(&curRec)) {
			if (compEng.Compare(&prevRec, &curRec, &sortorder)) {
				drobj->outputPipe->Insert(&prevRec);
				prevRec.Consume(&curRec);
			}
	}
		drobj->outputPipe->Insert(&prevRec);
        drobj->outputPipe->ShutDown();
}


void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread1, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
	runlen=10;
}
// ###############################################################################################

void * groupbyWorker(void *obj)
{
	//cout<<"Inside GroupBy";
	GroupBy *gbyobj=(GroupBy*)obj;
	Record prevRec,curRec;
	ComparisonEngine compEng;
	int intresult,fintresult;
	Type datatype;
	double doubleresult,fdoubleresult;
	fintresult=0;
	fdoubleresult=0.0;
	Pipe *sortedPipe=new Pipe(1000);
    OrderMaker *sortorder=gbyobj->groupAtts;
    //cout<<"Before Groupby BiGQ \n";
	BigQ *bq = new BigQ(*(gbyobj->inputPipe),*(sortedPipe),*sortorder, gbyobj->runlen);
	//cout<<"After GroupBy BigQ\n";
	int numAttsRight,*attsToKeep,*attsToKeepRight,numAttsToKeep,numAttsInRecord;
	numAttsRight=gbyobj->groupAtts->numOfAttsb();
	attsToKeepRight=gbyobj->groupAtts->attsList();
	numAttsToKeep = numAttsRight + 1;
	attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0;
	for(int j=1; j<numAttsToKeep; j++)
		 attsToKeep[j] = attsToKeepRight[j-1];

	if(sortedPipe->Remove(&prevRec))
	{
		datatype=gbyobj->computeMe->Apply(prevRec,intresult,doubleresult);
		if(datatype == Int)
			    fintresult+=intresult;
		else
			    fdoubleresult+=doubleresult;
		//cout<<"doubleresult="<<doubleresult<<"\n";
		numAttsInRecord = prevRec.numOfAttInRecord();
	}

	while (sortedPipe->Remove(&curRec)) {
	            if (compEng.Compare(&prevRec, &curRec, sortorder)) {
	            	    char buf[50];
	            		Attribute attb;
	            		if(datatype == Int)
	            		{
	            			sprintf(buf,"%d|",fintresult);
	            			attb.myType=Int;
	            		}
	            		else
	            		{
	            		    attb.myType=Double;
	            			sprintf(buf,"%.22g|",fdoubleresult);
	            		}

	            		attb.name="dummy";

	            		Schema sch("out_sch",1,&attb);
	            		Record finalRec,tempRec;
	            		tempRec.ComposeRecord(&sch,buf);
	            		finalRec.MergeRecords(&tempRec,&prevRec,1,numAttsInRecord,attsToKeep,numAttsToKeep,1);
	            		gbyobj->outputPipe->Insert(&finalRec);
	            		datatype=gbyobj->computeMe->Apply(curRec,intresult,doubleresult);
	            		fintresult=0;
	                    fdoubleresult=0.0;
	            	    if(datatype == Int)
	            			     fintresult+=intresult;
	            		else
	            			     fdoubleresult+=doubleresult;
					    prevRec.Consume(&curRec);

	            }
	            else
	            {
	            	intresult=0;
	            	doubleresult=0;
	                datatype=gbyobj->computeMe->Apply(curRec,intresult,doubleresult);
	                if(datatype == Int)
	            		    fintresult+=intresult;
	                else
	            	        fdoubleresult+=doubleresult;
	            }
		} //End While

	      char buf[50];
		  Attribute attb;
		  if(datatype == Int)
		  {
		      sprintf(buf,"%d|",fintresult);
		      attb.myType=Int;
		  }
		  else
		  {
		       attb.myType=Double;
		       sprintf(buf,"%.22g|",fdoubleresult);
		   }

		  attb.name="dummy";
		  Schema sch("out_sch",1,&attb);
		  Record finalRec,tempRec;
		  tempRec.ComposeRecord(&sch,buf);
		  finalRec.MergeRecords(&tempRec,&prevRec,1,numAttsRight,attsToKeep,numAttsToKeep,1);
		  gbyobj->outputPipe->Insert(&finalRec);
		  gbyobj->outputPipe->ShutDown();
}



void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	inputPipe=&inPipe;
	outputPipe=&outPipe;
	this->groupAtts=&groupAtts;
	this->computeMe=&computeMe;
	 pthread_create(&thread1,NULL,groupbyWorker,(void*)this);
}

void GroupBy::WaitUntilDone () {
	pthread_join (thread1, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
	this->runlen=runlen;
}

//################################################################################################

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL    = &inPipeL;
	this->inPipeR    = &inPipeR;
	this->outPipe    = &outPipe;
	this->selOP      = &selOp;
	this->literal    = &literal;
	pthread_create( &thread, NULL, workerFunc, (void*)this );
}

void Join::WaitUntilDone (){
	pthread_join (thread, NULL);
}


void Join:: Use_n_Pages (int n){
	this-> runlen = n;
}

void* Join::workerFunc( void *ob ){
	//cout<<"Inside Join Worker Thread Start"<<endl;
	Join *jo = (Join* )ob;
	OrderMaker left, right;
	int val = jo->selOP->GetSortOrders(left,right);
	if( val != 0 ){
		// Do SortMerge
		sortMergeJoin( ob, left, right );
	}
	else {
		// Do nested join
		nestedJoin( ob );
	}
	//cout<<"Inside Join Worker Thread END"<<endl;
}

void Join::nestedJoin( void *ob ){
	//cout<<"Inside Nested Join START"<<endl;
	ComparisonEngine comp;
	Record lRecord, rRecord;
	Join *jo = (Join *)ob;
	DBFile temp;
	temp.Create("jointemp.bin", heap, NULL );
	jo->inPipeR->Remove(&rRecord);
	jo->inPipeL->Remove(&lRecord);

	// Get the parameters for the merge record functions
	int numAttsLeft     =  lRecord.numOfAttInRecord();
	int numAttsRight    =  rRecord.numOfAttInRecord();
	int numAttsToKeep   =  numAttsLeft + numAttsRight;
	int* attsToKeep     =  new int[numAttsToKeep];

	for(int i =0;i<numAttsLeft;i++)
		attsToKeep[i] = i;
	int startOfRight = numAttsLeft;
	for(int i=numAttsLeft; i<(numAttsRight+numAttsLeft);i++)
		attsToKeep[i] = i-numAttsLeft;

	// Write the records of right pipe to a DBfile
	do{
		temp.Add(rRecord);
	}while( jo->inPipeR->Remove(&rRecord));
	temp.MoveFirst();

	// Now do the nested join
	do{
		while( temp.GetNext(rRecord)){
			Record mergeRecord;
			mergeRecord.MergeRecords(&lRecord, &rRecord, numAttsLeft, numAttsRight, attsToKeep,
					numAttsToKeep, startOfRight);
			jo->outPipe->Insert(&mergeRecord);
		}
		temp.MoveFirst();
	}while(jo->inPipeL->Remove( &lRecord ));
	// Clean up
	temp.Close();
	remove("jointemp.bin");
    remove("jointemp.bin.meta");

    jo->inPipeL->ShutDown();
	jo->inPipeR->ShutDown();
	jo->outPipe->ShutDown();
	//cout<<"Inside Nested Join END"<<endl;
}


void Join::sortMergeJoin( void *ob, OrderMaker &leftO, OrderMaker &rightO){
	//cout<<"Inside SORT MERGE Join START"<<endl;
	Join *jo = (Join *)ob;
	ComparisonEngine comp;
	Record lRecord, rRecord;
	DBFile temp;
	//temp.Create("jointemp.bin", heap, NULL );
	Pipe leftPipe(1000), rightPipe(1000);
	int numAttsLeft, numAttsRight, numAttsToKeep, startOfRight;
	int* attsToKeep;
	int runLen = jo->runlen;
	//runLen = 1;
	//cout<<"Run Length is "<<runLen<<endl;
    //cout<<"Reached Here \n";
	BigQ bq1( *(jo->inPipeL), leftPipe,  leftO, runLen );
	BigQ bq2( *(jo->inPipeR), rightPipe, rightO, runLen );
	int left  = leftPipe.Remove( &lRecord );
	int right = rightPipe.Remove( &rRecord );
	if( left && right ){
		// Get the parameters for the merge record functions
		numAttsLeft     = lRecord.numOfAttInRecord();
		numAttsRight    = rRecord.numOfAttInRecord();
		numAttsToKeep   = numAttsLeft + numAttsRight;
		attsToKeep     = new int[numAttsToKeep];

		for(int i =0;i<numAttsLeft;i++)
			attsToKeep[i] = i;
		startOfRight = numAttsLeft;
		for(int i=numAttsLeft; i<(numAttsRight+numAttsLeft);i++)
			attsToKeep[i] = i-numAttsLeft;
	}

	int val   = comp.Compare(&lRecord, &leftO, &rRecord, &rightO );
	while( left && right ){
		if( val == 1){ 			// when left record is bigger than right
			//cout<<" Inside Right smaller"<<endl;
			right = rightPipe.Remove( &rRecord );
			val   = right == 1 ? comp.Compare(&lRecord, &leftO, &rRecord, &rightO) : 2;
		}
		else if( val == -1 ){   // when left record is smaller than right
			//cout<<" Inside Right greater"<<endl;
			left  = leftPipe.Remove( &lRecord );
			val   = left == 1  ? comp.Compare(&lRecord, &leftO, &rRecord, &rightO) : 2;
		}
		else if( val == 0){
			//cout<<"Records must be joined"<<endl;
			temp.Create("jointemp.bin", heap, NULL );
			// Make copy of right record
			Record *rightCopy = new Record;
			rightCopy->Copy(&rRecord);
			// Iterate over the right pipe till the left and right record matches
			// And keep adding the matched right records to the DBFile
			while( right && (val == 0 )){
				//cout<<"Adding to heap file"<<endl;
				temp.Add(rRecord);
				right = rightPipe.Remove( &rRecord );
				val   = right == 1 ? comp.Compare(&lRecord, &leftO, &rRecord, &rightO) : 2;
			}
			// Move the file to the beginning to prepare for merge
			temp.Close();
			temp.Open("jointemp.bin");
			temp.MoveFirst();

			// Actual Merge
			Record tempRight ;
			int flag = 0;
			do{
				//cout<<"Inside outer loop left"<<endl;
				while( temp.GetNext( tempRight ) != 0 ){
				//cout<<"MERGING"<<endl;
					Record mergedRecord;
					mergedRecord.MergeRecords( &lRecord, &tempRight, numAttsLeft, numAttsRight, attsToKeep,
						numAttsToKeep, startOfRight	);
					jo->outPipe->Insert( &mergedRecord );
				}
				temp.MoveFirst();
				left  = leftPipe.Remove( &lRecord );
				flag  = left == 1 ? comp.Compare( &lRecord, &leftO, rightCopy, &rightO) : 2;
			}while( left && (flag == 0));
			if( left && right ){
				val = comp.Compare(&lRecord, &leftO, &rRecord, &rightO);
			}
			delete rightCopy;
		}
		else break;   // one or both of the input pipes have exhausted
	}
	//Again some cleaning work to be done
	temp.Close();
	remove("jointemp.bin");
	remove("jointemp.bin.meta");
	leftPipe.ShutDown();
	rightPipe.ShutDown();
	jo->outPipe->ShutDown();
	//cout<<"Inside SORT MERGE Join END"<<endl;
}
