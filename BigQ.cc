#include "BigQ.h"
#include<cassert>
/**
 * Constructor for RecordWrapper to create a copy of record from the record specified and point to OrderMaker object passed.
 */
RecordWrapper::RecordWrapper(Record* recordToCopy,OrderMaker* sortOrder)
{
	(this->record).Copy(recordToCopy);
	this->sortOrder = sortOrder;
}
/**
 * @method compareRecords compares records passed as RecordWrapper.
 * @returns 1 or 0 based on sort Order
 */
int RecordWrapper::compareRecords(const void* rw1, const void *rw2)
{
	RecordWrapper* recWrap1 = ((RecordWrapper*) rw1);
	RecordWrapper* recWrap2 = ((RecordWrapper*) rw2);
	OrderMaker* sortOrder =  ((RecordWrapper*) rw1)->sortOrder;
	ComparisonEngine compEngine;
	return (compEngine.Compare(&(recWrap1->record),&(recWrap2->record),sortOrder) < 0);
}
/**
 * Constructor for ComparisonClass where object of ComparisonEngine is instantiated which will be used by priority queue for comparing 
 * records in priorityQueue.
 */
ComparisonClass:: ComparisonClass()
{
	compEngine = new ComparisonEngine();
}
/**
 * Operator overloading for comparison in priorityQueue.
 */
bool ComparisonClass:: operator()(const pair<RecordWrapper*,int> &lhs, const pair<RecordWrapper*,int> &rhs)
{
	RecordWrapper* rw1 = lhs.first;
	RecordWrapper* rw2 = rhs.first;
	OrderMaker* sortOrder = rw1->sortOrder;
	return (compEngine->Compare(&(rw1->record),&(rw2->record),sortOrder) > 0);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): inputPipe(in),outputPipe(out),sortOrder(sortorder) {
	// read data from in pipe sort them into runlen pages
	runLength = runlen;
	
	//Start thread for internal sorting.
	int rc = pthread_create(&worker,NULL,workerFunc,(void*)this);
	if(rc){
		cerr<<"Not able to create worker thread"<<endl;
		exit(1);
	}
}

BigQ::~BigQ () 
{
	//wait for the thread to complete.
	pthread_join(worker, NULL);
}
/*
 * @method workerFunc which starts as a thread and splits it work amond other functions. Function first invokes createRuns which creates runs and 
 * adds it to File element passed as argument. Also createRuns method calls sortAndCopyToFile method which does internal sorting of run length 
 * number of pages and adds it to File. This method also initialises runLengthInfo vector which is passed to it by reference. Method then 
 * invokes mergeRunsFromFile which merges runs file as per runLengthInfo in vector. 
 */
void* workerFunc(void *bigQ)
{
	BigQ *bq  = (BigQ*) bigQ;
	//create a temporary file which will be used for storing temporary results and each of runs.
	File* file = new File();
	file->Open(0,BIGQTEMPFILE);
	Pipe& in = bq->inputPipe;
	Pipe& out = bq->outputPipe;
	int runlen = bq->runLength;

	//list of start and last page for each run.
	vector<pair<off_t,off_t> > runLengthInfo;

	//create file of sorted runs.
	createRuns(runlen,in,file,(bq->sortOrder),runLengthInfo); 

	//once a file is created of sorted runs merge each of the run.
	mergeRunsFromFile(file,runlen,out,(bq->sortOrder),runLengthInfo);
	//Close the temporary file used for sorting and storing temporary results.
	file->Close();

	//Remove the temporary file used for storing runs.
	remove(BIGQTEMPFILE);
	out.ShutDown ();
}
/**
 * @method createRuns to create a file of sorted runs and number of runs.
 * @param runlen integer to specify runlength of pages.
 * @param file File pointer to file where each of the runs needed to be added.
 * @param sortOrder Object of class OrderMaker used to specify ordering of records.
 * @param runLengthInfo list of run start and end page for each runs which is initialised here.
 *
 */
void createRuns(int runlen, Pipe& in, File *file, OrderMaker& sortOrder, vector<pair<off_t,off_t> >& runLengthInfo)
{
	Record* currentRecord = new Record();
	Page page;
	vector<RecordWrapper*> list;
	RecordWrapper *tempWrapper;
	int numPages = 0;
	while(in.Remove(currentRecord) !=0)
	{
		//create a copy of record in recordWrapper which will be pushed to List for sorting.
		tempWrapper = new RecordWrapper(currentRecord,&sortOrder);
		if(page.Append(currentRecord) == 0)
		{
			//when unable to add to page implies page is full so increament number of pages.
			numPages++;
			//check if number of pages read is equivalent to number of pages specified in runlength.
			if(numPages == runlen)
			{
				sortAndCopyToFile(list,file,runLengthInfo);
				//clean the list for next run.
				list.clear();
				//set numPages to 0 again.
				numPages =0;
			}
			page.EmptyItOut();
			page.Append(currentRecord);
		}
		list.push_back(tempWrapper);
	}
	delete currentRecord;
	//If there are more records on list which needs to be written on disk
	if(list.size() >0)
		sortAndCopyToFile(list,file,runLengthInfo);
	list.clear();
	in.ShutDown();
}
/**
 * @method sortAndCopyToFile to sort the given vector list of records and then append it to end of file. Method maintains a static count of
 * pages written to file which is used as offset to add page to file.
 * @param List of type vector<RecordWrapper*> a reference to list of Records wrapped in RecordWrapper.
 * @param File Pointer to File to add pages to end of the same file.
 * @param runLength vector of pairs which is passed by reference and initialised here with one entry for each run.
 *
 */
void sortAndCopyToFile(vector<RecordWrapper*>& list,File* file,vector<pair<off_t,off_t> >& runLengthInfo)
{
	static off_t nextPageMarker = 0;
	Page page;
	off_t initialPage = nextPageMarker;
	//First Sort the vector and then add the records in sorted order in file.
	sort(list.begin(), list.end(), RecordWrapper::compareRecords);
	//Write the sorted records to file.
	for(std::vector<RecordWrapper*>::iterator iter = list.begin() ; iter != list.end(); ++iter)
	{
		Record tempRec  =  (*iter)->record;
		if(page.Append(&tempRec) == 0)
		{
			//Add the full page to file and increament the marker.
			file->AddPage(&page,nextPageMarker);
			//Increament the next Page Marker.
			nextPageMarker++;
			//Empty the page and write the failed Record.
			page.EmptyItOut();
			page.Append(&tempRec);
		}
	}
	
    //Add the last page to file and increameant counter.
	file->AddPage(&page,nextPageMarker);
	nextPageMarker++;
	off_t lastPage = nextPageMarker-1;
	
    //pair to store first and last page of each run.
	pair<off_t,off_t> runLengthPair(initialPage,lastPage);
	runLengthInfo.push_back(runLengthPair);
	page.EmptyItOut();
}
/**
 * @method mergeRunsFromFile to merge runs from file specified and write it to output pipe. Meta data for each run is read from runLengthInfo
 * vector. which stores start and end page of each run. Also size of runLength Info specifies number of runs.
 * @param File* pointer to file, from where runs needs to be read.
 * @param runLength integer to specify length of each run.
 * @param out output Pipe for consumer to read records, all records are pushed from priority queue to this pipe.
 * @param sortOrder object of OrderMaker class used to sort records.
 * @param runLengthInfo list of pairs, each pair has start and end page for each run.
 */
void mergeRunsFromFile(File* file, int runLength, Pipe& out, OrderMaker& sortOrder, vector<pair<off_t,off_t> >& runLengthInfo)
{
	int numRuns = runLengthInfo.size();
	std::priority_queue<pair<RecordWrapper*,int>, std::vector<pair<RecordWrapper*,int> >,ComparisonClass> priorityQueue;

	//Array of Pages to keep hold of current Page from each of run.
	Page* pageBuffers = new Page[numRuns];
	
    //initialise each page with corresponding page in File.
	vector<off_t> offset;
	
	//initialise offset array to keep track of next page for each run.
	//Initialise each of the Page Buffers with the first page of each run.
	
	for(int i=0;i<numRuns;i++)
	{
		offset.push_back(runLengthInfo[i].first);
		
        //Get the Page for offset in Buffer.
		file->GetPage(&(pageBuffers[i]),offset[i]);
		
        //For each of the page get the first record in priority queue.
		Record* record = new Record();
		pageBuffers[i].GetFirst(record);
		RecordWrapper* rWrap = new RecordWrapper(record,&sortOrder);
		priorityQueue.push(make_pair(rWrap,i));
	}
        
	//while priority queue is not empty keeping popping records from Priority Queue and write it to pipe.
	RecordWrapper* recordWrap;
	int count = 0;
	while(!priorityQueue.empty())
	{
		count++;
		pair<RecordWrapper*, int> topPair = priorityQueue.top();
		recordWrap = topPair.first;
		int runNum = topPair.second;
		
		priorityQueue.pop();
		Record * record = &(recordWrap->record);
		//Write record to output pipe.
		out.Insert(record);
		//Get the next record from Record Num and add it to priorityQueue.
		Record* recordToInsert = new Record();
		if(pageBuffers[runNum].GetFirst(recordToInsert) == 0)
		{
			//if no records were found from the page try to get the next page if page exists in the same run.
			off_t currOffset = offset[runNum];
			//only if there are more pages in run read the next page else skip.
			if(currOffset < runLengthInfo[runNum].second)
			{
				pageBuffers[runNum].EmptyItOut();

				//increament offset after reading page from current offset for run
				offset[runNum]++;	

				file->GetPage(&pageBuffers[runNum],offset[runNum]);

				assert(pageBuffers[runNum].GetFirst(recordToInsert) != 0);
			}
			else
			{
				//No more Pages to read
				continue;
			}
		}//no else block required since it has no more pages to read.
		//Insert new Record in priorityQueue.
		RecordWrapper* nextRecordWrap = new (std::nothrow) RecordWrapper(recordToInsert,&sortOrder);
		priorityQueue.push(make_pair(nextRecordWrap,runNum));
	}

}
