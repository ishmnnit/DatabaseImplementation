
#include <assert.h>
#include <iostream>
#include <stdlib.h>
#include "Sort.h"
#include <fstream>
#include <string.h>


Sort::Sort () {
	//Initialize current record pointer to point to the first record in the file.

	mode = 'R';
	in = new Pipe(100);
	out = new Pipe(100);
	sortorder = new OrderMaker();
	CurrentRecord.PageNumber = 0;
	CurrentRecord.PageOffset = 0;
	DirtyPageAdded = true;
	CurrentFile = new File();
	Adder = new Page();
	start = CurrentRecord.PageNumber;
	cnfsort = new OrderMaker();
	query = NULL;
	first = true;
	globalcount = 0;
	insertoffset = 0;
	count =0;

}

int Sort::Create (char *f_path,void *startup) {
	//Use the File Class from File.h to store the actual database data.
	//Any meta-data needed can be stored in an associated file as <filename>.header
	//ftype is either heap, sorted or tree - enum defined in DBFile.h
	//f_path is the location
	//Returns 1 on success and 0 on failure.
	//cout<< " in create of sort "<<endl;
	if(query != NULL){
		delete query;
		query = NULL;
	}
	mode = 'R';
	//cout<< " After delete query "<< f_path<<endl;
	int Returnvalue=0;
	CurrentFile->Open(0, f_path);
	strcpy(filepath, f_path);
	//cout<< " After creating file "<< f_path<< endl;
	Returnvalue = 1;
	return(Returnvalue);
	//*startup is a dummy parameter not used in Assignment 1
}

void Sort::Load (Schema &f_schema, char *loadpath) {
	//Bulk loads the DBFile instance from a text file, appending new data to it using the SuckNextRecord function
	//loadpath is the name of the data file to bulk load
	if(query != NULL){
		delete query;
		query = NULL;
	}
	Record *temp = new Record();
	FILE *tablefile = fopen(loadpath, "r");
	while(temp->SuckNextRecord(&f_schema, tablefile) == 1){
		Add(*temp);
		temp = new Record();
	}
	fclose(tablefile);
}

int Sort::Open (char *f_path) {
	if(query != NULL){
		delete query;
		query = NULL;
	}
	//Function assumes that the DBFile has been created and closed before.
	//If any other parameters are needed, they should be written into the auxillary .header file before which should also be opened at startup
	//Returns 1 on success and 0 on failure
	CurrentFile->Open(1, f_path);
	ifstream readmetafile;
	string reader;
	string type;
	bool first = true;
	//cout << " in sort OPEN " <<endl;
	readmetafile.open("metafile");
	if(readmetafile.good()){
		getline(readmetafile, file_path);
		//strcpy(filepath, reader);
		getline(readmetafile, reader);
		type = reader;
		//cout<< " type " <<type <<endl;
		getline(readmetafile, reader);
		runlength = atoi(reader.c_str());
		//cout<< " RUNL " <<runlength <<endl;
		getline(readmetafile, reader);

		sortorder->numAtts = atoi(reader.c_str());
		//cout << " numatts " << sortorder->numAtts<< " " << reader<< endl;
		for(int i=0;i<sortorder->numAtts;i++)
		{
			getline(readmetafile, reader);
			sortorder->whichAtts[i] = atoi(reader.c_str());
			//cout << " whichatts " << sortorder->whichAtts[i]<< " " << reader<< endl;
		}

		getline(readmetafile, reader);  //to get semi colon tht separates
		for(int i=0;i<sortorder->numAtts;i++)
		{
			getline(readmetafile, reader);
			if(reader=="0")
				sortorder->whichTypes[i] = Int;
			else if(reader=="1")
				sortorder->whichTypes[i] = Double;
			else if(reader=="2")
				sortorder->whichTypes[i] = String;
			//cout << " whichtype " << sortorder->whichTypes[i]<< " " << reader<< endl;
		}

	}
	readmetafile.close();
}

void Sort::MoveFirst () {	//Forces 'he current pointer in DBFile to point to the first record in the file
	if(query != NULL){
		delete query;
		query = NULL;
	}
	if(mode=='W')
	{
		merge();
	}
	//Forces the current pointer in DBFile to point to the first record in the file
	CurrentRecord.PageNumber = 0;
	CurrentRecord.PageOffset = 0;
}

int Sort::Close () {
	//Simply closes the file. Returns 1 on success and 0 on failure
	//cout<<" in sort close "<<mode <<endl;
	if(query != NULL){
		delete query;
		query = NULL;
	}

	if(mode=='W')
	{
		//cout<<"Calling merge in close"<<endl;
		merge();
	}
	CurrentFile->Close();

}


void Sort::Add (Record &rec) {
	//For the HeapFile implementation, this function simply adds the new record to the end of the file
	//Function should consume rec, so that once rec has been put into the file it cannot be used again
	//cout<< " IN add " << mode <<endl;
	if(mode != 'W')
	{
		//cout<<" create new bigq " <<endl;
		mode = 'W';

		in->Insert(&rec);
		globalcount++;
		bigq = new BigQ(*in,*out,*sortorder,runlength);

	}
	else if(mode == 'W'){
		//cout<< " in ADD " <<mode <<endl;
		in->Insert(&rec);
		globalcount++;
	}
	/*else{
		perror("Something is wrong with mode");
		assert(1 == 2);
	}*/
}

void Sort::AddPage(){
	//Function to add a page into the file
}

int Sort::GetNext (Record &fetchme) {
	//Returns the record next to which the current pointer is pointing, and the current pointer is incremented
	//Returns 0 if the current pointer is pointing to the last record.
	//This function should also handle dirty reads. This happens when CurrentRecord.PageNumber is equal to CurrentFile.GetLength() - 1
	if(query != NULL){
		delete query;
		query = NULL;
	}
	if(mode=='W')
	{
		merge();
	}
	int Returnvalue=0;
	int j=0;
	off_t i=0;

	if((CurrentRecord.PageOffset == 0) && (CurrentRecord.PageNumber == 0)){ //Fetch new page if the page offset is 0 which means a new page needs to be fetched


		Adder = new Page();
		Adder->EmptyItOut();
		CurrentFile->GetPage(Adder, CurrentRecord.PageNumber);  //get 1st page
	}


	j = Adder->GetFirst(&fetchme);

	//If no record has been returned, increment page number, set page offset to 0, and read new page and first record
	if(j == 0){
		CurrentRecord.PageNumber++;
		CurrentRecord.PageOffset=0;
		if(CurrentFile->GetLength() - 1== CurrentRecord.PageNumber)
			return(Returnvalue);
		if(((CurrentFile->GetLength() - 2) == CurrentRecord.PageNumber)&&(!DirtyPageAdded)){
			//Trying to access the CurrentPage which hasnt been pushed into the file yet
			//So first push it into the file
			AddPage();
			DirtyPageAdded = true;
		}
		//CurrentPage.EmptyItOut();
		Adder = new Page();
		Adder->EmptyItOut();
		//CurrentFile.GetPage(&CurrentPage, CurrentRecord.PageNumber);
		CurrentFile->GetPage(Adder, CurrentRecord.PageNumber);

		assert(Adder != NULL);
		j = Adder->GetFirst(&fetchme);

	}
	if(j == 1){
		//Put the next record in the location where it has been asked
		CurrentRecord.PageOffset++;
		Returnvalue = 1;
	}
	return(Returnvalue);
}

int Sort::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//Returns the next record in the file which is accepted by the selection predicate compared to the literal.
	//The literal is generated when the parse tree for the CNF is processed

	if(mode=='W')
	{
		merge();
	}
	int returnvalue = 0;
	//cout<< " SORT getnext with cnf "<<sortorder->whichAtts[0]<<endl;
	//examine CNF to create Ordermaker
	OrderMaker *cnforder;
	//OrderMaker *sortordcpy = &sortorder;
	if(query == NULL){
		//cout<< " SORT getnext with cnf 1 "<<endl;
		query = new OrderMaker();
		cnforder = new OrderMaker();
		GetCNFSortOrder(*cnforder, cnf);
		OrderMaker *copy = new OrderMaker();
		*copy = *sortorder;
		int k=0;
		//cout<< " SORT getnext with cnf x "<<sortorder->whichAtts[0]<< " " << cnforder->numAtts << " " << sortorder->numAtts<<endl;
		//cnf.GetSortOrders(*query,*copy);
		cnf.GetQuerySortOrders(*query, *copy);

	}

	if(query->numAtts!=0)
	{
		//cout<<" binary search "<< endl;
		returnvalue = binarysearch(query,fetchme,literal,cnf);
	}
	return returnvalue;
}


int Sort::binarysearch(OrderMaker *query,Record &fetchme, Record &literal, CNF &cnf)
{
	ComparisonEngine compare;
	start = CurrentRecord.PageNumber;
	end = CurrentFile->GetLength()-1;
	mid = (start + end)/2;
	int lastpagefound = mid;
	Page *searchPage= new Page();
	Record *temp = new Record();
	int retval = 0;
	int ret = 1;
	int cnfreturn;
	off_t oldmid = 100000;
	bool found = false;
	bool firstfound = false;
	//	CurrentFile->GetPage(searchPage,mid);		//check mid page
	int i=0;
	//cout << " BINARY  SEARCH "<< start << " " <<end << " " << mid<<" "<<CurrentFile->GetLength()<<endl;
	while(ret != 0){
		//cout<<"Calling linear scan on page "<<mid<<" start "<<start<<" end "<<end<<endl;
		ret = linearscan(mid,literal,query,fetchme,cnf);
		if((oldmid == mid)&&(ret != 0)){
			break;
		}
		else{
			oldmid = mid;
		}
		//int x;
		//cin>>x;
		//cout<<ret;
		if(ret == 2){
			//cout<<"Cannot find a match"<<endl;
			break;
		}
		else if(ret == 1){
			//cout<<"RETURNING 1///////////////////////"<<endl;
			end = mid;
			mid = (start + end)/2;
			if(CurrentRecord.PageNumber > mid){
				//cout<<"Cannot find a match"<<endl;
				break;
			}
		}
		else if(ret == -1){
			//cout<<"RETURNING -1///////////////////////"<<endl;
			start = mid;
			mid = (start + end)/2;
			if(CurrentRecord.PageNumber < mid){
				//cout<<"Cannot find a match"<<endl;
				break;
			}
		}
		else if(ret == 0){		//found
			retval = 1;
		}
		else if(ret == -2){	//not found anything
			retval = 0;
			break;
		}
	}
	return retval;
}
int Sort::linearscan(off_t Pagenum,Record &literal,OrderMaker *query,Record &fetchme, CNF &cnf)
{
	ComparisonEngine compare;
	Page *searchPage = new Page();
	int prevq = -2; //-2 uninitialized, -1 means temp is less than literal, 0 is equal, +1 means temp is greater than literal
	//+2 will mean, prevq was -1 before and now it is +1 which means query cant find the record
	int currentq = -2;
	int cnfcompare = 2;
	int ret = 0;
	int pageoffset = 0;
	OrderMaker *cnforder = new OrderMaker();
	int numatts = GetCNFSortOrder(*cnforder, cnf);
	//cout<<"Cnf checks "<<numatts<<endl;

	Record *temp = new Record();

	CurrentFile->GetPage(searchPage, Pagenum);
	while(searchPage->GetFirst(temp)){

		if((pageoffset < CurrentRecord.PageOffset)&&(Pagenum == CurrentRecord.PageNumber)){
			pageoffset++;
			continue;
		}
		currentq = compare.Compare(temp, sortorder, &literal, query);

		//cout<<"Currentq is "<<currentq<<endl;
		if(currentq == 0){
			//cnfcompare = compare.Compare(temp, sortorder, &literal, cnforder);
			cnfcompare = compare.Compare(temp,&literal,&cnf);
			//cout<<"cnf compare is "<<cnfcompare<<endl;
			if(cnfcompare == 1){
				count++;
				//cout<<"Comparing using cnf"<<endl;
				//cout<<"crpage offset cnfcomp  "<<count << " " <<CurrentRecord.PageOffset<<" "<<Pagenum<<" "<<CurrentRecord.PageNumber<<endl;
				if((CurrentRecord.PageOffset == 0)&&(Pagenum-1 >= 0)){			//if 1st rec matches go to prev page
					Pagenum--;
					ret = linearscan(Pagenum,literal,query,*temp,cnf);
					if(ret != 0){
						Pagenum++;
						CurrentRecord.PageOffset = 1;
						CurrentFile->GetPage(searchPage, Pagenum);
						searchPage->GetFirst(&fetchme);
						//cout<<"Return 0 in the page which had first record hit"<<endl;
						return 0;
					}
					else if(ret == 0){
						//cout<<"Return 0 in the page which was previous to the page which had first hit"<<endl;
						fetchme.Copy(temp);
						return 0;
					}
				}
				else{
					//cout<<"Return 0 in the current page"<<endl;
					CurrentRecord.PageNumber = Pagenum;
					CurrentRecord.PageOffset = pageoffset+1;
					fetchme.Copy(temp);
					return 0;
				}
			}
			//cout<<"Currentq "<<currentq<<endl;
		}
		else{
			if(prevq == -2){
				//cout<<"Set prevq as "<<currentq<<endl;
				prevq = currentq;
			}
			else if((prevq != currentq)&&(currentq != 0)){
				//cout<<"prevq "<<prevq<<" page offset "<<pageoffset<<endl;
				prevq = 2; //Query cannot find anything in the page
				//cout<<"currentq "<<currentq<<endl;
				return prevq;
			}
			//cout<<"Page offset is "<<CurrentRecord.PageOffset<<endl;
			//CurrentRecord.PageOffset++;
			pageoffset++;
		}
	}
	return prevq;
}

int Sort :: GetCNFSortOrder (OrderMaker &left, CNF &cnf) {

	// initialize the size of the OrderMakers
	left.numAtts = 0;
	//	right.numAtts = 0;

	// loop through all of the disjunctions in the CNF and find those
	// that are acceptable for use in a sort ordering
	for (int i = 0; i < cnf.numAnds; i++)
	{
		// if we don't have a disjunction of length one, then it
		// can't be acceptable for use with a sort ordering
		if (cnf.orLens[i] != 1) {
			continue;
		}

		// made it this far, so first verify that it is an equality check
		if (cnf.orList[i][0].op != Equals) {
			continue;
		}

		// check that operand1 is Left and operand2 is Literal
		if (cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Literal)
		{
			// get type and column-position-in-file for column
			left.whichAtts[left.numAtts] = cnf.orList[i][0].whichAtt1;
			left.whichTypes[left.numAtts] = cnf.orList[i][0].attType;
			// get type and the position in CNF
			//		right.whichAtts[right.numAtts] = cnf.orList[i][0].whichAtt2;
			//		right.whichTypes[right.numAtts] = cnf.orList[i][0].attType;
		}

		// check that operand1 is Literal and operand2 is Right
		else if (cnf.orList[i][0].operand1 == Literal && cnf.orList[i][0].operand2 == Right)
		{
			// get type and column-position-in-file for column
			left.whichAtts[left.numAtts] = cnf.orList[i][0].whichAtt2;
			left.whichTypes[left.numAtts] = cnf.orList[i][0].attType;
			// get type and the position in CNF
			//		right.whichAtts[right.numAtts] = cnf.orList[i][0].whichAtt1;
			//		right.whichTypes[right.numAtts] = cnf.orList[i][0].attType;
		}
		else
			continue;

		left.numAtts++;
		//	right.numAtts++;
	}

	return left.numAtts;

}

void Sort::merge()
{
	if(query != NULL){
		delete query;
		query = NULL;
	}
	bool Filecomplete = false;
	bool pipecomplete = false;
	TempFile = new File();
	TempFile->Open(0, "sample");
	//cout<<"A new file's length is "<<TempFile->GetLength();
	//cout<<"Before comparing"<<CurrentFile->GetLength()<<endl;
	if(CurrentFile->GetLength() == 0){
		first = true;
		//cout<<"First time"<<endl;
	}
	else{
		//cout<<"Not the first time"<<endl;
		first=false;
	}
	Page *newPage = new Page();
	off_t offset =0;


	in->ShutDown();
	//pthread_join(bigq->worker, NULL);

	bool lastpgdone = false;
	Page *insertPage = NULL;
	//cout<< " close MERGE 1 " <<globalcount<<endl;
	int ReturnValue;
	int returnval;
	int Pagereturnval;
	Record *rec = NULL;
	Record *rec1 = NULL;
	int j=0;
	if(first)
	{

		rec = new Record();
		while(out->Remove(rec)==1)
		{
			lastpgdone = false;
			//cout<<"Inside while"<<endl;
			//int ret=out->Remove(rec);
			//get record from pipe

			//cout<< " complete 1 " << j++ <<endl;
			if(insertPage == NULL){
				insertPage = new Page();
			}
			Pagereturnval =insertPage->Append(rec);
			if(Pagereturnval ==0)							//if full get a new page
			{
				lastpgdone = true;
				if(CurrentFile->GetLength() == 0){
					insertoffset = CurrentFile->GetLength();
					//cout<<"First page in first time being added"<<endl;
				}
				else{
					//cout<<"Not the first page in the first time being added"<<endl;
					insertoffset = CurrentFile->GetLength()-1;
				}
				//cout<<"Inserting at "<<insertoffset<<endl;
				CurrentFile->AddPage(insertPage,insertoffset);		//put into temp file
				//cout<<"Length now is "<<CurrentFile->GetLength();
				insertPage = new Page();
				Pagereturnval =insertPage->Append(rec);
			}
			rec = new Record();
			globalcount--;
			if(globalcount<=0){
				delete rec;
				rec = NULL;
				break;
			}
		}
		if(!lastpgdone){
			if(CurrentFile->GetLength() == 0){
				//cout<<"Last page is the First page in first time"<<endl;
				insertoffset = CurrentFile->GetLength();
			}
			else{
				//cout<<"Last page is Not the first page in first time"<<endl;
				insertoffset = CurrentFile->GetLength()-1;
			}
			//cout<<"Inserting at "<<insertoffset<<endl;
			CurrentFile->AddPage(insertPage, insertoffset);
			//cout<<"Length now is "<<CurrentFile->GetLength();
		}


		//pthread_join(bigq->worker, NULL);


		//	out->ShutDown();
		CurrentFile->Close();
		return;
	}


	//cout<<"Getting page in merge"<<endl;
	CurrentFile->GetPage(newPage,offset++);
	int i=0;
	while((!Filecomplete)&&(!pipecomplete))
	{
		i++;
		if(rec == NULL){
			rec = new Record();
			//cout<< " Getting from pipe"<<endl;
			returnval=out->Remove(rec);			//get record from pipe
			if(returnval==0){				//if empty set flag
				//cout<<"No more records in pipe"<<endl;
				rec = NULL;
				pipecomplete =true;
			}
		}
		if(rec1 == NULL){
			rec1 = new Record();
			//cout<<"Getting from File"<<endl;
			ReturnValue = newPage->GetFirst(rec1);			//get a record from file
			if(ReturnValue==0)						//if empty get a new page
			{
				if(offset < CurrentFile->GetLength()-1){
					//cout<<"offset "<<offset<<" length"<<CurrentFile->GetLength();
					CurrentFile->GetPage(newPage,offset++);
				}
				ReturnValue = newPage->GetFirst(rec1);
				if(ReturnValue ==0){
					//cout<<"No more records in File"<<endl;
					rec1 = NULL;
					Filecomplete = true;
				}
			}
		}
		if((!Filecomplete) && (!pipecomplete)){
			if((rec != NULL)&&(rec1 != NULL)){
				if(order_based_sort(rec, rec1, *sortorder))		//compare rec.. set min to insertPage
				{
					if(insertPage == NULL){
						insertPage = new Page();
					}
					Pagereturnval =insertPage->Append(rec);
					if(Pagereturnval ==0)							//if full get a new page
					{
						if(TempFile->GetLength() == 0){
							insertoffset = TempFile->GetLength();
							//cout<<"First page being inserted into temp file. Neither pipe, nor file empty "<<insertoffset<<endl;

						}
						else{
							insertoffset = TempFile->GetLength()-1;
							//cout<<"File and pipe still have contents, not first page to tempfile "<<insertoffset<<endl;

						}
						TempFile->AddPage(insertPage,insertoffset);		//put into temp file
						insertPage = new Page();
						Pagereturnval =insertPage->Append(rec);
					}
					rec = NULL;
				}
				else
				{
					if(insertPage == NULL){
						insertPage = new Page();
					}
					Pagereturnval =insertPage->Append(rec1);
					if(Pagereturnval ==0)
					{
						if(TempFile->GetLength() == 0){
							insertoffset = TempFile->GetLength();
							//cout<<"First page being inserted into temp file. Neither pipe, nor file empty "<<insertoffset<<endl;

						}
						else{
							insertoffset = TempFile->GetLength()-1;
							//cout<<"Not First page being inserted into temp file. Neither pipe, nor file empty "<<insertoffset<<endl;

						}
						TempFile->AddPage(insertPage,insertoffset);
						insertPage = new Page();
						Pagereturnval =insertPage->Append(rec1);
					}
					rec1 = NULL;
				}
			}
		}
	}
	while(Filecomplete && (!pipecomplete))			//if either one complete dump contents of other into output page
	{
		if(rec != NULL){
			if(insertPage == NULL){
				insertPage = new Page();
			}
			Pagereturnval =insertPage->Append(rec);
			if(Pagereturnval ==0)
			{
				if(TempFile->GetLength() == 0){
					insertoffset = TempFile->GetLength();
					//cout<<"File is empty, pipe has records, first page to tempfile "<<insertoffset<<endl;
				}
				else{
					insertoffset = TempFile->GetLength()-1;
					//cout<<"File is empty, pipe has records, not first page to tempfile "<<insertoffset<<endl;

				}
				TempFile->AddPage(insertPage,insertoffset);
				insertPage = new Page();
				Pagereturnval =insertPage->Append(rec);
			}
		}
		rec = new Record();
		returnval=out->Remove(rec);
		if(returnval==0){
			delete rec;
			rec = NULL;
			pipecomplete =true;
		}
	}
	while(!Filecomplete && (pipecomplete))
	{
		if(rec1 != NULL){
			if(insertPage == NULL){
				insertPage = new Page();
			}
			Pagereturnval =insertPage->Append(rec1);
			if(Pagereturnval ==0)
			{
				if(TempFile->GetLength() == 0){
					insertoffset = TempFile->GetLength();
					//cout<<"File has records, pipe empty, first page to tempfile"<<insertoffset<<endl;

				}
				else{
					insertoffset = TempFile->GetLength()-1;
					//cout<<"File has records, pipe empty, not first page to tempfile "<<insertoffset<<endl;

				}
				TempFile->AddPage(insertPage,insertoffset);
				insertPage = new Page();
				Pagereturnval =insertPage->Append(rec1);
			}
		}
		rec1 = new Record();
		ReturnValue = newPage->GetFirst(rec1);
		if(ReturnValue==0)
		{
			if(offset < CurrentFile->GetLength()-1){
				//cout<<"offset "<<offset<<" length"<<CurrentFile->GetLength();
				CurrentFile->GetPage(newPage,offset++);
			}
			ReturnValue = newPage->GetFirst(rec1);
			if(ReturnValue ==0){
				delete rec1;
				rec1 = NULL;
				Filecomplete = true;
			}
		}
	}
	if(Filecomplete && pipecomplete){
		rec = NULL;
		rec1 = NULL;
		if(insertPage != NULL){
			//last page is not full ..needs to be added to tempfile
			if(TempFile->GetLength() == 0){
				insertoffset = TempFile->GetLength();
				//cout<<"File is empty, pipe is empty, first page to tempfile "<<insertoffset<<endl;

			}
			else{
				insertoffset = TempFile->GetLength()-1;
				//cout<<"File is empty, pipe is empty, not first page to tempfile "<<insertoffset<<endl;

			}
			TempFile->AddPage(insertPage,insertoffset);
		}
		//break;
	}

	TempFile->Close();
	CurrentFile->Close();

	delete CurrentFile;
	CurrentRecord.PageNumber = 0;
	CurrentRecord.PageOffset = 0;
	mode = 'R';
	//cout<<"Removing "<<file_path<<endl;

	if( remove(file_path.c_str() ) != 0 )
		perror( "Error deleting file at the end" );
	rename("sample", file_path.c_str());
	CurrentFile = new File();
	strcpy(filepath, file_path.c_str());
	CurrentFile->Open(1, filepath);
	/*ifstream reading;
	ofstream writing;
	reading.open("sample", ios::binary);
	writing.open(filepath, ios::binary);
	ifstream::pos_type size;
	char *memblock;
	if(reading.is_open()){
		size = reading.tellg();
		memblock = new char[size];
		reading.seekg(0, ios::beg);
		reading.read(memblock, size);
		reading.close();
		writing<<memblock;
		writing.close();
	}*/
	cin.clear();
}

bool Sort::order_based_sort(Record *rec1, Record *rec2, OrderMaker &sortingorder){
	ComparisonEngine compare;
	int result = compare.Compare(rec1,rec2,&sortingorder);
	switch(result)
	{
	case 0 : return false;
	case -1 : return true;
	case 1 : return false;
	default : return false;

	}
}
