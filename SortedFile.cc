#include <sstream>
#include "SortedFile.h"
#include <fstream>

SortedFile::SortedFile(){
	myFile=NULL;
	currPage=0;
	mode = reading;
	notCalled = false;
}

bool iswrite=true;

/*void *run_q (void *arg) {
	bigq_util *t = (bigq_util *) arg;
	BigQ biqQ(*(t->in),*(t->out),*(t->sort_order),t->run_len);
}*/

int SortedFile::Create (char *f_path, fType f_type, void *startup){
	notCalled = false;
	ofstream output_meta;
	string p(f_path);
	string meta_file_name = p + ".meta";
	output_meta.open(meta_file_name.c_str(), ios::app);
	struct SortInfo *sort_info  = (struct SortInfo*)startup;

	// Write the info to meta file
	output_meta<<sort_info->myOrder->numAtts<<"\n";
	for(int i=0; i<sort_info->myOrder->numAtts;i++){
		output_meta<<sort_info->myOrder->whichAtts[i]<<"\n";
	}
	for(int i=0; i<sort_info->myOrder->numAtts;i++){
		output_meta<<sort_info->myOrder->whichTypes[i]<<"\n";
	}
	output_meta<<sort_info->run_length;
	output_meta.close();
	// Do stuff related to creating the sort file
	initialiseSortedFile( f_path, 0);
	if(myFile == NULL)
	{   return 0;
	}
	else
		return 1;
}

void SortedFile::initialiseSortedFile( char *f_path, int file_length){
	if(myFile == NULL)
	{
	   myFile = new File;
	   //cout<<"rereoeo\n";
	}	myFile->Open(file_length, f_path);
}

int SortedFile::Open (char *f_path){
	notCalled = false;
	ifstream input_meta;
	string line;
	fpath = f_path;
	string meta_file_name = f_path;
	meta_file_name.append(".meta");
	input_meta.open(meta_file_name.c_str());
	getline( input_meta, line ); // First line is the sorted/heap
	getline( input_meta, line ); // Second line is num_atts
	int num_atts = atoi(line.c_str());
	OrderMaker order;
	order.numAtts = num_atts;
    //struct SortInfo *sort_info= new SortInfo;

	
	// Read the num_atts equivalent of attributes
	int count = 0;
	while( count < num_atts ){
		getline( input_meta, line);
		order.whichAtts[count++] = atoi(line.c_str());
	}
	// Read the num_atts types
	count = 0;
	while( count < num_atts ){
		getline( input_meta, line);
		if(line == "Int")
		      order.whichTypes[count++] = Int;
		else if(line == "Double")
			  order.whichTypes[count++] = Double;
		else
			  order.whichTypes[count++] = String;
	}
	int run_len;
	getline( input_meta, line);
	//cout<<"line = "<<line<<endl;
	//cout<<"line.c_str="<<line.c_str()<<"\n";
    istringstream ( line ) >> run_len;
	input_meta.close();
	runlen=run_len;
	sort_info_od = &order;
	initialiseSortedFile(f_path, 1);
	if(myFile->GetLength() > 0)
	    myFile->GetPage(&readBuffer,0);
	if( myFile == NULL) return 0;
	else return 1;
}


void SortedFile::MoveFirst () {
	notCalled = false;
     if(mode == writing)
	 {
		 ChangeMode();
	 }
	 currPage=0;
	 //cout<<myFile->GetLength()<<endl;
}
 
 
int SortedFile::Close (){
	notCalled = false;
   if(mode == writing)
	 {
		 ChangeMode();
		 //cout<<"in close\n";
	 }
	if(!myFile->Close()){
		//cout<<"Error in close\n";
		 return 0;
		}
	else{
		    //delete myFile;
		    return 1;
	      }
}
 

void SortedFile::Load(Schema &f_schema,char* loadpath)
{
	notCalled = false;
	Record temp;
	FILE *filepath=fopen(loadpath,"r");
	int c=0;
	while (temp.SuckNextRecord (&f_schema,filepath)) {
		   c++;
           Add(temp);          	  	
	}
}

 

void SortedFile::AddData(Record &rec,int &pageNum) {	
	static int cnt=0;
	while(!tempAddpage.Append(&rec))
	{
		tempFile.AddPage(&tempAddpage,pageNum);
		pageNum++;
		tempAddpage.EmptyItOut();
	}
	
	//cout<<"AddData::"<<cnt++<<"\n";
}


void SortedFile::Add (Record &rec) {
	notCalled = false;
	////cout<<"Record ka Aage\n";
	if(mode == reading)
	{
		ChangeMode();
	}
	in->Insert(&rec);
	//cout<<"Record added";
}

void SortedFile::ChangeMode() {
	//cout<<"changemode begin\n";
    if(mode == writing )
      {
           mode=reading;
           in->ShutDown();
           MergeFile();
           delete in;
           delete out;
		   //delete util;
	  }
    else //Mode=reading
	 {
          mode=writing;
          in=new Pipe(MAX_BUFFER);
          out=new Pipe(MAX_BUFFER);
		  bq = new BigQ( *in, *out, *sort_info_od, runlen);
	 }
	 currPage = 0;
           
}

void SortedFile::MergeFile()
{
	static int pageNum=0;
	Record record1,record2;
	ComparisonEngine comp;
	iswrite=false;
	/* Temp File is not delared as a Pointer , No need to call Create because Create check whether it is NULl and 
	 * then allocate Memory,insted dirctly call Open so we will not worried about intializing in constructor 
	 */
	char *temppath="tempFile.bin"; 
	tempFile.Open(0,temppath);
	
	int fstatus,qstatus;
	if (myFile->GetLength()>0)
	{
	      myFile->GetPage(&temp_page,0);
		  fstatus=GetNext_File(record1);
		  //cout<<"OKkk";
	}
	else
		fstatus=0;		  
		  
	qstatus=out->Remove(&record2);
	
	while(1) {
		        if(fstatus && qstatus) {
					RecordWrapper *rw1 = new RecordWrapper( &record1, sort_info_od);
		        	RecordWrapper *rw2 = new RecordWrapper( &record2, sort_info_od);;
		        	if( RecordWrapper::compareRecords(rw1, rw2) < 0){
					           AddData(record1,pageNum);
							   //cout<<"part1\n";
							   fstatus=GetNext_File(record1);
						     }
				        else {
		     		           AddData(record2,pageNum);
							   //cout<<"part2\n";
							    qstatus=out->Remove(&record2);
							 }
						 continue;
			     }
				 
			    else if(fstatus) {
		             do {
					       AddData(record1,pageNum);
						   //t<<record1;
						   //cout<<"Part3\n";
					    } while(GetNext_File(record1));
					 break;
			    }
				
			   else if(qstatus) {
				   
				     do {
					       AddData(record2,pageNum);
						   //fwrite (record2, sizeof(buffer), pFile);
						} while(out->Remove(&record2));
				    break;
				}
				
			  else 
				break;
		}  //End while(1)
		
	 tempFile.AddPage(&tempAddpage,pageNum);
	 rename("tempFile.bin",fpath);
	 myFile=&tempFile;
	 //cout<<"myFile="<<tempFile.GetLength()<<"\n";
	 tempFile.Close();
	  
}


int SortedFile::GetNext_File(Record &fetchme) {
	//cout<<"Currpage="<<currPage<<"\n";
	while(!temp_page.GetFirst(&fetchme)) {
		if(currPage<myFile->GetLength()-1) {
			//cout<<"GetNext_file\n";
			myFile->GetPage(&temp_page,currPage);
			currPage++;
			
		}
		else
			return 0;
	}
	return 1;
}

int SortedFile::GetNext (Record &fetchme) {
	notCalled = false;
	//GetNextcall Number Record..
	if( mode == writing && iswrite){
		ChangeMode();
	}
	return GetNextWrapper(fetchme);
	
}

int SortedFile::GetNextWrapper(Record &fetchme)
{
	while(!readBuffer.GetFirst(&fetchme)){
		readBuffer.EmptyItOut();
		currPage++;
		if(currPage >= myFile->GetLength()-1)
			return 0;
		myFile->GetPage(&readBuffer,currPage);
		
	}
	return 1;
}
int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    cout<<"yes";	
	if(mode == writing) 
	{
		ChangeMode(); 
	}
	OrderMaker query_order;
	cnf.QueryMaker(*sort_info_od,query_order);
	
	if(query_order.numAtts==0 || notCalled)
		return LinearSearch(fetchme, cnf, literal);
	else
		BinarySearch(fetchme,cnf,literal,query_order);
	  
}


int SortedFile::LinearSearch(Record &fetchme, CNF &cnf, Record &literal) {
	    ComparisonEngine comp;
		bool flag = false;
		while(GetNext(fetchme)!= 0 )
		{
			if(comp.Compare(&fetchme, &literal, &cnf) == 1)
				return 1;
		}
		return 0;
}


int SortedFile::BinarySearch(Record &fetchme, CNF &cnf, Record &literal,OrderMaker &query_order) 
{	    
	    notCalled = true;
	    int f_page=0;
		int l_page=myFile->GetLength()-2;
		int comparison;
		bool flag=false;
		int m_page;
		ComparisonEngine comp;
		
		while(f_page<=(l_page)) {
		      m_page=floor((f_page+l_page)/2.0);
			  myFile->GetPage(&readBuffer,m_page);
		      readBuffer.GetFirst(&fetchme);
			  comparison=comp.Compare(&fetchme,&literal,&query_order);
			  if(comparison)
				   {
					   l_page = m_page;
				   }											
			  if (comparison > 0) 
			          l_page=m_page;
			  else if(comparison < 0) 
			         f_page=m_page;
       }
	   currPage = m_page;
	   myFile->GetPage(&readBuffer,currPage);
	   
	   return LinearSearch(fetchme,cnf, literal);
	   
	   
}



int SortedFile::GetMatch(Record &fetchme, Record &literal, CNF &cnf, OrderMaker &search_order,OrderMaker &literal_order) {
	ComparisonEngine comp;
	int comparison;
	while(readBuffer.GetFirst(&fetchme)) {
		if (!comp.Compare (&fetchme,&search_order,&literal, &literal_order)) {
			    if(comp.Compare(&fetchme,&literal,&cnf))
				   return 1;
		}
		else {
			continue;
		}
	}
	return 0;
}

//############################################################################################################
//#################################################### END SORTEDFILE ########################################
//############################################################################################################

