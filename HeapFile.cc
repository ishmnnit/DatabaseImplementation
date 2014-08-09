#include<iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "HeapFile.h"

//########################################################################################################################
//##################################### HeapFile ###########################################################################

/*First parameter to this function is a text string that tells you where the binary data is physically
to be located

The second parameter to the Create function tells you the type of the file. In HeapFile.h, you an enumeration
called myType with three possiblevalues: heap, sorted, and tree. When the HeapFile is created, one of these
treevalues is passed to Create to tell the file what type it will be.

the last parameter to Create is a dummy parameter that you won’t use for this assignment, but you will use for
assignment two.

Return value from Create is a 1 on success and a zero on failure.

*/

HeapFile::HeapFile(){
	myFile=NULL;
	readBuffer=NULL;
	writeBuffer=NULL;
	currPage=0;
}

int HeapFile::Create(char *f_path, fType f_type, void *startup){
	if(myFile==NULL){
		myFile=new File;
	}
    myFile->Open(0,f_path);
    return 1;
}

/*
 * The Load function bulk loads the HeapFile instance from a text file, appending new data to it using
 * the SuckNextRecord function from Record.h
 *
 * The character string passed to Load is the name of the data file to bulk load
 */

void HeapFile::Load(Schema &f_schema,char* loadpath){
	Record temp;
	FILE *filepath=fopen(loadpath,"r");
	int c=0;
	while (temp.SuckNextRecord (&f_schema,filepath)) {
		c++;
        Add(temp);
	}
}

/*
 * This function assumes that the HeapFile already exists and has previously been created and then closed.
 * The one parameter to this function is simply the physical location of the file.
 * The return value is a 1 on success and a zero on failure.
 */

int HeapFile::Open(char *f_path){
	if(myFile==NULL){
		myFile=new File;
	}
       myFile->Open(1,f_path);
    return 1;
}

void HeapFile::write_buffer(){
	int pageNo=myFile->GetLength();
	if(writeBuffer!=NULL)
	{
		myFile->AddPage(writeBuffer,pageNo);
		writeBuffer->EmptyItOut();
	}
}


void HeapFile::MoveFirst () {
	currPage=0;
}

/*
 *  Close simply closes the file. The return value is a 1 on success and a zero on failure.
*/

int HeapFile::Close(){
	write_buffer();
	if(!myFile->Close()){
		cout<<"Error in close\n";
		return 0;
	}
	else{
		delete myFile;
		return 1;
	}
}

/*
 * In the case of the unordered heap file that you are implementing in this assignment, this function
 * simply adds the new record to the end of the file:
 */

void HeapFile::Add (Record &rec) {
	if(writeBuffer==NULL){
		writeBuffer=new Page;
		off_t page_num = myFile->GetLength();
		if(page_num>0){
			off_t writePage = page_num;
			myFile->GetPage(writeBuffer, writePage);
		}
	}
	while(!writeBuffer->Append(&rec)){
		int pageNo=myFile->GetLength();
		myFile->AddPage(writeBuffer, pageNo);
		writeBuffer->EmptyItOut();
	}
}

/*
 * simply gets the next record from the file and returns it to the user, where “next” is defined to be
 * relative to the current location of the pointer. After the function call returns, the pointer into the
 * file is incremented, so a subsequent call to GetNext won’t return the same record twice. The return
 * value is an integer whose value is zero if and only if there is not a valid record returned from the function
 * call (which will be the case, for example, if the last record in the file has already been returned).
*/

int HeapFile::GetNext (Record &fetchme) {
	//GetNextcall Number Record..
	if(readBuffer==NULL){
		readBuffer=new Page;
		myFile->GetPage(readBuffer,currPage);
	}
	//cout<<"Inside GetNext\n";
	while(!readBuffer->GetFirst(&fetchme)){
		//cout<<"Inside GetNextdjdfkdfdfrnfhrhkrfhefejfefhkfefeiufefefheiu\n";
		readBuffer->EmptyItOut();
		currPage++;
		if(currPage >= myFile->GetLength()-1)
			return 0;
		myFile->GetPage(readBuffer,currPage);
	}
	return 1;
}

/*
The next version of GetNext also accepts a selection predicate (this is a conjunctive
normal form expression). It returns the next record in the file that is accepted by the
selection predicate. The literal record is used to check the selection predicate, and is
created when the parse tree for the CNF is processed.
*/


int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	    ComparisonEngine comp;
        while (GetNext(fetchme)){
		if (comp.Compare (&fetchme, &literal, &cnf))
	       return 1;
	}//end While
	    return 0;
}
