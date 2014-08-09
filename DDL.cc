#include <iostream>
#include "ParseTree.h"
#include "QueryPlan.h"
#include <fstream>
#include "DBFile.h"
#include <time.h>
#include "DDL.h"

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

//external variables
extern	char *createName; // Create Table name if create command
extern	struct AttList *newAttList; // Attribute list for the table to be created
extern	int commandType; // Type of the command that is select,create insert or delete
extern	char* fileName;
extern	int outType;	//wether to be printed to text, console or 3 for query plan generation
extern  char *outFileName;
extern  char* fileName;
int ConsoleReturnType=0;

//delete Table
void dropTableCatalog(char* tablename){
	ConsoleReturnType=0;
	string line,tmpline;
	int i=0;
	bool flag=false;
	bool found=false;
	bool ffound=false;

	ifstream infile("catalog");
	ofstream outtmpfile("tmp");

	while(!infile.eof()){
		getline(infile,line);	//check whether table exists
		if (strcmp((char*)line.c_str(),"BEGIN")==0){
			tmpline = line;
			getline(infile,line);
			flag = true;
		}
		if (strcmp(createName,(char*)line.c_str())==0){	//if table found, dropping it
			found=true;									//no file-write
			ffound=true;
			flag = false;
			while(strcmp((char*)line.c_str(),"END")!=0){
				getline(infile,line);
			}
		}
		else{
			if (flag == true){				//no file-write
				outtmpfile.write((char*)tmpline.c_str(),tmpline.	length());
				outtmpfile.put('\n');
				flag = false;
			}
		}
		if (found==true && (strcmp((char*)line.c_str(),"END")==0)){			//if not found in the file
			found=false;			//error
			continue;
		}
		outtmpfile.write((char*)line.c_str(),line.length());
		outtmpfile.put('\n');
	}
	infile.close();
	outtmpfile.close();

	if(!ffound){
		cout<<"\nNo such table found"<<endl;
		remove("tmp");
		ConsoleReturnType= -1;
	}

	remove("catalog");
	rename("tmp","catalog");
	if (ConsoleReturnType ==-1)
	{
		cout<<"\nCould not drop the table"<<endl;
		return;
	}
	else
	{
		strcat(createName,".bin");
		remove(createName);
		strcat (createName,".header");
		remove(createName);

		cout << "\nTable dropped"<<endl;
	}
}

//gets catalog and writes table details into the catalog
void insertTableCatalog()
{
	//ConsoleReturnType=0;
	DBFile *myDBFile = new DBFile();
	Schema *mysch = new Schema ("catalog", createName);
	if (findTable()==-1){
		strcat(createName,".bin");

		myDBFile->Create(createName, heap, mysch);
		myDBFile->Close();

		myDBFile->Open(createName);
		myDBFile->Load(*mysch,fileName);
		myDBFile->Close();
		cout << "\nTable inserted"<<endl;
	}
	else
		cout<< "\Could not insert the table"<<endl;
}


void createTableCatalog(){
	ConsoleReturnType=0;
	string line;
	char *tmp = createName;
	AttList *tmpAttList = newAttList;

	if(findTable()== -1)
	{
		ConsoleReturnType = -1;

	}
	ifstream infile ("catalog");
	ofstream outfile("catalog",ios_base::app);

	//write in specified format
	outfile.write("BEGIN\n",6);	//starts with 'BEGIN' in the catalog
	while(*(tmp)!='\0'){
		outfile.put(*(tmp));
		tmp++;
	}
	outfile.put('\n');
	tmp = createName;
	while(*(tmp)!='\0'){
		outfile.put(*(tmp));
		tmp++;
	}
	outfile.write(".tbl\n",5);
	while(tmpAttList != NULL){	//write attribute names to catalog
		tmp = tmpAttList->AttInfo->name;
		while(*(tmp)!='\0'){			//attribute name
			outfile.put(*(tmp));
			tmp++;
		}
		if (tmpAttList->AttInfo->code == INT)	//attribute type
			outfile.write(" Int\n",4);
		else if (tmpAttList->AttInfo->code == DOUBLE)
			outfile.write(" Double\n",7);
		else if (tmpAttList->AttInfo->code == STRING)
			outfile.write(" String\n",7);
		outfile.put('\n');
		tmpAttList = tmpAttList->next;
	}
	outfile.write("END\n",4);
	infile.close();
	outfile.close();
	DBFile *DBobj = new DBFile();
	if (ConsoleReturnType ==-1)
	{
		cout << "\nCould not create the table"<<endl;
		return;
	}
	Schema *mysch = new Schema ("catalog", createName);
	strcat(createName,".bin");

	DBobj->Create(createName,heap,NULL);
	DBobj->Close();

	cout << "\nTable created"<<endl;
}

int findTable()
{
	string line;
	char *tmp = createName;
	ifstream infile ("catalog");
	ofstream outfile("catalog",ios_base::app);

	while(!infile.eof()){
		getline(infile,line);
		if (line.compare((string)createName)==0){
			cout << "\nTable already in the database"<<endl;
			infile.close();
			outfile.close();
			return -1;
		}
	}
	infile.close();
	outfile.close();

	return 0;
}
