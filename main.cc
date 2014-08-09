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
extern	int commandType; // Type of the command that is select,create insert or delete

int main () {

	bool flag = true;
	streambuf *sb,*tmp;
	while(flag == true)
	{

		cout<<endl<<endl;
		cout<<"Enter SQL> ";
		
		yyparse();

		clock_t init, final;
		init = clock();
		
		//Choose whether to create/update/delete based on commandType
		switch (commandType){
			case 1:{
				createTableCatalog();	//create table
				break;
			}
			case 2:{
				insertTableCatalog();		//insert table
				break;
			}
			case 3:
			{
				dropTableCatalog(createName);	//drop a table
				break;
			}
			case 4:
				//outType is updated
				break;
			case 5:{
				QueryPlan Q;		//select and execute/print query
				Q.start();
				break;
			}
			case 6:{
				flag = false;	//exit
				break;
			}
			default:
				break;
		}
		
		final = clock() - init;
		cout<<"Time taken: "<<(double)final / ((double)CLOCKS_PER_SEC)<<endl;
	}
}
