#include "DBFile.h"
#include "SortedFile.h"
#include "HeapFile.h"
//#################################################################################################
//####################################### DBFILE FUNCTION #########################################
//#################################################################################################

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	string meta_file_name = f_path;
	meta_file_name.append(".meta");
	output_meta.open(meta_file_name.c_str(), ios::out);
	if( f_type == sorted ){
    	db_file_ob = new SortedFile;
        sort_info  = (struct SortInfo*)startup;
    	// Write the type of file to meta file
    	output_meta<<"sorted"<<"\n";
    	output_meta.close();
    }
	else { // if( f_type == heap
		db_file_ob = new HeapFile;
		// Write the type of file to meta file
		output_meta<<"heap"<<"\n";
		output_meta.close();
	}
	return db_file_ob->Create(f_path, f_type, startup);
}


int DBFile::Open (char *f_path){
	string meta_file_name = f_path;
	meta_file_name.append(".meta");
	input_meta.open(meta_file_name.c_str());
	string type, line;
	// read the first line of the file to know whether sorted or heap
	getline(input_meta, type);
	input_meta.close();
	//cout<<"DBFIle::open\n";
	if( type == "sorted")
		{
		  db_file_ob = new SortedFile;
		  //cout<<"IEECJECNJEJCVER,vebvrvervhvhejevhevehverv\n";
		}
	else
		db_file_ob = new HeapFile;
	return db_file_ob->Open(f_path);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {	
	return db_file_ob->Load(f_schema,loadpath);

}

/**
 */
void DBFile::MoveFirst () {
    return db_file_ob->MoveFirst();
}
/**
 */
int DBFile::Close () {
	return db_file_ob->Close();
}
/**
 */
 
void DBFile::Add (Record &rec) {
	return db_file_ob->Add(rec);
}

/**
 */
int DBFile::GetNext (Record &fetchme) {
	return db_file_ob->GetNext(fetchme);
}
/**
 */
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return db_file_ob->GetNext(fetchme,cnf,literal);
}


