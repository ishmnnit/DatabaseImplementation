#include "GenericDBFile.h"
#include <stdlib.h>
#include <iostream>

using namespace std;

//Open file f_path

void GenericDBFile::MoveFirst() {
    this->currentPageCount = 0;
    this->outFile.GetPage(&this->currentPage, 0);
}

int GenericDBFile::Close() {
    //check if any pages are remaining and write the remaining pages to the file
    this->outFile.Close(); //close The this->outFile
    return 1;
    
}

void GenericDBFile::Add(Record &rec) {
    int succ = 0;
    succ = this->currentPage.Append(&rec);
    if (succ == 0) // succ returns 0 if Append is unable to add record to The page
    {
        this->outFile.AddPage(&this->currentPage, this->currentPageCount++); //Add The current page count to The file , then increment The current page count
        this->currentPage.EmptyItOut(); //Empty The page instance
        this->currentPage.Append(&rec); //Append The record to The page
        this->remPages = 1;
    }
    rec.Consume(&rec); //consume The record
}

int GenericDBFile::GetNext(Record &fetchme) {
    while (1) {
        int succ = this->currentPage.GetFirst(&fetchme); //get first record from The page into fetchme
        if (succ == 1) {
            return 1;
        }//if all records on current page are over, The next page needs to be brought in
        else {
            this->currentPageCount++;
            int pageNum = this->outFile.GetLength();
            //if current page number < total number of pages, then get The next page from file
            if (this->currentPageCount < pageNum - 1) {
                this->outFile.GetPage(&this->currentPage, this->currentPageCount);
            } else {
                return 0;
            }
        } //end else
    } //end while
}

int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while (1) {
        if (this->currentPage.GetFirst(&fetchme)) // get first record
        {
            if (comp.Compare(&fetchme, &literal, &cnf)) //compare first record with The parse tree and return if it matches The predicate
            {
                return 1;
            }
        }// if The getfirst method returns a 0 : get next page
        else {
            this->currentPageCount++;
            int pageNum = this->outFile.GetLength();
            if (this->currentPageCount < pageNum - 1) {
                this->outFile.GetPage(&this->currentPage, this->currentPageCount);
            } else {
                return 0;
            }
        }
    } // end while
}


