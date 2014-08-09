#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>

int Schema::Find(char *attName) {

    for (int i = 0; i < numAtts; i++) {
        if (!strcmp(attName, myAtts[i].name)) {
            return i;
        }
    }

    // if we made it here, the attribute was not found
    return -1;
}

Type Schema::FindType(char *attName) {

    for (int i = 0; i < numAtts; i++) {
        if (!strcmp(attName, myAtts[i].name)) {
            return myAtts[i].myType;
        }
    }

    // if we made it here, the attribute was not found
    return Int;
}

int Schema::GetNumAtts() {
    return numAtts;
}

Attribute *Schema::GetAtts() {
    return myAtts;
}

Schema::Schema(char *fpath, int num_atts, Attribute *atts) {
    fileName = strdup(fpath);
    numAtts = num_atts;
    myAtts = new Attribute[numAtts];
    for (int i = 0; i < numAtts; i++) {
        if (atts[i].myType == Int) {
            myAtts[i].myType = Int;
        } else if (atts[i].myType == Double) {
            myAtts[i].myType = Double;
        } else if (atts[i].myType == String) {
            myAtts[i].myType = String;
        } else {
            cout << "Bad attribute type for " << atts[i].myType << "\n";
            delete [] myAtts;
            exit(1);
        }
        myAtts[i].name = strdup(atts[i].name);
    }
}

Schema::Schema() {
    numAtts = 0;
    myAtts = NULL;
}

Schema::Schema(char *fName, char *relName) {

    FILE *foo = fopen(fName, "r");

    // this is enough space to hold any tokens
    char space[200];

    fscanf(foo, "%s", space);
    int totscans = 1;

    // see if the file starts with the correct keyword
    if (strcmp(space, "BEGIN")) {
        cout << "Unfortunately, this does not seem to be a schema file.\n";
        exit(1);
    }

    while (1) {

        // check to see if this is the one we want
        fscanf(foo, "%s", space);
        totscans++;
        if (strcmp(space, relName)) {

            // it is not, so suck up everything to past the BEGIN
            while (1) {

                // suck up another token
                if (fscanf(foo, "%s", space) == EOF) {
                    cerr << relName << ":Could not find the schema for the specified relation\n";
                    cerr << "Please Create the schema before applying any queries on it\n";
                    exit(1);
                }

                totscans++;
                if (!strcmp(space, "BEGIN")) {
                    break;
                }
            }

            // otherwise, got the correct file!!
        } else {
            break;
        }
    }

    // suck in the file name
    fscanf(foo, "%s", space);
    totscans++;
    fileName = strdup(space);

    // count the number of attributes specified
    numAtts = 0;
    while (1) {
        fscanf(foo, "%s", space);
        if (!strcmp(space, "END")) {
            break;
        } else {
            fscanf(foo, "%s", space);
            numAtts++;
        }
    }

    // now actually load up the schema
    fclose(foo);
    foo = fopen(fName, "r");

    // go past any un-needed info
    for (int i = 0; i < totscans; i++) {
        fscanf(foo, "%s", space);
    }

    // and load up the schema
    myAtts = new Attribute[numAtts];
    for (int i = 0; i < numAtts; i++) {

        // read in the attribute name
        fscanf(foo, "%s", space);
        myAtts[i].name = strdup(space);

        // read in the attribute type
        fscanf(foo, "%s", space);
        if (!strcmp(space, "Int")) {
            myAtts[i].myType = Int;
        } else if (!strcmp(space, "Double")) {
            myAtts[i].myType = Double;
        } else if (!strcmp(space, "String")) {
            myAtts[i].myType = String;
        } else {
            cout << "Bad attribute type for " << myAtts[i].name << "\n";
            exit(1);
        }
    }

    fclose(foo);
}

Schema::~Schema() {
    delete [] myAtts;
    myAtts = 0;
}

void Schema::Print() {
    for (int i = 0; i < numAtts; i++) {
        cout << myAtts[i].name << ":  ";
        if (myAtts[i].myType == Int) {
            cout << "Int";
        } else if (myAtts[i].myType == Double) {
            cout << "Double";
        } else if (myAtts[i].myType == String) {
            cout << "String";
        }
        cout << endl;
    }
}

void Schema::renameSchema(char* newname, Schema *s) {
    numAtts = s->GetNumAtts();
    Attribute *atts = s->GetAtts();
    myAtts = new Attribute[numAtts];
    string newAttsName;
    strcat(newname, ".");
    for (int i = 0; i < numAtts; i++) {
        newAttsName = newname;
        if (atts[i].myType == Int) {
            myAtts[i].myType = Int;
        } else if (atts[i].myType == Double) {
            myAtts[i].myType = Double;
        } else if (atts[i].myType == String) {
            myAtts[i].myType = String;
        }
        myAtts[i].name = strdup((newAttsName + atts[i].name).c_str());
    }
}

void Schema::copySchema(Schema *s) {
    numAtts = s->GetNumAtts();
    Attribute *atts = s->GetAtts();
    myAtts = new Attribute[numAtts];
    for (int i = 0; i < numAtts; i++) {
        if (atts[i].myType == Int) {
            myAtts[i].myType = Int;
        } else if (atts[i].myType == Double) {
            myAtts[i].myType = Double;
        } else if (atts[i].myType == String) {
            myAtts[i].myType = String;
        }
        myAtts[i].name = strdup(atts[i].name);
    }
}

void Schema::joinSchema(Schema *s1, Schema *s2) {
    int numAtts1 = s1->GetNumAtts();
    int numAtts2 = s2->GetNumAtts();
    numAtts = numAtts1 + numAtts2;
    Attribute *atts1 = s1->GetAtts();
    Attribute *atts2 = s2->GetAtts();
    myAtts = new Attribute[numAtts];
    int i, j;
    for (i = 0; i < numAtts1; i++) {
        if (atts1[i].myType == Int) {
            myAtts[i].myType = Int;
        } else if (atts1[i].myType == Double) {
            myAtts[i].myType = Double;
        } else if (atts1[i].myType == String) {
            myAtts[i].myType = String;
        }
        myAtts[i].name = strdup(atts1[i].name);
    }
    j = numAtts1;
    for (i = 0; i < numAtts2; i++, j++) {
        if (atts2[i].myType == Int) {
            myAtts[j].myType = Int;
        } else if (atts2[i].myType == Double) {
            myAtts[j].myType = Double;
        } else if (atts2[i].myType == String) {
            myAtts[j].myType = String;
        }
        myAtts[j].name = strdup(atts2[i].name);
    }
}

void Schema::projectSchema(Schema *s1, int keepMe[], int numAttstoKeep) {
    Attribute *atts1 = s1->GetAtts();
    numAtts = numAttstoKeep;
    myAtts = new Attribute[numAttstoKeep];
    for (int i = 0; i < numAttstoKeep; i++) {
        myAtts[i].myType = atts1[keepMe[i]].myType;
        myAtts[i].name = strdup(atts1[keepMe[i]].name);
    }
}

void Schema::addSchema(char *fpath, char ** attName, char * tablename) {
    if (check(fpath, tablename) == 0) {
        ofstream file;
        file.open(fpath, fstream::app);
        file << "BEGIN" << endl;
        file << tablename << endl;
        string stableName = tablename;
        file << stableName << ".tbl" << endl;

        for (int i = 0; i < numAtts; i++) {
            file << attName[i] << " ";
            if (myAtts[i].myType == Int) {
                file << "Int";
            } else if (myAtts[i].myType == Double) {
                file << "Double";
            } else if (myAtts[i].myType == String) {
                file << "String";
            }
            file << endl;
        }
        file << "END" << endl;
        file.close();
    }
}

int Schema::check(char *fName, char *relName) {
    FILE *foo = fopen(fName, "r");

    // this is enough space to hold any tokens
    char space[200];

    fscanf(foo, "%s", space);
    int totscans = 1;

    // see if the file starts with the correct keyword
    if (strcmp(space, "BEGIN")) {
        return 0;
    }

    while (1) {

        // check to see if this is the one we want
        fscanf(foo, "%s", space);
        totscans++;
        if (strcmp(space, relName)) {

            // it is not, so suck up everything to past the BEGIN
            while (1) {

                // suck up another token
                if (fscanf(foo, "%s", space) == EOF) {
                    return 0;
                }

                totscans++;
                if (!strcmp(space, "BEGIN")) {
                    break;
                }
            }

            // otherwise, got the correct file!!
        } else {
            return 1;
        }
    }
}