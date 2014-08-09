#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <cctype>
#include <stdio.h>
#include <tr1/unordered_map>
#include <iomanip>
#include <locale>
#include <sstream>

using namespace std;

struct TableDetails
{
	unsigned long int numtuples;
	int partition;
	std::tr1::unordered_map<string , int> AttrMap;
	TableDetails(){
		numtuples=0;
		partition =0 ;  //0 means singleton
	};
};
class Statistics
{

public:

	std::tr1::unordered_map<string , TableDetails> RelationMap;
	std::tr1::unordered_map<int , vector<string> > JoinMap;
	std::tr1::unordered_map<string , int> RelJoin;
	std::tr1::unordered_map<string , string> AttrRelMap;
	set <string> joinset;

	struct EstimateInfo
	{
		int AttrCount;  //number of times same column is repeated
		long double estimatetuples;   //this might change depending on how many columns participate
	};
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	bool chkParsetree1(struct AndList *parseTree, char *relNames[], int numToJoin,vector<string> &AttsToEstimate);
	bool chkParsetree2(struct AndList *parseTree, char *relNames[], int numToJoin);

	bool has_any_digits(string t);
	double getResult(set <string> &jointableset,vector<double> &estimates);
};

#endif
