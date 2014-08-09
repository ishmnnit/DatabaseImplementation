#include "RelOp.h"
#include "Schema.h"
#include "Pipe.h"
#include "Statistics.h"
#include "QueryNode.h"
#include <string>
#include <string.h>
#include <vector>
#include <tr1/unordered_map>
#include <set>
#include <algorithm>
#include <iostream>
#include <map>
#include <unistd.h>

using namespace std;
extern	struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern 	struct TableList *tables; // the list of tables and aliases in the query
extern	struct AndList *boolean; // the predicate in the WHERE clause
extern	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern	int distinctFunc;  // 2 if there is a DISTINCT in an aggregate query
extern int outType;
extern char* outFileName;

class comparefunc{

public:
	bool operator() (string s1, string s2)
	{
		return s1<s2;
	}
};
class myfun{

public:
	bool operator()(set<string, comparefunc> v1, set<string, comparefunc> v2)
	{
		return v1.size() < v2.size();
	}
};

class QueryPlan

{
	std::tr1::unordered_map<struct OrList*, set<string, comparefunc> > JoinMap;	//all joins .. the orlist , tables associated with tht orlist
	std::tr1::unordered_map<string, vector<OrList*> > SelectionMap;		//all selections
	std::map< set<string,comparefunc>, JoinOrderInfo* > JoinOrderMap;		//combinations of joins
	set < set<string, comparefunc> > FinalcomboList;			//list of various combinations
	vector < set <string, comparefunc> > comboListcopy;
	string *joinstr;

	BaseNode* root;
	DBFile *dbf;
	int globalTablecount;
	vector<OrList*> UsedOrList;
	int numberofpipes;
	int pipe_size;

public:

	QueryPlan()
	{
		globalTablecount = 0;
		numberofpipes = 0;
		pipe_size = 2000;
	}
	~QueryPlan()
	{
		JoinOrderMap.clear();
		JoinMap.clear();
		SelectionMap.clear();
		FinalcomboList.clear();
		comboListcopy.clear();
		UsedOrList.clear();
	}
	//main operations
	void executeQueryPlan();
	void start();
	void init();
	void getOrListInfo(struct OrList*);
	void JoinOrder();
	void FindCombinations(set<string, comparefunc>, set<string, comparefunc>);
	void getRelnames(char**, set<string, comparefunc>);
	void CreateTree();
	void DetermineOperation();
	void ExecuteTree(BaseNode*);
	int clear_pipe (Pipe &in_pipe, Schema *schema, bool print);

	//miscellaneous
	Schema* GetSchema(char *tablename);
	string GetTableName(char *);
	Schema* CombineSchema(Schema *leftSchema, Schema *rightSchema);
	void PrintSchema(Schema *mySchema);
	void PrintTree(BaseNode * node);
	AndList* CreateAndList( vector<char*> &input);
	OrList* CreateOrList(char * input);
	bool CompareSets(set<string, comparefunc>&, set<string, comparefunc>&);

	//operations
	void CreateProjectNode();
	void CreateDuplicateRemovalNode();
	void CreateGroupbyNode();
	void CreateSumNode();
	BaseNode* SelectFileNode(string);

	//execute operations
	void execSF(BaseNode *node);
	void execP(BaseNode *node);
	void execGrp(BaseNode *node);
	void execJoin(BaseNode *node);
	void execDist(BaseNode *node);
	void execSum(BaseNode *node);


};

