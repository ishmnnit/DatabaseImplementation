#include "Defs.h"


class BaseNode
{
	friend class QueryPlan;
private:
	BaseNode* left;
	BaseNode* right;
	BaseNode* parent;

	RelationalOp *nodeRelOp;
	Schema *nodeSchema;
	CNF *nodeCnf;
	Record *nodeLiteral;
	Function *computeMe;
	OrderMaker *groupAtts;

	AndList *nodeAndList;
	string nodeTables;
	OpType nodeReltype;

	Pipe* inPipe1;
	Pipe* inPipe2;
	Pipe* outPipe;

	int nodepipeID;
	int* keepMe;
	int numAttsInput;
	int numAttsOutput;

	DBFile nodeDBF;

public:
	BaseNode()
	{

	}


};

class JoinOrderInfo
{
	friend class QueryPlan;
	double size;
	double cost;
	Statistics *stat;
	set<OrList*> orListSet;
public:
	JoinOrderInfo(){}
	JoinOrderInfo(double s, double c, Statistics *st, set<OrList*> sor)
	{
		size = s;
		cost = c;
		stat = st;
		orListSet = sor;
	}
};
