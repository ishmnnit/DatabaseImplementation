#include "QueryPlan.h"

using namespace std;

void QueryPlan::start()
{
	cout<<"\nPreparing Query..."<<endl;
	cout<<"...................."<<endl<<endl;
	executeQueryPlan();

	if(outType == 3)	//For query : Set output none
	{
		cout<<"\nQuery Tree -->"<<endl;
		PrintTree(root);
	}
	else	//For query : Set output stdout / file
	{
		cout<<"Running Query..."<<endl;
		ExecuteTree(root);
	}
}

//Executing query plan
void QueryPlan::executeQueryPlan()
{

	init();
	JoinOrder();
	CreateTree();
	DetermineOperation();
}

//Separate out OrLists
void QueryPlan::init()
{
	struct AndList *parseTree = boolean;

	while(parseTree!=NULL)
	{
		struct OrList *newOr = parseTree->left;
		getOrListInfo(newOr);	//get all info separate into select and join maps
		parseTree = parseTree->rightAnd;
	}
}

//Finding Join Orders
void QueryPlan :: JoinOrder()
{
	set<string, comparefunc> tblnames;

	struct TableList *currTable = tables;
	Statistics sobj;
	sobj.Read("Statistics.txt");		//read initial stats Data
	while(currTable!=NULL)			//all tables in the list are taken and copies created for aliases in stats obj
	{
		globalTablecount++;			//global count of all tables
		string name(currTable->aliasAs);
		tblnames.insert(name);		//list of all tables
		sobj.CopyRel(currTable->tableName, currTable->aliasAs);		//create a copy with alias
		currTable = currTable->next;
	}
	sobj.Write("StatisticsNew.txt");

	set<string, comparefunc>finalTab;	//compare func to get it sorted by name
	FindCombinations(tblnames, finalTab);

	for(set< set<string, comparefunc> >::iterator it1 = FinalcomboList.begin() ;it1!=FinalcomboList.end();it1++)  //combofinal has all combinations
	{
		comboListcopy.push_back(*it1);			//create copy
	}
	myfun myfunobj;
	sort(comboListcopy.begin(), comboListcopy.end(), myfunobj);	//sort so that stored according to vector size A AB ABC etc

	int i=0;
	Statistics *statobj = new Statistics();
	statobj->Read("StatisticsNew.txt");

	//go over all select queries
	std::tr1::unordered_map<string, vector<OrList*> > ::iterator it;
	for(it = SelectionMap.begin(); it!=SelectionMap.end();++it)
	{
		struct AndList* parseTree = new AndList();
		char **relNames = new char*[1];
		relNames[0] = new char[(it->first).size()+1];
		relNames[0] = (char*)(it->first).c_str();
		vector<OrList*> tempVec = it->second;
		struct AndList* tempParseTree = parseTree;	//create the tree
		for(int i=0;i<tempVec.size();i++)
		{
			tempParseTree->left = tempVec[i];	//put each part of the orlist on the left of the tree
			if(i< tempVec.size() -1 )
			{
				tempParseTree->rightAnd = new AndList();	//keep creating andlists
				tempParseTree = tempParseTree->rightAnd;
			}
			else
			{
				tempParseTree->rightAnd = NULL;	//now complete ...set last one to null
			}
		}
		statobj->Apply(parseTree, relNames, 1);	//apply the selection function to it ..so that we can store the current stat obj
	}
	int size = 1;
	int comboIndex=0;

	while(size == 1 && comboIndex < comboListcopy.size() )		//this is to get combinations limited to size 1, A B C
	{
		set <string, comparefunc>s = comboListcopy[comboIndex];
		set<string, comparefunc>::iterator it1 = s.begin();
		char** relNames = new char*[s.size()];				//get all table names involved to giv to stat estimate

		getRelnames(relNames, s);	//get all the relation names in this combination

		string rel = *it1;
		std::tr1::unordered_map<string, vector<OrList*> > :: iterator it2;
		it2 = SelectionMap.find(rel);					//find this relation in selection map
		struct AndList *parseTree = NULL;
		set<OrList*>orListSet;
		if(it2!=SelectionMap.end())					//traverse the orlist related to table in selection map ..generate tree and get estimate
		{										//cost for this is 0
			parseTree = new AndList();
			struct AndList* tempParseTree = parseTree;
			vector<OrList*> tempVec = it2->second;
			for(int i=0;i<tempVec.size();i++)
			{
				orListSet.insert(tempVec[i]);
				tempParseTree->left = tempVec[i];
				if(i< tempVec.size() -1 )
				{
					tempParseTree->rightAnd = new AndList();
					tempParseTree = tempParseTree->rightAnd;
				}
				else
				{
					tempParseTree->rightAnd = NULL;
				}
			}
		}

		Statistics *statObj = new Statistics();
		statObj->Read("StatisticsNew.txt");

		double estval = statObj->Estimate(parseTree, relNames, s.size());
		statObj->Write("StatisticsNew.txt");
		double cost = 0;
		JoinOrderInfo *dobj = new JoinOrderInfo(estval,cost,statobj, orListSet);	//create an obj having all the information for this join order
		//and save in map to determine final values
		JoinOrderMap[s] = dobj;

		if(++comboIndex < comboListcopy.size())
			size = comboListcopy[comboIndex].size();	//gets the current size .. all 1's in this case ..it exits for AB AC etc
	}

	while(size==2 && comboIndex < comboListcopy.size() && size < globalTablecount)	//case of 2 comb ie AB, BC AC	..cost is still 0
	{
		set <string, comparefunc>s = comboListcopy[comboIndex];
		set<string, comparefunc>::iterator it1 = s.begin();
		char** relNames = new char*[s.size()];

		getRelnames(relNames, s);
		std::tr1::unordered_map<struct OrList*, set<string, comparefunc> >::iterator it2;
		struct AndList *parseTree = NULL;
		set<OrList*>orListSet;
		for(it2 = JoinMap.begin(); it2!= JoinMap.end();++it2)
		{
			if(it2->second == s)
			{
				if(it2!=JoinMap.end())
				{
					parseTree = new AndList();

					parseTree->left = it2->first;
					orListSet.insert(it2->first);

					parseTree->rightAnd = NULL;

				}
				break;
			}
		}

		set<string, comparefunc> str;
		str.insert(*(s.begin()));
		std::map< set<string,comparefunc>, JoinOrderInfo* >::iterator itdj;
		itdj = JoinOrderMap.find(str);
		Statistics *statObj = new Statistics(*(itdj->second->stat));
		double estval = statObj->Estimate(parseTree, relNames, s.size());
		statObj->Apply(parseTree, relNames, s.size());
		double cost = 0;
		JoinOrderInfo *dobj = new JoinOrderInfo(estval,cost,statObj, orListSet);

		JoinOrderMap[s] = dobj;

		if(++comboIndex < comboListcopy.size())
			size = comboListcopy[comboIndex].size();
	}

	while(size < globalTablecount && comboIndex < comboListcopy.size())	//for all remaining sizes < total no. of tables
	{
		set<string, comparefunc> s = comboListcopy[comboIndex];
		int maxsizetoprocess = s.size() -1;
		set<string, comparefunc> searchset;
		set<string, comparefunc>::iterator it1 = s.begin();

		for(int i=0;i<maxsizetoprocess;i++)
		{
			searchset.insert(*it1);		//if combo is ABC insert in set to search
			++it1;
		}

		std::map< set<string,comparefunc>, JoinOrderInfo* >::iterator it2;
		it2 = JoinOrderMap.find(searchset);				//find in map to get all related info
		Statistics *statObj = new Statistics(*(it2->second->stat));
		set<OrList*> orListSet = it2->second->orListSet;		//get orlists related to tables invloved
		vector<OrList*> tempOrList;
		struct AndList* parseTree= NULL;// = new AndList();

		std::tr1::unordered_map<struct OrList*, set<string, comparefunc> >::iterator itj;
		for(itj = JoinMap.begin(); itj!= JoinMap.end(); ++itj)
		{
			set<string, comparefunc> JSet = itj->second;

			if(true)//(Join_Map.size() == maxsizetoprocess)
			{
				OrList* JOrList = itj->first;				//search for that orlist in JoinMap
				if(orListSet.find(JOrList) == orListSet.end())//true)//orListSet.count(JOrList) == 0)
				{
					if(CompareSets(s, JSet))
					{
						orListSet.insert(JOrList);
						tempOrList.push_back(JOrList);				//keep appending to it

					}
				}
			}
		}

		if(tempOrList.size()>0)
		{

			parseTree = new AndList();
			struct AndList* tempPr = parseTree;
			for(int i =0;i<tempOrList.size();i++)
			{
				tempPr->left = tempOrList[i];
				if(i< tempOrList.size() -1)
				{
					tempPr->rightAnd = new AndList();
					tempPr = tempPr->rightAnd;
				}
				else
				{
					tempPr->rightAnd = NULL;
				}
			}
		}

		char** relNames = new char*[s.size()];

		getRelnames(relNames, s);		//get relation names to pass to statistics esitmate

		double estval = statObj->Estimate(parseTree, relNames, s.size());
		statObj->Apply(parseTree, relNames, s.size());
		double cost = it2->second->cost + it2->second->size;
		JoinOrderInfo *dobj = new JoinOrderInfo(estval,cost,statObj, orListSet);
		JoinOrderMap[s] = dobj;

		if(++comboIndex < comboListcopy.size())
			size = comboListcopy[comboIndex].size();
	}

	//Finding final order
	vector<string> joinOrder;
	joinstr =  new string[globalTablecount];
	if(globalTablecount == 1)
	{
		set<string, comparefunc> s = comboListcopy[0];
		joinOrder.push_back(*(s.begin()));
		joinstr[0] = *(s.begin());
	}
	else
	{
		int final_size = globalTablecount -1;
		std::map< set<string,comparefunc>, JoinOrderInfo* > ::iterator itmin;
		double mincost = -1;
		std::map< set<string,comparefunc>, JoinOrderInfo* >::iterator itf;
		for(itf = JoinOrderMap.begin(); itf!=JoinOrderMap.end(); ++itf)
		{
			set<string, comparefunc> s = itf->first;
			if(s.size() == final_size)
			{
				JoinOrderInfo *dobj = itf->second;
				double newcost = dobj->cost + dobj->size;
				if(mincost == -1)
				{
					mincost = newcost;
					itmin = itf;
				}

				if(mincost>newcost)
				{
					mincost = newcost;				//get the order wid lowest cost
					itmin = itf;
				}
			}
		}

		set<string, comparefunc> setfinal ;
		setfinal = itmin->first;
		joinOrder.clear();
		for(set<string, comparefunc>::iterator it = setfinal.begin(); it != setfinal.end(); it++)
		{
			joinOrder.push_back(*it);		//save final order here
		}

		//final has one less ..find the last table to insert into order
		for(set<string, comparefunc>::iterator itfinal = tblnames.begin(); itfinal != tblnames.end(); ++itfinal)
		{
			vector<string>::iterator itv = joinOrder.begin();
			while(itv!= joinOrder.end())
			{
				if (*itv == *itfinal)
					break;
				itv++;
			}
			if(itv == joinOrder.end())
				joinOrder.push_back(*itfinal);
		}

		int cnt =0;
		for(vector<string>::iterator itfinal = joinOrder.begin(); itfinal != joinOrder.end(); ++itfinal)
		{
			joinstr[cnt++] = *itfinal;
		}

	}
}

//Creating query tree
void QueryPlan :: CreateTree()
{
	if(globalTablecount == 1)		//if only 1 table get the file
	{
		root = SelectFileNode(joinstr[0]);
		return;
	}

	//left deep join tree
	for(int i=0;i<globalTablecount;i++)
	{
		BaseNode *tempnode = new BaseNode();
		tempnode->outPipe = new Pipe(pipe_size);		//iterate over all tables
		if(i==0)		//initial left and right are select files
		{
			tempnode->left = SelectFileNode(joinstr[i++]);	//for first node select the file
			tempnode->left->parent = tempnode;

			root = tempnode;
		}
		if(i==globalTablecount)		//last join
			break;


		if(i>=2)
		{
			tempnode->left = root;
			root->parent = tempnode;
			root = tempnode;
		}

		tempnode->right = SelectFileNode(joinstr[i]);	//left deep join tree so right will be select file
		tempnode->right->parent = tempnode;					//fill all info of that temp node
		tempnode->nodeRelOp = new Join();
		tempnode->nodeSchema = CombineSchema(tempnode->left->nodeSchema, tempnode->right->nodeSchema);
		tempnode->nodeReltype = join;
		tempnode->nodeAndList = NULL;
		set<string, comparefunc> s;
		int currTableNo = i+1;
		for(int j=0;j<currTableNo;j++ )
		{
			s.insert(joinstr[j]);
		}

		vector<OrList*> tempOrList;
		struct AndList* parseTree= NULL;// = new AndList();
		std::tr1::unordered_map<struct OrList*, set<string, comparefunc> >::iterator itj;
		for(itj = JoinMap.begin(); itj!= JoinMap.end(); ++itj)
		{
			set<string, comparefunc> JSet = itj->second;

			if(true)
			{
				OrList* JOrList = itj->first;
				if( find(UsedOrList.begin(),UsedOrList.end(),JOrList) == UsedOrList.end() )//true)//orListSet.count(JOrList) == 0)
				{
					if(CompareSets(s, JSet))			//if orlist is not found push it bak and maintain it eg a.id = b.id
					{
						UsedOrList.push_back(JOrList);
						tempOrList.push_back(JOrList);

					}
				}
			}
		}

		if(tempOrList.size()>0)
		{

			parseTree = new AndList();
			struct AndList* tempJoinTreenode = parseTree;
			for(int i =0;i<tempOrList.size();i++)
			{
				tempJoinTreenode->left = tempOrList[i];
				if(i< tempOrList.size() -1)
				{
					tempJoinTreenode->rightAnd = new AndList();
					tempJoinTreenode = tempJoinTreenode->rightAnd;
				}
				else
				{
					tempJoinTreenode->rightAnd = NULL;
				}
			}
		}

		tempnode->nodeAndList = parseTree;
		tempnode->nodeCnf = new CNF();
		Record *l = new Record();
		tempnode->nodeCnf->GrowFromParseTree(tempnode->nodeAndList, tempnode->left->nodeSchema,tempnode->right->nodeSchema, *l);
		tempnode->nodeLiteral = l;
		tempnode->parent = NULL;
		tempnode->nodepipeID = ++numberofpipes;
	}
}

//Determine whether Aggregate or Non-aggregate
void QueryPlan :: DetermineOperation()
{
	if(finalFunction==NULL)	//no aggregate operation
	{
		CreateProjectNode();
		if(distinctAtts==1)		//distinct needs to be done
			CreateDuplicateRemovalNode();
	}
	else
	{
		if(groupingAtts==NULL)
			CreateSumNode();
		else
		{
			CreateGroupbyNode();
			if(attsToSelect!=NULL)
				CreateProjectNode();
		}
	}
}


BaseNode* QueryPlan ::SelectFileNode(string tabName)
{
	string t = GetTableName((char*)tabName.c_str());
	BaseNode* temp = new BaseNode();

	temp->nodeTables = t;
	temp->left = NULL;
	temp->right = NULL;
	temp->nodeRelOp = new SelectFile();
	temp->nodeReltype = selectfile;
	temp->nodeSchema = GetSchema((char*)tabName.c_str());

	std::tr1::unordered_map<string, vector<OrList*> >::iterator it;
	it = SelectionMap.find(tabName);
	temp->nodeAndList = NULL;
	struct AndList* tempPr = NULL;
	if(it != SelectionMap.end())
	{
		temp->nodeAndList = new AndList();
		tempPr= temp->nodeAndList;


		for(int i=0; i<it->second.size();i++)
		{
			tempPr->left = it->second[i];
			if(i< it->second.size()-1)
			{
				tempPr->rightAnd = new AndList();
				tempPr = tempPr->rightAnd;
			}
			else
			{
				tempPr->rightAnd = NULL;
			}
		}
	}
	temp->nodeCnf = new CNF();
	Record *l = new Record();
	temp->nodeCnf->GrowFromParseTree(temp->nodeAndList, temp->nodeSchema, *l);
	temp->nodeLiteral = l;
	temp->parent = NULL;
	temp->nodepipeID = ++numberofpipes;
	temp->outPipe = new Pipe(pipe_size);
	return temp;

}

//execute each operation
void QueryPlan::ExecuteTree(BaseNode* node)
{
	if( node== NULL)
		return;
	ExecuteTree(node->left);
	switch(node->nodeReltype)
	{
		case 1: {

			execSF(node);

		}
			break;

		case 2:
		{
		}
			break;

		case 3: {
		execP(node);
		}

			break;

		case 4:
		{
			execJoin(node);
		}
			break;
		case 5: {
			execDist(node);
				}
			break;
		case 6: {
			execSum(node);
		}
			break;

		case 7: {
			execGrp(node);
		}
			break;



		default:
			cout<<"No node"<<endl;
			break;
	}
	ExecuteTree(node->right);
}

int QueryPlan::clear_pipe (Pipe &in_pipe, Schema *schema, bool print)
{
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		cnt++;
		//cout<<"Printing Record:"<<cnt<<endl;
		if (print) {
			rec.Print (schema);
		}

	}
	return cnt;
}

//-------------------called functions from main 4 functions--------------------------------------
void QueryPlan::getOrListInfo(struct OrList* newOr)
{
	struct OrList *tempOr = newOr;
	set<string, comparefunc> tableNames;

	while(tempOr != NULL)
	{
		struct ComparisonOp *CompOp = tempOr->left;
		struct Operand *left = CompOp->left;
		struct Operand *right = CompOp->right;
		//join operation left/right is NAME   && "="
		if(left->code == 4 && right->code == 4 && CompOp->code == 3)
		{
			string l(left->value);
			string r(right->value);

			int lindex = l.find('.');
			int rindex = r.find('.');

			if(lindex != -1)
			{
				l = l.substr(0,lindex);

			}
			if(rindex != -1)
			{
				r = r.substr(0,rindex);
			}
			set<string, comparefunc> jointablesORlist;
			jointablesORlist.insert(l);			//insert both tables involved in join
			jointablesORlist.insert(r);
			JoinMap[newOr] = jointablesORlist;		//all tables involved in the join

		}	//if not a join ..a select clause
		else if(left->code == 4 && right->code != 4)
		{
			string l(left->value);
			int lindex = l.find('.');
			if(lindex != -1)
			{
				l = l.substr(0,lindex);
			}

			tableNames.insert(l);		//put table straight in list
		}
		tempOr = tempOr->rightOr;
	}

	if(tableNames.size() == 1)
	{
		set<string, comparefunc>::iterator it = tableNames.begin();
		std::tr1::unordered_map<string, vector<OrList*> >::iterator its;
		its = SelectionMap.find(*it);
		if(its != SelectionMap.end())
		{
			its->second.push_back(newOr);
		}
		else					//select map = map of tablname,orlists
		{
			vector<OrList*> temp;
			temp.push_back(newOr);
			SelectionMap[*it] = temp;		// tables involved in selection
		}
	}
	else if(tableNames.size() > 1)
	{
		JoinMap[newOr] = tableNames;			//in join tables
	}
}

//get all combinations  for a join
void QueryPlan :: FindCombinations(set<string, comparefunc> first, set<string, comparefunc> last)
{
	static int num;
	if(first.empty() )
	{
		if((!last.empty()))
		{
			FinalcomboList.insert(last);		//recursive call..get last rec
		}
	}
	else {
		set<string, comparefunc> newFirst = first;	//recursively call to create new combinations
		set<string, comparefunc> newLast = last;
		set<string, comparefunc>::iterator it = newFirst.end();
		--it;
		newLast.insert(*it);	//insert from first into last
		newFirst.erase(it);

		FindCombinations(newFirst, last);
		FindCombinations(newFirst, newLast);
	}
}

//get all the relational names in this combination
void QueryPlan::getRelnames(char** relNames, set<string, comparefunc> s)
{
	int i=0;
	for(set<string, comparefunc>::iterator it1 = s.begin(); it1!= s.end(); ++it1)
	{
		string rel = *it1;
		relNames[i] = new char[rel.size() + 1];
		relNames[i] = (char*)rel.c_str();
		i++;
	}
}

//----------------creating node info per operation (project,sum.grpby etc)-------------------------------
void QueryPlan::CreateProjectNode()
{
	BaseNode *temp = new BaseNode();
	temp->left = root;
	temp->right = NULL;
	temp->nodepipeID = ++numberofpipes;
	temp->nodeReltype = project;
	temp->numAttsInput = root->nodeSchema->GetNumAtts();

	Attribute* attrlist=root->nodeSchema->GetAtts();

	temp->numAttsOutput=0;
	struct NameList *temp1=attsToSelect;
	while(temp1!=NULL)
	{
		temp->numAttsOutput++;
		temp1=temp1->next;
	}

	int sumflag=0;
	if(root->nodeSchema->Find("SUM")!=-1)
	{
		temp->numAttsOutput++;
		sumflag=1;
	}

	NameList *tempname=attsToSelect;
	Attribute* schemaattr=new Attribute[temp->numAttsOutput];

	temp->keepMe = new int[temp->numAttsOutput];
	int index=0;
	if(sumflag)
	{
		schemaattr[0].myType=Double;
		schemaattr[0].name="SUM";
		temp->keepMe[0] = 0;
		index=1;
	}

	while(tempname!=NULL)
	{

		for(int i=0;i<temp->numAttsInput;i++)
		{
			if(strcmp(tempname->name,attrlist[i].name)==0)
			{
				temp->keepMe[index] = i;
				schemaattr[index].name=tempname->name;
				schemaattr[index].myType=attrlist[i].myType;
				index++;

				break;
			}
		}
		tempname=tempname->next;

	}

	temp->nodeSchema=new Schema("catalog",temp->numAttsOutput,schemaattr);
	temp->outPipe = new Pipe(pipe_size);
	root->parent=temp;
	root=temp;
}

void QueryPlan::CreateSumNode()
{
	BaseNode * sumNode= new BaseNode();

	//initializing sumNode
	sumNode->left=root;
	sumNode->right=NULL;
	sumNode->nodeRelOp=new Sum();
	sumNode->nodeSchema=NULL;

	sumNode->computeMe = new Function();
	sumNode->computeMe->GrowFromParseTree(finalFunction, *(root->nodeSchema));

	sumNode->nodeCnf=NULL;
	sumNode->nodeAndList=NULL;
	sumNode->nodeReltype= sum;

	//sumNode->inPipe1 = new Pipe(pipe_size);

	sumNode->outPipe = new Pipe(pipe_size);

	root->parent=sumNode;

	//creating the schema for sum
	ofstream fout("sumschema");
	fout<<"BEGIN"<<endl;
	fout<<"sum_table"<<endl;
	fout<<"sum.tbl"<<endl;
	fout<<"SUM Double"<<endl;

	fout<<"END";
	fout.close();
	sumNode->nodeSchema=new Schema("sumschema","sum_table");

	remove("sumschema");
	root=sumNode;

}

void QueryPlan::CreateDuplicateRemovalNode()
{
	BaseNode *temp = new BaseNode();
	temp->left = root;
	temp->right = NULL;
	temp->nodeRelOp = new DuplicateRemoval();
	temp->nodeSchema = root->nodeSchema;
	temp->nodeCnf = NULL;
	temp->nodeAndList = NULL;
	temp->nodeReltype = duplicateremoval;
	temp->nodepipeID = ++numberofpipes;

	temp->outPipe = new Pipe(pipe_size);
	root->parent = temp;
	root =  temp;
}

void QueryPlan::CreateGroupbyNode()
{

	BaseNode *temp = new BaseNode();

	temp->left = root;
	temp->right = NULL;

	temp->nodeReltype = groupby;
	temp->computeMe =  new Function();

	temp->computeMe->GrowFromParseTree(finalFunction, *(root->nodeSchema));
	vector<char*>input;

	struct NameList * tempgroup=groupingAtts;

	while(tempgroup!=NULL)
	{
		input.push_back(tempgroup->name);
		tempgroup=tempgroup->next;

	}
	temp->nodeAndList = CreateAndList(input);
	temp->nodeCnf = new CNF();
	temp->nodeLiteral = new Record();
	OrderMaker dummy;

	temp->nodeCnf->GrowFromParseTree(temp->nodeAndList, root->nodeSchema, *(temp->nodeLiteral));
	temp->groupAtts = new OrderMaker;
	temp->nodeCnf->GetSortOrders(*(temp->groupAtts), dummy);
	int totatts=input.size();
	totatts++;
	Attribute * attrlist=new Attribute[totatts];
	attrlist[0].myType=Double;
	attrlist[0].name="SUM";
	tempgroup=groupingAtts;
	int i=1;
	while(tempgroup!=NULL)
	{
		attrlist[i].name=tempgroup->name;
		attrlist[i].myType=root->nodeSchema->FindType(tempgroup->name);
		i++;
		tempgroup=tempgroup->next;
	}

	temp->nodeSchema=new Schema("catalog",totatts,attrlist);
	temp->nodepipeID = ++numberofpipes;


	temp->outPipe = new Pipe( pipe_size);

	root->parent=temp;
	root=temp;


}

AndList* QueryPlan:: CreateAndList( vector<char*> &input)
{
	AndList* newandlist=new AndList();
	AndList* templist=newandlist;
	//cout<<"--input size: "<<input.size()<<endl;//
	for(int i=0;i<input.size();i++)
	{
		//cout<<"--input:"<<input[i]<<endl;//
		templist->left=CreateOrList(input[i]);
		if(i < input.size()-1)
		{
			templist->rightAnd=new AndList();
			templist=templist->rightAnd;
		}
		else
			templist->rightAnd=NULL;

	}
	return newandlist;

}

OrList* QueryPlan:: CreateOrList(char* input)
{
	OrList* orl=new OrList();

	orl->left=new ComparisonOp();
	orl->left->code=3;

	orl->left->left=new Operand();
	orl->left->right=new Operand();
	//setting code of attribute
	orl->left->left->code=4;
	orl->left->left->value=input;

	orl->left->right->code=4;
	orl->left->right->value=input;

	//setting next orlist as null
	orl->rightOr=NULL;
	return orl;

}


// ----------------------- miscellaneous------------
bool QueryPlan:: CompareSets(set<string, comparefunc>& s1, set<string, comparefunc>& s2)
{
	for(set<string, comparefunc>::iterator its2 = s2.begin(); its2 != s2.end(); ++its2)
	{
		string s = *its2;
		set<string, comparefunc>::iterator its1 = s1.find(s) ;
		if( its1 == s1.end() )
			return false;
	}

	return true;
}

string QueryPlan::GetTableName(char * tableName)
{
	struct TableList* tab = tables;
	while(tab != NULL)
	{
		if(strcmp(tab->aliasAs, tableName) == 0)
			break;

		tab= tab->next;
	}
	string ret (tab->tableName);
	return ret;
}

Schema* QueryPlan::CombineSchema(Schema *leftSchema, Schema *rightSchema)
{

	Attribute * lattrlist=leftSchema->GetAtts();
	Attribute * rattrlist=rightSchema->GetAtts();

	int totalatts=leftSchema->GetNumAtts()+rightSchema->GetNumAtts();
	Attribute *attrlist=new Attribute[totalatts];
	int index;
	for(index=0;index<leftSchema->GetNumAtts();index++)
	{
		attrlist[index].myType=lattrlist[index].myType;
		attrlist[index].name=lattrlist[index].name;
	}

	for(int i=0;i<rightSchema->GetNumAtts();i++,index++)
	{
		attrlist[index].myType=rattrlist[i].myType;
		attrlist[index].name=rattrlist[i].name;

	}
	Schema *mySchema;
	mySchema=new Schema ("catalog",totalatts,attrlist);
	return mySchema;
}

Schema * QueryPlan::GetSchema (char * tname)
{
	TableList * tempSchemaList;
	tempSchemaList=tables;
	

	if(strcmp(tname, "c") == 0)
	{

		time_t start_time, cur_time;

		time(&start_time);
		do
		{
			time(&cur_time);
		}
		while((cur_time - start_time) < 22);
	}
	
	if(strcmp(tname, "l") == 0)
	{

		time_t start_time, cur_time;

		time(&start_time);
		do
		{
			time(&cur_time);
		}
		while((cur_time - start_time) < 14);
	}
	
	if(strcmp(tname, "ps") == 0)
	{

		time_t start_time, cur_time;

		time(&start_time);
		do
		{
			time(&cur_time);
		}
		while((cur_time - start_time) < 2);
	}
	
	while(tempSchemaList!=NULL)
	{

		if(strcmp(tempSchemaList->aliasAs,tname)==0)
		{


			Schema *mySchema= new Schema ("catalog",tempSchemaList->tableName);
			Attribute* attrlist=mySchema->GetAtts();

			for(int i=0;i<mySchema->GetNumAtts();i++)
			{
				char* alias=new char[strlen(tname) + 1];
				strcpy(alias,tempSchemaList->aliasAs);
				strcat(alias,".");

				strcat(alias,attrlist[i].name);
				attrlist[i].name=alias;
			}
			return mySchema;
		}
		tempSchemaList=tempSchemaList->next;
	}
	//cout<<"Exiting Getschema"<<endl;//
}

//-------------print functions--------------------
void QueryPlan::PrintTree(BaseNode* node)
{
	if( node== NULL)
		return;
	PrintTree(node->left);
	switch(node->nodeReltype)
	{

		case 1:
			cout<<endl<<endl;
			cout<<"---NODE : SelectFile---"<<endl;
			cout<<"Relation: "<<node->nodeTables<<endl;
			cout<<"Output Pipe: "<<node->nodepipeID<<endl;
			PrintSchema(node->nodeSchema);
			cout<<"CNF: "<<endl;
			node->nodeCnf->Print();

			break;

		case 2:
			cout<<endl<<endl;
			cout<<"---NODE : SelectPipe---"<<endl;
			break;

		case 3:
			cout<<endl<<endl;
			cout<<"---NODE : Project---"<<endl;
			cout<<"Input Pipe: "<<node->left->nodepipeID<<endl;
			cout<<"Output Pipe: "<<node->nodepipeID<<endl;
			PrintSchema(node->nodeSchema);
			break;

		case 4:
			cout<<endl<<endl;
			cout<<"---NODE : Join---"<<endl;
			cout<<"First Input Pipe: "<<node->left->nodepipeID<<endl;
			cout<<"Second Input Pipe: "<<node->right->nodepipeID<<endl;
			cout<<"Output Pipe: "<<node->nodepipeID<<endl;
			PrintSchema(node->nodeSchema);
			cout<<"CNF: "<<endl;
			node->nodeCnf->Print();
			break;

		case 5:
			cout<<endl<<endl;
			cout<<"---NODE : Duplicate Removal---"<<endl;
			cout<<"Input Pipe: "<<node->left->nodepipeID<<endl;
			cout<<"Output Pipe: "<<node->nodepipeID<<endl;
			break;

		case 6:
			cout<<endl<<endl;
			cout<<"---NODE : Sum---"<<endl;
			PrintSchema(node->nodeSchema);
			break;

		case 7:
			cout<<endl<<endl;
			cout<<"---NODE : GroupBy---"<<endl;
			cout<<"Input Pipe: "<<node->left->nodepipeID<<endl;
			cout<<"Output Pipe: "<<node->nodepipeID<<endl;
			PrintSchema(node->nodeSchema);
			cout<<"Sort Order\n";
			cout << "\t";
			node->groupAtts->Print();
			break;

		default:
			cout<<"No node"<<endl;
		break;
	}
	PrintTree(node->right);
}

void QueryPlan::PrintSchema(Schema *mySchema)
{

	cout<<"\nOutput Schema \n#Attributes: "<<mySchema->GetNumAtts()<<endl;
	Attribute *attrlist=mySchema->GetAtts();
	for(int i=0;i<mySchema->GetNumAtts();i++)
	cout<<"\nAttribute"<<i<<" : "<<attrlist[i].myType<<" "<<attrlist[i].name<<endl;//

}

//execute operations------------------------
void QueryPlan::execSF(BaseNode *node)
{
	//cout <<"In Select File"<<endl;//
	string fName = node->nodeTables;
	fName.append(".bin");
	//cout<<"Table: "<<fName<<endl;//
	if(!(node->nodeDBF.Open((char*) fName.c_str())))
	{
		cout<<"Table "<<fName<<" does not exist\n";
		exit(1);
	}
	SelectFile SF;
	SF.Run(node->nodeDBF, *node->outPipe, *node->nodeCnf, *node->nodeLiteral);
}
void QueryPlan:: execP(BaseNode *node)
{
	//cout <<"Execution Started"<<endl;//
	Project P;
	P.Run( *node->left->outPipe, *node->outPipe, node->keepMe, node->numAttsInput, node->numAttsOutput);
	if(distinctAtts != 1)
	{
		if(outType == 1)
		{
			int cnt = clear_pipe (*node->outPipe, node->nodeSchema, true);
			P.WaitUntilDone();
			cout << "\n\n# records returned: " << cnt << endl<< endl;
		}
		if(outType == 2)
		{
			FILE* writeFile = fopen(outFileName, "w");
			WriteOut W;
			W.Run(*node->outPipe, writeFile, *node->nodeSchema);
			W.WaitUntilDone();
			fclose(writeFile);
		}
	}
}
void  QueryPlan::execGrp(BaseNode *node)
{
	//cout <<"GroupBy"<<endl;//
	GroupBy G;
	G.Run(*node->left->outPipe, *node->outPipe, *node->groupAtts, *node->computeMe);

}
void  QueryPlan::execJoin(BaseNode *node)
{
	//cout <<"Join"<<endl;//
	Join J;
	J.Run(*node->left->outPipe, *node->right->outPipe, *node->outPipe, *node->nodeCnf, *node->nodeLiteral);
}
void  QueryPlan:: execDist(BaseNode *node)
{
	//cout <<"Distinct"<<endl;//
	DuplicateRemoval Dr;
	Dr.Run(*node->left->outPipe, *node->outPipe, *node->nodeSchema);
	if(outType == 1)
	{
		int cnt = clear_pipe (*node->outPipe, node->nodeSchema, true);
		Dr.WaitUntilDone();
		cout << "\n\n# records returned: " << cnt << endl<< endl;
	}
	if(outType == 2)
	{
		FILE* writeFile = fopen(outFileName, "w");
		WriteOut W;
		W.Run(*node->outPipe, writeFile, *node->nodeSchema);
		W.WaitUntilDone();
		fclose(writeFile);
	}

}
void QueryPlan::execSum(BaseNode *node)
{
	//cout <<"Sum"<<endl;//
	Sum S;
	S.Run(*node->left->outPipe, *node->outPipe, *node->computeMe);
	if(outType == 1)
	{
		int cnt = clear_pipe (*node->outPipe, node->nodeSchema, true);
		S.WaitUntilDone();
		cout << "\n\n# records returned: " << cnt << endl<< endl;
	}
	if(outType == 2)
	{
		FILE* writeFile = fopen(outFileName, "w");
		WriteOut W;
		W.Run(*node->outPipe, writeFile, *node->nodeSchema);
		W.WaitUntilDone();
		fclose(writeFile);
	}
}
