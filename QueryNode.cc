#include "QueryNode.h"

//----------------creating node info per operation (project,sum.grpby etc)-------------------------------
void QueryNode::CreateProjectNode(QueryNode *root,int n)
{
	QueryNode *temp = new QueryNode();
	temp->left = root;
	temp->right = NULL;
	temp->nodepipeID = n;
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
	//temp->inPipe1 = new Pipe(pipe_size);
	temp->outPipe = new Pipe(2000);
	root->parent=temp;
	root=temp;
}

void QueryNode::CreateSumNode(QueryNode *root)
{
	QueryNode * sumNode= new QueryNode();

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
	sumNode->outPipe = new Pipe(2000);
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

void QueryNode::CreateDuplicateRemovalNode(QueryNode *root,int n)
{
	QueryNode *temp = new QueryNode();
	temp->left = root;
	temp->right = NULL;
	temp->nodeRelOp = new DuplicateRemoval();
	temp->nodeSchema = root->nodeSchema;
	temp->nodeCnf = NULL;
	temp->nodeAndList = NULL;
	temp->nodeReltype = duplicateremoval;
	temp->nodepipeID = n;
	//temp->inPipe1 = new Pipe(pipe_size);
	temp->outPipe = new Pipe(2000);
	root->parent = temp;
	root =  temp;
}

void QueryNode::CreateGroupbyNode(QueryNode *root,int n)
{

	QueryNode *temp = new QueryNode();
	temp->left = root;
	temp->right = NULL;
	temp->nodeReltype = groupby;
	temp->computeMe =  new Function();
	temp->computeMe->GrowFromParseTree(finalFunction, *(root->nodeSchema));
	vector<char*>input;

	struct NameList * tempgroup=groupingAtts;

	while(tempgroup!=NULL)
	{
		//strcat(input,tempgroup->name);
		input.push_back(tempgroup->name);
		tempgroup=tempgroup->next;

	}
	temp->nodeAndList = CreateAndList(input);
	temp->nodeCnf = new CNF();
	temp->nodeLiteral = new Record();
	OrderMaker dummy;

	//cout<<"GB 2\n";
	temp->nodeCnf->GrowFromParseTree(temp->nodeAndList, root->nodeSchema, *(temp->nodeLiteral));
	//cout<<"GB 3\n";
	temp->groupAtts = new OrderMaker;
	temp->nodeCnf->GetSortOrders(*(temp->groupAtts), dummy);
	//cout<<"GB 4\n";
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
	//cout<<"GB 5\n";
	temp->nodeSchema=new Schema("catalog",totatts,attrlist);
	temp->nodepipeID = n;

	//temp->inPipe1 = new Pipe( pipe_size );
	temp->outPipe = new Pipe(2000);

	root->parent=temp;
	root=temp;

	//cout<<"GB End\n";

}

AndList* QueryNode:: CreateAndList( vector<char*> &input)
{
	AndList* newandlist=new AndList();
	AndList* templist=newandlist;
	//cout<<"input size: "<<input.size()<<endl;
	for(int i=0;i<input.size();i++)
	{
		//cout<<"input:"<<input[i]<<endl;
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

OrList* QueryNode:: CreateOrList(char* input)
{
	OrList* orl=new OrList();
	orl->left=new ComparisonOp();
	orl->left->code=3;

	orl->left->left=new Operand();
	orl->left->right=new Operand();
	orl->left->left->code=4;
	orl->left->left->value=input;
	//cout<<"COR 2\n";
	orl->left->right->code=4;
	orl->left->right->value=input;

	//setting next orlist as null
	orl->rightOr=NULL;
	return orl;

}
