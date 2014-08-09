#include "Statistics.h"

Statistics::Statistics()
{

}
Statistics::Statistics(Statistics &copyMe)
{
	std::tr1::unordered_map<string, TableDetails>::iterator it1;
	std::tr1::unordered_map<string, int>::iterator it2;
	for(it1 = copyMe.RelationMap.begin(); it1 != copyMe.RelationMap.end(); it1++){
		struct TableDetails table;
		RelationMap[it1->first] = table;
		RelationMap[it1->first].partition = it1->second.partition;
		RelationMap[it1->first].numtuples = it1->second.numtuples;

		for(it2 = it1->second.AttrMap.begin(); it2 != it1->second.AttrMap.end(); it2++){
			RelationMap[it1->first].AttrMap[it2->first] = it2->second;
		}

	}
}
Statistics::~Statistics()
{
}
/*
 * This operation adds another base relation into the structure. The parameter set tells the
statistics object what the name and size of the new relation is (size is given in terms of the
number of tuples)
 */
void Statistics::AddRel(char *relName, int numTuples)
{
	//cout<<"AddRel"<<endl;//

	string name= string(relName);
	struct TableDetails table;
	table.numtuples = numTuples;
	if(RelationMap.count(name) == 0)
	{
		//cout<< " in rel 1 " << name << " " << endl;
		RelationMap.insert(pair<string,TableDetails>(name,table));
		RelationMap[name].numtuples = numTuples;

	}
	else
	{
		RelationMap[name] = table;
		RelationMap[name].numtuples = numTuples;
	}

	std::tr1::unordered_map<string, TableDetails>::iterator it1;
	/*	for(it1 = RelationMap.begin(); it1 != RelationMap.end(); it1++){
		cout<< " IN ADDREL " << it1->first << " " <<  RelationMap[it1->first].numtuples<< endl;
	}*/
}

/*
 * This operation adds an attribute to one of the base relations in the structure. The
parameter set tells the Statistics object what the name of the attribute is, what
relation the attribute is attached to, and the number of distinct values that the relation has
for that particular attribute. If numDistincts is initially passed in as a –1, then the
number of distincts is assumed to be equal to the number of tuples in the associated
relation.
 */
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	//cout<<"AddAtt"<<endl;//
	string rname = string(relName);
	string aname = string(attName);
	//cout<< " in add att  " <<  rname << " " <<  aname  << " " << numDistincts;
	if(RelationMap.count(rname)!=0)
	{
		struct TableDetails table;
		//RelationMap[rname] = table;
		if(RelationMap[rname].AttrMap.count(aname) == 0)
		{
			RelationMap[rname].AttrMap.insert(pair<string,int>(aname,numDistincts));

		}
		else
		{
			RelationMap[rname].AttrMap[aname] = numDistincts;

		}
	}

	if(AttrRelMap.count(aname)==0)
	{
		//    cout << " input in attr " <<AttrRelMap.count(aname)<< " " <<  aname<<endl;
		AttrRelMap[aname] = rname;
	}

	std::tr1::unordered_map<string, int>::iterator it1;
	std::tr1::unordered_map<string, TableDetails>::iterator it2;
	for(it2 =RelationMap.begin(); it2 != RelationMap.end(); it2++){
		for(it1 =RelationMap[it2->first].AttrMap.begin(); it1 != RelationMap[it2->first].AttrMap.end(); it1++){
	//			cout<< " IN addatt " << it1->first << " " << it1->second << endl;
		}
	}
	std::tr1::unordered_map<string, string>::iterator it;
	for(it = AttrRelMap.begin(); it != AttrRelMap.end(); it++)
	{
	//	       cout << "ATTRIBUTEMAP "  << AttrRelMap[it->first] << endl;
	}
//	cout << " in add att " << AttrRelMap.size() <<endl;
}

/*
 * This operation produces a copy of the relation (including all of its attributes and all of its
statistics) and stores it under the new name.
 */
void Statistics::CopyRel(char *oldName, char *newName)
{
	//cout<<"CopyRel"<<endl;//
	string oname = string(oldName);
	string nname = string(newName);
	if((RelationMap.count(oldName) != 0)&&(RelationMap.count(newName) == 0)){
		struct TableDetails table;
		RelationMap[nname] = table;
		RelationMap[nname].numtuples =RelationMap[oname].numtuples;
		std::tr1::unordered_map<string, int>::iterator it;
		for(it = RelationMap[oname].AttrMap.begin(); it != RelationMap[oname].AttrMap.end(); it++){
			RelationMap[nname].AttrMap[it->first] = it->second;
		}
	}
	//std::tr1::unordered_map<string, int>::iterator it1;
	std::tr1::unordered_map<string, TableDetails>::iterator it2;
	for(it2 =RelationMap.begin(); it2 != RelationMap.end(); it2++){
		//	for(it1 =RelationMap[it2->first].AttrMap.begin(); it1 != RelationMap[it2->first].AttrMap.end(); it1++){
		//	cout<< " AFTER COPY REL " << it2->first << " "  << RelationMap[it2->first].numtuples<< endl;
		//	}
	}

}

void Statistics::Read(char *fromWhere)
{
	//cout<<"Read"<<endl;//
	ifstream reader;
	reader.open(fromWhere);

	int relsize = 0;
	int attsize = 0;
	string relname;
	string attname;
	int disttuples = 0;
	reader>>relsize;
//	cout<<"In Read RELATIONMAP: relsize is "<<relsize<<endl;
	if(relsize != 0){
		for(int i=0;i<relsize;i++)
		{
			struct TableDetails Table;
			reader>>relname;
			//		cout<<"In Read: relname is "<<relname<<endl;
			RelationMap[relname] = Table;
			reader>>RelationMap[relname].numtuples;
			//		cout<<"In Read: RelationMap[relname].numtuples is "<<RelationMap[relname].numtuples<<endl;
			reader>>RelationMap[relname].partition;
			//		cout<<"In Read: RelationMap[relname].partition is "<<RelationMap[relname].partition<<endl;
			reader>>attsize;
			//		cout<<"In Read: attsize is "<<attsize<<endl;
			for(int i=0;i<attsize;i++)
			{
				reader>>attname;
				reader>>disttuples;
				RelationMap[relname].AttrMap[attname]=disttuples;
				//		cout<<"In Read: attname is "<<attname<<" disttuples is "<<disttuples<<" added value is "<<RelationMap[relname].AttrMap[attname]<<endl;
			}
		}
	}
	reader>>relsize;
//	cout<<"In Read JOINMAP : relsize is "<<relsize<<endl;
	if(relsize != 0){
		for(int i=0; i<relsize; i++){
			int id;
			reader>>id;
			vector<string> temp;
			JoinMap[id] = temp;
			int vectsize = 0;
			reader>>vectsize;
			//		cout<<"In Read: id is "<<id<<" vectsize is "<<vectsize<<endl;
			for(int j=0; j<vectsize; j++){
				string tempstring;
				reader>>tempstring;
				//			cout<<"In Read: tempstring is "<<tempstring<<endl;
				JoinMap[id].push_back(tempstring);
			}
		}
	}

	reader>>relsize;
	//cout<<"In Read RELJOIN: relsize is "<<relsize<<endl;
	if(relsize != 0){
		for(int i=0; i<relsize; i++){
			string tempstring;
			int value = 0;
			reader>>tempstring;
			reader>>value;
			RelJoin[tempstring] = value;
			//		cout<<"In Read: tempstring is "<<tempstring<<" value is "<<value<<" inserted is "<<RelJoin[tempstring]<<endl;
		}
	}

	reader>>relsize;
//	cout<<"In Read ATTRELMAP: relsize is "<<relsize<<endl;
	if(relsize != 0){
		for(int i=0; i<relsize; i++){
			string tempstring;
			string tempvalue;
			reader>>tempstring;
			reader>>tempvalue;
			AttrRelMap[tempstring] = tempvalue;
//				cout<<"In Read: tempstring is "<<tempstring<<" tempvalue is "<<tempvalue<<" inserted is "<<AttrRelMap[tempstring]<< " count  " << i<< endl ;
		}
	}
//	cout << " size XXXXXXXX " << AttrRelMap.size() <<endl;

/*	reader>>relsize;
	cout<<"In Read JOINSET: relsize is "<<relsize<<endl;
	if(relsize != 0){
		for(int i=0; i<relsize; i++){
			string tempstring;
			reader>>tempstring;
			//	cout << " XXXXXXXX " << tempstring<<endl;
			joinset.insert(tempstring);
			//		cout<<"In Read: tempstring is "<<tempstring<<endl;
		}
	}*/
	reader.close();
}

void Statistics::Write(char *fromWhere)
{
	//cout<<"Write"<<endl;//
	ofstream writer;
	writer.open(fromWhere);

	//Write RelationMap
	writer<<RelationMap.size()<<endl;
//	cout << " write " <<RelationMap.size()<<endl;
	std::tr1::unordered_map<string, TableDetails>::iterator it;
	std::tr1::unordered_map<string, int>::iterator it2;
	for(it = RelationMap.begin(); it != RelationMap.end(); it++){
		writer<<it->first<<endl;
		writer<<it->second.numtuples<<endl;
		writer<<it->second.partition<<endl;
		writer<<it->second.AttrMap.size()<<endl;
		for(it2 = it->second.AttrMap.begin(); it2 != it->second.AttrMap.end(); it2++){
			writer<<it2->first<<endl;
			writer<<it2->second<<endl;
		}
	}
	//Write JoinMap
//	cout << " write " <<JoinMap.size()<<endl;
	writer<<JoinMap.size()<<endl;
	std::tr1::unordered_map<int, vector<string> >::iterator it3;
	vector<string>::iterator it4;

	for(it3 = JoinMap.begin(); it3 != JoinMap.end(); it3++){
		writer<<it3->first<<endl;
		writer<<it3->second.size()<<endl;
		for(it4 = it3->second.begin(); it4 != it3->second.end(); it4++){
			writer<<*it4<<endl;
		}
	}

	//Write RelJoin
//	cout << " write " <<RelJoin.size()<<endl;
	writer<<RelJoin.size()<<endl;
	std::tr1::unordered_map<string, int>::iterator it5;

	for(it5 = RelJoin.begin(); it5 != RelJoin.end(); it5++){
		writer<<it5->first<<endl;
		writer<<it5->second<<endl;
	}

	//Write AttrRelMap
//	cout << " write " <<AttrRelMap.size()<<endl;
	writer<<AttrRelMap.size()<<endl;
	std::tr1::unordered_map<string, string>::iterator it6;

	for(it6=AttrRelMap.begin(); it6 != AttrRelMap.end(); it6++){
		writer<<it6->first<<endl;
		writer<<it6->second<<endl;
//		cout << " written attrel " << it6->first << " " << it6->second << endl;
	}

	//Write joinset
/*	writer<<joinset.size()<<endl;
	set<string>::iterator it7;
	for(it7 = joinset.begin(); it7 != joinset.end(); it7++){
		writer<<*it7<<endl;
	}*/

	writer.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	//cout<<"Apply"<<endl;//
	bool no_error =true;
	joinset.clear();
	vector<string> AttsToEstimate;
	no_error = chkParsetree1(parseTree, relNames, numToJoin,AttsToEstimate);    //check all parse tree attr in rel names attr
	if(!no_error)
	{
		//cout<<" in Apply Error in parsetree check " <<endl;
		exit(0);
	}

	no_error =chkParsetree2(parseTree, relNames, numToJoin);    //check partitions are valid
	if(!no_error)
	{
		//cout<<" In Apply Error in partition check " <<endl;
		exit(0);
	}
	double numTuples = Estimate(parseTree, relNames, numToJoin);    //get estimate
	//update required tables
	set<string>::iterator it;
	std::tr1::unordered_map<string, int>::iterator it2;
	bool partition_exists = false;
	int newID =0;

	//check in realtion map wether singleton  --- TO DETERMINE NEWID
	for(it = joinset.begin(); it != joinset.end(); it++){
		int partition = RelationMap[*it].partition;
		if(partition == 1 )
		{

			partition_exists = true;
			istringstream convert(*it);
			if ( !(convert >> newID) ) //give the value to 'Result' using the characters in the stream
				newID = 0;
			//cout<<"NAAAAAAAAAAAAAME "<< *it<<" "<<newID<<endl;
			break;
		}
	}

	if(!partition_exists)
	{
		for(it2 = RelJoin.begin(); it2 != RelJoin.end(); it2++){
			if(newID<it2->second)
				newID = it2->second;
		}
		newID++;


	}

	//first insert new values in RelJoin
	for(it = joinset.begin(); it != joinset.end(); it++){
		int partition = RelationMap[*it].partition;
		if(partition == 0 )
		{
			RelJoin[*it] = newID;
		}
	}
	//update JoinMap


	if(JoinMap.count(newID)==0)
	{
		vector<string> tablenames;
		for(it = joinset.begin(); it != joinset.end(); it++){

			tablenames.push_back( *it);

		}

		JoinMap[newID]  = tablenames;
	}
	else
	{
		for(it = joinset.begin(); it != joinset.end(); it++){
			string temp = string(*it);

			if(!has_any_digits(temp))
				JoinMap[newID].push_back(temp);
			else{
				vector<string>::iterator it1;
				int temp1;

				istringstream convert(*it);
				if ( !(convert >> temp1) ){ //give the value to 'Result' using the characters in the stream
					//cout<<"Cannot convert the string to integer somehow"<<endl;
					exit(0);
				}

				for(it1 = JoinMap[temp1].begin(); it1 != JoinMap[temp1].end(); it1++){
					//cout<<"!!!!!!!!!!!IM here!"<<*it1<<endl;
					vector<string>::iterator tempit;
					vector<string> tempvector;
					for(tempit = JoinMap[newID].begin(); tempit != JoinMap[newID].end(); tempit++){
						//cout<<*tempit<<endl;
						tempvector.push_back(*tempit);
					}
					string tempstring = *it1;
					//cout<<tempstring<<endl;
					//JoinMap[newID].push_back(tempstring);
					JoinMap[newID] = tempvector;
					RelJoin[*it1] = newID;

				}
			}
		}

	}

	// first combine the tables in RelationMap
	//Update partition to 1
	//Copy the map
	//copy the numtuples? - comes from the estimate
	std::tr1::unordered_map<string, TableDetails>::iterator it5;
	set<string>::iterator it3;
	ostringstream convert;
	convert << newID;      // insert the textual representation of 'Number' in the characters in the stream
	string newkey = convert.str();
	TableDetails table;

	//RelationMap[newkey] = table;
	int temppartition;
	//RelationMap[newkey].numtuples = (unsigned long int)numTuples;
	for(it3 = joinset.begin(); it3 != joinset.end(); it3++){
		int temp;
		RelationMap[*it3].partition = 1;
		std::tr1::unordered_map<string, int>::iterator it4;
		for(it4 = RelationMap[*it3].AttrMap.begin(); it4 != RelationMap[*it3].AttrMap.end(); it4++){
			table.AttrMap[it4->first] = it4->second;

		}
		TableDetails table1;
		RelationMap[newkey] = table1;
		RelationMap[newkey].numtuples = (unsigned long int)numTuples;
		RelationMap[newkey].partition = 1;
		for(it4 = table.AttrMap.begin(); it4 != table.AttrMap.end(); it4++){
			RelationMap[newkey].AttrMap[it4->first] = it4->second;

		}

	}
	std::tr1::unordered_map<string, int>::iterator it4;
	for(it4 = RelationMap[newkey].AttrMap.begin(); it4 != RelationMap[newkey].AttrMap.end(); it4++){

	}
}

bool Statistics::has_any_digits(string s)
{
	string::iterator *it;
	int k = 0;
	while(k < s.size()){
		if(s[k] >= '0' && s[k] <= '9'){
			return true;
		}
		k++;
	}
	return false;
}

bool Statistics::chkParsetree1(struct AndList *parseTree, char *relNames[], int numToJoin,vector<string> &AttsToEstimate)
{
	//cout << " in chk tree 1 bef " << AttrRelMap.size() <<endl;
	for(int i=0;i<numToJoin;i++)
	{
		string rname = string(relNames[i]);
		if(RelationMap.find(rname) == RelationMap.end())
			return false;

	}
	/*	std::tr1::unordered_map<string, string>::iterator it;
	for(it = AttrRelMap.begin(); it != AttrRelMap.end(); it++)
	{
		cout << "ATTRIBUTEMAP "  << AttrRelMap[it->first] << endl;
	}*/
	string tblname;
	string colname;

	AndList* andtree = parseTree;
	while(andtree!=NULL)
	{
		OrList *ortree = andtree->left;
		while(ortree!=NULL)
		{
			ComparisonOp* compOp = ortree->left;
			if(compOp == NULL)
				break;
			int lcode = compOp->left->code;
			string lval = compOp->left->value;
			ostringstream convert;
			convert << lcode;
			string val = convert.str();

			AttsToEstimate.push_back(val);
			AttsToEstimate.push_back(lval);
			ostringstream convert1;
			convert1 << compOp->code;
			val="";
			int opcode = compOp->code;
			val = convert1.str();
			AttsToEstimate.push_back(val);
			//		cout << " left list  " << lcode << " " << lval << " " <<  compOp->code<< endl;
			if(lcode == NAME)
			{    //check if attribute exists
				int pos = lval.find(".");
				if(pos!=string::npos)
				{
					tblname = lval.substr(0, pos);
					colname  = lval.substr(pos+1);

				}
				else
				{
					colname = lval;

				}
				//	cout << " before parsetree1 error chk " << AttrRelMap.size() << " " << colname<<" "<<AttrRelMap[colname] << " " << AttrRelMap.count(colname)<<endl;
				if(opcode == EQUALS)
				{

//					cout << AttrRelMap[colname] <<endl;
					string i=AttrRelMap[colname];
					if(AttrRelMap.count(colname)==0){

						//cout<<"AttrRelMap could not be found "<<colname<<endl;
						return false;
					}
				}
			}
			int rcode = compOp->right->code;
			string rval = compOp->right->value;
			val="";
			ostringstream convert2;
			convert2 << rcode;
			val = convert2.str();

			AttsToEstimate.push_back(val);
			AttsToEstimate.push_back(rval);
			//	cout << " right list  " << rcode << " " << rval << endl;
			if(rcode == NAME)
			{    //check if attribute exists
				int pos = rval.find(".");
				if(pos!=string::npos)
				{
					tblname = rval.substr(0, pos);
					colname  = rval.substr(pos+1);

				}
				else
				{
					colname = rval;

				}
				//cout << " before parsetree1 error chk " << AttrRelMap.size() << " " << colname<<" "<<AttrRelMap[colname]<<endl;
				if(opcode == EQUALS)
				{
					string i=AttrRelMap[colname];
	//				cout << " CHKKKK 2 " << AttrRelMap[colname] << " " << colname << endl;
					if(AttrRelMap.count(colname)==0)
					{
						//cout<<"AttrRelMap could not be found "<<colname<<endl;
						return false;
					}
				}
			}
			if(ortree->rightOr != NULL)
				AttsToEstimate.push_back("OR");
			ortree = ortree->rightOr;

		}
		if(andtree->rightAnd != NULL)
			AttsToEstimate.push_back("AND");
		else
			AttsToEstimate.push_back("END");

		andtree = andtree->rightAnd;
	}
//	cout << " in chk tree 1 aft " << AttrRelMap.size() <<endl;
	return true;
}

bool Statistics::chkParsetree2(struct AndList *parseTree, char *relNames[], int numToJoin)
{

//	cout << " in chk tree 2 bef " << AttrRelMap.size() <<endl;
	string temp;
	joinset.clear();
	for(int i=0; i<numToJoin; i++){
		ostringstream convert;   // stream used for the conversion
		string tblname = string(relNames[i]);
		int singleton = RelationMap[tblname].partition;
		if(singleton != 0){
			bool found = false;
			int id = RelJoin[tblname];
			convert << id;      // insert the textual representation of 'Number' in the characters in the stream
			temp = convert.str();
			joinset.insert(temp);
			vector<string>::iterator it;
			//	cout << " 1 chktre2 " << temp <<endl;
			for(it = JoinMap[id].begin(); it != JoinMap[id].end(); it++){
				string st1 = *it;
				for(int k = 0; k<numToJoin; k++){
					string st2 = string(relNames[k]);
					//			cout << " 2 chktre2 " << st1 << " " << st2 <<endl;
					if(st1.compare(st2) == 0){
						found = true;
						break;
					}
				}
				if(found == false){
					//cout<<"Returning false from partition check"<<endl;
					return false;
				}
			}
		}
		else
		{
			//		cout << " insert joinset 2 " << tblname << " " << relNames[i] << endl;
			joinset.insert(tblname);
		}
	}
//	cout << " in chk tree 2 aft " << AttrRelMap.size() <<endl;
	return true;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

//	cout << " in est bef " << AttrRelMap.size() <<endl;
	double estimate =0;
	bool no_error =true;
	joinset.clear();
	set <string> jointableset;
	map<string, EstimateInfo> EstimateChk;
	vector<double> estimates;
	vector<string> AttsToEstimate;        //has the attributes from the parsetree used to estimate
	string prev_andor = "";

	no_error = chkParsetree1(parseTree, relNames, numToJoin,AttsToEstimate);    //check all parse tree attr in rel names attr

	if(!no_error)
	{
		//cout<<" Error in parsetree check " <<endl;
		exit(0);
	}

	no_error =chkParsetree2(parseTree, relNames, numToJoin);    //check partitions are valid
	if(!no_error)
	{
		//cout<<" Error in partition check " <<endl;
		exit(0);
	}
	int i=0;


	i=0;
	while(i<AttsToEstimate.size())
	{
		estimate =0;

		int att1code = atoi(AttsToEstimate.at(i++).c_str());
		string att1val = AttsToEstimate.at(i++);

		int opcode = atoi(AttsToEstimate.at(i++).c_str());

		int att2code = atoi(AttsToEstimate.at(i++).c_str());
		string att2val = AttsToEstimate.at(i++);

		string andor = AttsToEstimate.at(i++);

		string relation1, relation2;
		//to get the 2 tables that are being joined based on attributes
		//cout << " estimate xx " << att1code << " " << att1val << " " << opcode << " " << att2code << " " << att2val << " " << andor << endl;
		if(att1code==NAME)
		{
			//to chk sytax of entered table
			int pos = att1val.find(".");
			if(pos!=string::npos)
			{
				relation1 = att1val.substr(0,pos);
				att1val = att1val.substr(pos+1);
				bool found = false;
				for(int i=0; i< numToJoin;i++)
				{
					if(relNames[i] == relation1)
					{
						found = true;
						break;
					}

				}
				if(!found)
					relation1 = AttrRelMap[att1val];


			}
			else
			{
				relation1 = AttrRelMap[att1val];
			}
			if(relation1.size()!=0)
				jointableset.insert(relation1);
			//	cout << " in att1code " <<  relation1 << " " << att1code << " "<< att1val << " "<< pos<<endl ;
		}
		if(att2code==NAME)
		{
			int pos = att2val.find(".");
			if(pos!=string::npos)
			{
				relation2 = att2val.substr(0,pos);
				att2val = att2val.substr(pos+1);
				att1val = att1val.substr(pos+1);
				bool found = false;
				for(int i=0; i< numToJoin;i++)
				{
					if(relNames[i] == relation2)
					{
						found = true;
						break;
					}

				}
				if(!found)
					relation2 = AttrRelMap[att2val];


			}
			else
			{
				relation2 = AttrRelMap[att2val];
			}
			if(relation2.size()!=0)
				jointableset.insert(relation2);
			//		cout << " in att1code " <<  relation2 << " " << att2code << " "<< att2val<< " "<<pos<<endl ;
		}
		//If both are NAMES then its a join eg a=b join on a=b
		if(att1code==NAME && att2code == NAME)
		{
			//the estimate will be 1/max(a,b)
			//		cout << " values to get max " << RelationMap[relation1].AttrMap[att1val] << " " << RelationMap[relation2].AttrMap[att2val]<<endl;
			double  maxval = max(RelationMap[relation1].AttrMap[att1val],RelationMap[relation2].AttrMap[att2val]);

			estimate = 1.0/maxval;
			estimates.push_back(estimate);
		}
		else if(att1code==NAME || att2code == NAME)
		{
			string attribute;
			TableDetails tableinfo;
			if(att1code == NAME)
			{
				tableinfo = RelationMap[relation1];
				attribute = att1val;
			}
			else
			{
				tableinfo = RelationMap[relation2];
				attribute = att2val;
			}
			if(opcode == EQUALS)
			{
				if(andor == "OR" || prev_andor=="OR")		//if encountered before use diff formula 1/n or (1- 1/n) and put count to show repitition
				{
					//check if the attribute with = has already been encountered once
					if(EstimateChk.find(attribute + "EQ") == EstimateChk.end())
					{
						EstimateInfo *est = new EstimateInfo();
						estimate = (1.0- 1.0/tableinfo.AttrMap[attribute]);
						est->AttrCount = 1;
						est->estimatetuples = estimate;
						EstimateChk[attribute + "EQ"] = *est;
					}
					else
					{
						estimate = 1.0/tableinfo.AttrMap[attribute];
						EstimateInfo *est = &(EstimateChk[attribute + "EQ"]);
						est->AttrCount += 1;
						est->estimatetuples = est->AttrCount * estimate;
					}

					if(andor != "OR")
					{
						//compute all estimates and load onto vector using the formula [1.0 - (1 - 1/f1)(1-1/f2)...]
						long double tempResult = 1.0;
						map<string, EstimateInfo>::iterator it;
						for(it = EstimateChk.begin(); it !=EstimateChk.end(); it++)
						{
							if(it->second.AttrCount == 1)
								tempResult *= it->second.estimatetuples;
							else
								tempResult *= (1 - it->second.estimatetuples);
						}

						long double totalCurrentEstimate = 1.0 - tempResult;
						estimates.push_back(totalCurrentEstimate);
						EstimateChk.clear();
					}
				}

				else
				{
					estimate = 1.0/tableinfo.AttrMap[attribute];
					estimates.push_back(estimate);
				}
			}
			//for < or >
			else
			{
				if((andor == "OR") || (prev_andor == "OR"))
				{
					estimate = (1.0 - 1.0/3);

					EstimateInfo *est = new EstimateInfo();
					est->AttrCount = 1;
					est->estimatetuples = estimate;
					EstimateChk[attribute] = *est;
					 //i.e. it's either AND or "END" so current orList is done
					if(andor != "OR")
					{
						//compute estimates = [1.0 - (1 - 1/f1)(1-1/f2)...]
						long double tempResult = 1.0;
						long double totalCurrentEstimate;
						map<string, EstimateInfo>::iterator it ;
						for(it = EstimateChk.begin(); it != EstimateChk.end(); it++)
						{
							if(it->second.AttrCount == 1)
								tempResult *= it->second.estimatetuples;	//propagate thru all values
							else
								tempResult *= (1 - it->second.estimatetuples);
						}

						totalCurrentEstimate = 1.0 - tempResult;
						estimates.push_back(totalCurrentEstimate);
						EstimateChk.clear();
					}
				}
				else
				{
					estimate = (1.0/3);
					estimates.push_back(estimate);
				}

			}
		}
		prev_andor = andor;
	}

	double result = getResult( jointableset,estimates);
	//cout << " in est aft " << AttrRelMap.size() <<endl;
	return result;
}

double Statistics:: getResult(set <string> &jointableset,vector<double> &estimates)
{
	unsigned long long int estimateN = 1;    //get numerator

		set <string>::iterator it ;
		for (it = jointableset.begin(); it != jointableset.end(); it++)
		{
			string id = *it;
			if(RelationMap[id].partition==1)
			{
				int newid = RelJoin[id];
				stringstream convert;   // stream used for the conversion
				convert << newid;      // insert the textual representation of 'Number' in the characters in the stream
				id = convert.str();
			}

			estimateN *= RelationMap[id].numtuples;
		}
	//	cout << " numerator  " << estimateN<< " "<< estimates.size()<< endl;

		double result = estimateN;
		for(int i = 0; i < estimates.size(); i++)    //multiply by calculated denominator
		{
	//		cout << "  denom  " << estimates[i]<<endl;
			result *= estimates[i];
		}
	//	cout << "RESULT " << result <<endl;
//		cout << " in get result aft " << AttrRelMap.size() <<endl;
		return result;
}
