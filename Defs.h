#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

enum OpType {selectfile = 1, selectpipe, project, join, duplicateremoval, sum, groupby, writeout};
unsigned int Random_Generate();

struct RecPointer{
	off_t PageNumber;
	off_t PageOffset;
};

#endif
