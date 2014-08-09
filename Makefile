all: main 
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o Statistics.o SortedFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o QueryPlan.o DDL.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o Statistics.o HeapFile.o SortedFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o QueryPlan.o DDL.o main.o  -lfl -lpthread

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

main.o: main.cc
	$(CC) -g -c main.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

HeapFile.cc.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

Sort.o: SortedFile.cc
	$(CC) -g -c Sort.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
DDL.o: DDL.cc
	$(CC) -g -c DDL.cc	

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
