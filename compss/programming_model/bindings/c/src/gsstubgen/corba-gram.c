
#ifndef lint
static const char yysccsid[] = "@(#)yaccpar	1.9 (Berkeley) 02/21/93";
#endif

#define YYBYACC 1
#define YYMAJOR 1
#define YYMINOR 9
#define YYPATCH 20140101

#define YYEMPTY        (-1)
#define yyclearin      (yychar = YYEMPTY)
#define yyerrok        (yyerrflag = 0)
#define YYRECOVERING() (yyerrflag != 0)

#define YYPREFIX "yy"

#define YYPURE 0

#line 2 "corba-gram.y"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "param_metadata.h"
#include "semantic.h"

#if 0
#define YYERROR_VERBOSE
#endif

int yylex(void);
void yyerror(char *s);
#line 16 "corba-gram.y"
#ifdef YYSTYPE
#undef  YYSTYPE_IS_DECLARED
#define YYSTYPE_IS_DECLARED 1
#endif
#ifndef YYSTYPE_IS_DECLARED
#define YYSTYPE_IS_DECLARED 1
typedef union {
	char		*name;
	char		*classname;
	enum datatype	dtype;
	enum direction	dir;
} YYSTYPE;
#endif /* !YYSTYPE_IS_DECLARED */
#line 46 "corba-gram.c"

/* compatibility with bison */
#ifdef YYPARSE_PARAM
/* compatibility with FreeBSD */
# ifdef YYPARSE_PARAM_TYPE
#  define YYPARSE_DECL() yyparse(YYPARSE_PARAM_TYPE YYPARSE_PARAM)
# else
#  define YYPARSE_DECL() yyparse(void *YYPARSE_PARAM)
# endif
#else
# define YYPARSE_DECL() yyparse(void)
#endif

/* Parameters sent to lex. */
#ifdef YYLEX_PARAM
# define YYLEX_DECL() yylex(void *YYLEX_PARAM)
# define YYLEX yylex(YYLEX_PARAM)
#else
# define YYLEX_DECL() yylex(void)
# define YYLEX yylex()
#endif

/* Parameters sent to yyerror. */
#ifndef YYERROR_DECL
#define YYERROR_DECL() yyerror(const char *s)
#endif
#ifndef YYERROR_CALL
#define YYERROR_CALL(msg) yyerror(msg)
#endif

extern int YYPARSE_DECL();

#define TOK_INTERFACE 257
#define TOK_LEFT_CUR_BRAKET 258
#define TOK_RIGHT_CUR_BRAKET 259
#define TOK_LEFT_PARENTHESIS 260
#define TOK_RIGHT_PARENTHESIS 261
#define TOK_COMMA 262
#define TOK_SEMICOLON 263
#define TOK_IN 264
#define TOK_OUT 265
#define TOK_INOUT 266
#define TOK_FILE 267
#define TOK_STATIC 268
#define TOK_UNSIGNED 269
#define TOK_VOID 270
#define TOK_SHORT 271
#define TOK_LONG 272
#define TOK_LONGLONG 273
#define TOK_INT 274
#define TOK_FLOAT 275
#define TOK_DOUBLE 276
#define TOK_CHAR 277
#define TOK_WCHAR 278
#define TOK_BOOLEAN 279
#define TOK_STRING 280
#define TOK_WSTRING 281
#define TOK_ANY 282
#define TOK_ERROR 283
#define TOK_EQUAL 284
#define TOK_DBLQUOTE 285
#define TOK_IDENTIFIER 286
#define YYERRCODE 256
static const short yylhs[] = {                           -1,
    0,    0,    5,    4,    7,    7,    8,    8,    8,    9,
    9,    9,    6,    6,   11,   13,   14,   15,   10,   16,
   17,   18,   19,   10,   20,   21,   22,   23,   10,   10,
   12,   12,   24,   24,   24,   24,   25,   25,    3,    3,
    3,    1,    1,    1,    1,    1,    1,    1,    1,    1,
    1,    2,    2,    2,    2,    2,    2,
};
static const short yylen[] = {                            2,
    0,    2,    0,    7,    0,    2,    1,    3,    1,    0,
    3,    1,    0,    3,    0,    0,    0,    0,   10,    0,
    0,    0,    0,   10,    0,    0,    0,    0,   11,    2,
    0,    1,    1,    3,    3,    1,    3,    3,    1,    1,
    1,    2,    1,    1,    1,    1,    1,    1,    1,    1,
    1,    1,    1,    1,    1,    1,    1,
};
static const short yydefred[] = {                         1,
    0,    0,    2,    3,    0,   13,    0,    0,    0,   51,
    0,    0,   50,   52,   53,   54,   55,   56,   57,   44,
   45,   46,   47,   48,   49,    0,    0,   43,    0,   30,
    4,    0,   42,   20,   15,    9,    0,   14,    0,    7,
   25,    0,    0,    0,    0,    6,    0,   21,   16,   11,
   12,    8,   26,    0,    0,    0,   36,   39,   40,   41,
    0,   22,    0,   33,   17,   27,    0,    0,    0,    0,
    0,    0,   38,   37,   23,   35,   34,   18,   28,    0,
    0,    0,   24,   19,   29,
};
static const short yydgoto[] = {                          1,
   27,   28,   61,    3,    5,    7,   38,   39,   40,   29,
   43,   62,   55,   71,   81,   42,   54,   69,   80,   47,
   56,   72,   82,   63,   64,
};
static const short yysindex[] = {                         0,
 -247, -257,    0,    0, -218,    0, -256, -205, -189,    0,
 -211, -267,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0, -208, -207,    0, -255,    0,
    0, -206,    0,    0,    0,    0, -200,    0, -235,    0,
    0, -172, -171, -195, -254,    0, -169,    0,    0,    0,
    0,    0,    0, -183, -183, -183,    0,    0,    0,    0,
 -210,    0, -170,    0,    0,    0, -193, -192, -166, -179,
 -165, -164,    0,    0,    0,    0,    0,    0,    0, -163,
 -162, -161,    0,    0,    0,
};
static const short yyrindex[] = {                         0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0,    0, -226,    0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0, -228,    0,    0,    0,    0,    0,
    0,    0,    0, -158, -158, -158,    0,    0,    0,    0,
    0,    0, -157,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,    0,
};
static const short yygindex[] = {                         0,
   37,   87,    0,    0,    0,    0,    0,    0,   60,    0,
    0,  -17,    0,    0,    0,    0,    0,    0,    0,    0,
    0,    0,    0,    0,   36,
};
#define YYTABLESIZE 106
static const short yytable[] = {                          8,
   36,   51,    9,   14,   15,   16,   17,   18,   19,    2,
   10,   11,   12,   13,   14,   15,   16,   17,   18,   19,
   20,   21,   22,   23,   24,   25,   45,   46,    4,   26,
   37,   37,    5,   10,   10,   10,   10,   65,   66,    6,
    5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
    5,    5,    5,    5,    5,    5,   10,   30,   12,   13,
   14,   15,   16,   17,   18,   19,   20,   21,   22,   23,
   24,   25,   57,   31,   32,   67,   76,   34,   35,   41,
   58,   59,   60,   44,   58,   59,   60,   48,   49,   50,
   53,   70,   73,   74,   75,   78,   79,   68,   33,   83,
   84,   85,   31,   32,   52,   77,
};
static const short yycheck[] = {                        256,
  256,  256,  259,  271,  272,  273,  274,  275,  276,  257,
  267,  268,  269,  270,  271,  272,  273,  274,  275,  276,
  277,  278,  279,  280,  281,  282,  262,  263,  286,  286,
  286,  286,  259,  262,  263,  262,  263,   55,   56,  258,
  267,  268,  269,  270,  271,  272,  273,  274,  275,  276,
  277,  278,  279,  280,  281,  282,  267,  263,  269,  270,
  271,  272,  273,  274,  275,  276,  277,  278,  279,  280,
  281,  282,  256,  263,  286,  286,  256,  286,  286,  286,
  264,  265,  266,  284,  264,  265,  266,  260,  260,  285,
  260,  262,  286,  286,  261,  261,  261,   61,   12,  263,
  263,  263,  261,  261,   45,   70,
};
#define YYFINAL 1
#ifndef YYDEBUG
#define YYDEBUG 0
#endif
#define YYMAXTOKEN 286
#define YYTRANSLATE(a) ((a) > YYMAXTOKEN ? (YYMAXTOKEN + 1) : (a))
#if YYDEBUG
static const char *yyname[] = {

"end-of-file",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"TOK_INTERFACE",
"TOK_LEFT_CUR_BRAKET","TOK_RIGHT_CUR_BRAKET","TOK_LEFT_PARENTHESIS",
"TOK_RIGHT_PARENTHESIS","TOK_COMMA","TOK_SEMICOLON","TOK_IN","TOK_OUT",
"TOK_INOUT","TOK_FILE","TOK_STATIC","TOK_UNSIGNED","TOK_VOID","TOK_SHORT",
"TOK_LONG","TOK_LONGLONG","TOK_INT","TOK_FLOAT","TOK_DOUBLE","TOK_CHAR",
"TOK_WCHAR","TOK_BOOLEAN","TOK_STRING","TOK_WSTRING","TOK_ANY","TOK_ERROR",
"TOK_EQUAL","TOK_DBLQUOTE","TOK_IDENTIFIER","illegal-symbol",
};
static const char *yyrule[] = {
"$accept : start",
"start :",
"start : start interface",
"$$1 :",
"interface : TOK_INTERFACE TOK_IDENTIFIER $$1 TOK_LEFT_CUR_BRAKET prototypes TOK_RIGHT_CUR_BRAKET TOK_SEMICOLON",
"constraints0 :",
"constraints0 : constraints1 TOK_SEMICOLON",
"constraints1 : constraint",
"constraints1 : constraints1 TOK_COMMA constraint",
"constraints1 : error",
"constraint :",
"constraint : TOK_IDENTIFIER TOK_EQUAL TOK_DBLQUOTE",
"constraint : error",
"prototypes :",
"prototypes : prototypes prototype constraints0",
"$$2 :",
"$$3 :",
"$$4 :",
"$$5 :",
"prototype : data_type TOK_IDENTIFIER $$2 TOK_LEFT_PARENTHESIS $$3 arguments0 $$4 TOK_RIGHT_PARENTHESIS $$5 TOK_SEMICOLON",
"$$6 :",
"$$7 :",
"$$8 :",
"$$9 :",
"prototype : TOK_IDENTIFIER TOK_IDENTIFIER $$6 TOK_LEFT_PARENTHESIS $$7 arguments0 $$8 TOK_RIGHT_PARENTHESIS $$9 TOK_SEMICOLON",
"$$10 :",
"$$11 :",
"$$12 :",
"$$13 :",
"prototype : TOK_STATIC TOK_IDENTIFIER TOK_IDENTIFIER $$10 TOK_LEFT_PARENTHESIS $$11 arguments0 $$12 TOK_RIGHT_PARENTHESIS $$13 TOK_SEMICOLON",
"prototype : error TOK_SEMICOLON",
"arguments0 :",
"arguments0 : arguments1",
"arguments1 : argument",
"arguments1 : arguments1 TOK_COMMA argument",
"arguments1 : arguments1 TOK_COMMA error",
"arguments1 : error",
"argument : direction data_type TOK_IDENTIFIER",
"argument : direction TOK_IDENTIFIER TOK_IDENTIFIER",
"direction : TOK_IN",
"direction : TOK_OUT",
"direction : TOK_INOUT",
"data_type : TOK_UNSIGNED numeric_type",
"data_type : numeric_type",
"data_type : TOK_CHAR",
"data_type : TOK_WCHAR",
"data_type : TOK_BOOLEAN",
"data_type : TOK_STRING",
"data_type : TOK_WSTRING",
"data_type : TOK_ANY",
"data_type : TOK_VOID",
"data_type : TOK_FILE",
"numeric_type : TOK_SHORT",
"numeric_type : TOK_LONG",
"numeric_type : TOK_LONGLONG",
"numeric_type : TOK_INT",
"numeric_type : TOK_FLOAT",
"numeric_type : TOK_DOUBLE",

};
#endif

int      yydebug;
int      yynerrs;

int      yyerrflag;
int      yychar;
YYSTYPE  yyval;
YYSTYPE  yylval;

/* define the initial stack-sizes */
#ifdef YYSTACKSIZE
#undef YYMAXDEPTH
#define YYMAXDEPTH  YYSTACKSIZE
#else
#ifdef YYMAXDEPTH
#define YYSTACKSIZE YYMAXDEPTH
#else
#define YYSTACKSIZE 10000
#define YYMAXDEPTH  10000
#endif
#endif

#define YYINITSTACKSIZE 200

typedef struct {
    unsigned stacksize;
    short    *s_base;
    short    *s_mark;
    short    *s_last;
    YYSTYPE  *l_base;
    YYSTYPE  *l_mark;
} YYSTACKDATA;
/* variables for the parser stack */
static YYSTACKDATA yystack;
#line 116 "corba-gram.y"

extern int line;


void yyerror(char *s)
{
	char const *function_name;
	
	fprintf(stderr, "%s:%i: ", get_filename(), line);
	fprintf(stderr, "%s", s);
	function_name = get_current_function_name();
	if (function_name != NULL) {
		fprintf(stderr, " in function '%s'", function_name);
		if (began_arguments) {
			fprintf(stderr, " parameter %i", get_next_argnum());
		}
	}
	fprintf(stderr,"\n");
	set_serious_error();
}

#line 336 "corba-gram.c"

#if YYDEBUG
#include <stdio.h>		/* needed for printf */
#endif

#include <stdlib.h>	/* needed for malloc, etc */
#include <string.h>	/* needed for memset */

/* allocate initial stack or double stack size, up to YYMAXDEPTH */
static int yygrowstack(YYSTACKDATA *data)
{
    int i;
    unsigned newsize;
    short *newss;
    YYSTYPE *newvs;

    if ((newsize = data->stacksize) == 0)
        newsize = YYINITSTACKSIZE;
    else if (newsize >= YYMAXDEPTH)
        return -1;
    else if ((newsize *= 2) > YYMAXDEPTH)
        newsize = YYMAXDEPTH;

    i = (int) (data->s_mark - data->s_base);
    newss = (short *)realloc(data->s_base, newsize * sizeof(*newss));
    if (newss == 0)
        return -1;

    data->s_base = newss;
    data->s_mark = newss + i;

    newvs = (YYSTYPE *)realloc(data->l_base, newsize * sizeof(*newvs));
    if (newvs == 0)
        return -1;

    data->l_base = newvs;
    data->l_mark = newvs + i;

    data->stacksize = newsize;
    data->s_last = data->s_base + newsize - 1;
    return 0;
}

#if YYPURE || defined(YY_NO_LEAKS)
static void yyfreestack(YYSTACKDATA *data)
{
    free(data->s_base);
    free(data->l_base);
    memset(data, 0, sizeof(*data));
}
#else
#define yyfreestack(data) /* nothing */
#endif

#define YYABORT  goto yyabort
#define YYREJECT goto yyabort
#define YYACCEPT goto yyaccept
#define YYERROR  goto yyerrlab

int
YYPARSE_DECL()
{
    int yym, yyn, yystate;
#if YYDEBUG
    const char *yys;

    if ((yys = getenv("YYDEBUG")) != 0)
    {
        yyn = *yys;
        if (yyn >= '0' && yyn <= '9')
            yydebug = yyn - '0';
    }
#endif

    yynerrs = 0;
    yyerrflag = 0;
    yychar = YYEMPTY;
    yystate = 0;

#if YYPURE
    memset(&yystack, 0, sizeof(yystack));
#endif

    if (yystack.s_base == NULL && yygrowstack(&yystack)) goto yyoverflow;
    yystack.s_mark = yystack.s_base;
    yystack.l_mark = yystack.l_base;
    yystate = 0;
    *yystack.s_mark = 0;

yyloop:
    if ((yyn = yydefred[yystate]) != 0) goto yyreduce;
    if (yychar < 0)
    {
        if ((yychar = YYLEX) < 0) yychar = 0;
#if YYDEBUG
        if (yydebug)
        {
            yys = yyname[YYTRANSLATE(yychar)];
            printf("%sdebug: state %d, reading %d (%s)\n",
                    YYPREFIX, yystate, yychar, yys);
        }
#endif
    }
    if ((yyn = yysindex[yystate]) && (yyn += yychar) >= 0 &&
            yyn <= YYTABLESIZE && yycheck[yyn] == yychar)
    {
#if YYDEBUG
        if (yydebug)
            printf("%sdebug: state %d, shifting to state %d\n",
                    YYPREFIX, yystate, yytable[yyn]);
#endif
        if (yystack.s_mark >= yystack.s_last && yygrowstack(&yystack))
        {
            goto yyoverflow;
        }
        yystate = yytable[yyn];
        *++yystack.s_mark = yytable[yyn];
        *++yystack.l_mark = yylval;
        yychar = YYEMPTY;
        if (yyerrflag > 0)  --yyerrflag;
        goto yyloop;
    }
    if ((yyn = yyrindex[yystate]) && (yyn += yychar) >= 0 &&
            yyn <= YYTABLESIZE && yycheck[yyn] == yychar)
    {
        yyn = yytable[yyn];
        goto yyreduce;
    }
    if (yyerrflag) goto yyinrecovery;

    yyerror("syntax error");

    goto yyerrlab;

yyerrlab:
    ++yynerrs;

yyinrecovery:
    if (yyerrflag < 3)
    {
        yyerrflag = 3;
        for (;;)
        {
            if ((yyn = yysindex[*yystack.s_mark]) && (yyn += YYERRCODE) >= 0 &&
                    yyn <= YYTABLESIZE && yycheck[yyn] == YYERRCODE)
            {
#if YYDEBUG
                if (yydebug)
                    printf("%sdebug: state %d, error recovery shifting\
 to state %d\n", YYPREFIX, *yystack.s_mark, yytable[yyn]);
#endif
                if (yystack.s_mark >= yystack.s_last && yygrowstack(&yystack))
                {
                    goto yyoverflow;
                }
                yystate = yytable[yyn];
                *++yystack.s_mark = yytable[yyn];
                *++yystack.l_mark = yylval;
                goto yyloop;
            }
            else
            {
#if YYDEBUG
                if (yydebug)
                    printf("%sdebug: error recovery discarding state %d\n",
                            YYPREFIX, *yystack.s_mark);
#endif
                if (yystack.s_mark <= yystack.s_base) goto yyabort;
                --yystack.s_mark;
                --yystack.l_mark;
            }
        }
    }
    else
    {
        if (yychar == 0) goto yyabort;
#if YYDEBUG
        if (yydebug)
        {
            yys = yyname[YYTRANSLATE(yychar)];
            printf("%sdebug: state %d, error recovery discards token %d (%s)\n",
                    YYPREFIX, yystate, yychar, yys);
        }
#endif
        yychar = YYEMPTY;
        goto yyloop;
    }

yyreduce:
#if YYDEBUG
    if (yydebug)
        printf("%sdebug: state %d, reducing by rule %d (%s)\n",
                YYPREFIX, yystate, yyn, yyrule[yyn]);
#endif
    yym = yylen[yyn];
    if (yym)
        yyval = yystack.l_mark[1-yym];
    else
        memset(&yyval, 0, sizeof yyval);
    switch (yyn)
    {
case 3:
#line 43 "corba-gram.y"
	{ begin_interface(yystack.l_mark[0].name); }
break;
case 4:
#line 45 "corba-gram.y"
	{ end_interface(); }
break;
case 8:
#line 53 "corba-gram.y"
	{ begin_constraints(); }
break;
case 11:
#line 58 "corba-gram.y"
	{ add_constraint(yystack.l_mark[-2].name); }
break;
case 15:
#line 67 "corba-gram.y"
	{  begin_function(yystack.l_mark[0].name); add_static(0); add_return_type(yystack.l_mark[-1].dtype, ""); }
break;
case 16:
#line 67 "corba-gram.y"
	{ begin_arguments(); }
break;
case 17:
#line 67 "corba-gram.y"
	{ end_arguments(); }
break;
case 18:
#line 67 "corba-gram.y"
	{ end_function(); }
break;
case 20:
#line 68 "corba-gram.y"
	{ begin_function(yystack.l_mark[0].name); add_static(0); add_return_type(object_dt, yystack.l_mark[-1].name); }
break;
case 21:
#line 68 "corba-gram.y"
	{ begin_arguments(); }
break;
case 22:
#line 68 "corba-gram.y"
	{ end_arguments(); }
break;
case 23:
#line 68 "corba-gram.y"
	{ end_function(); }
break;
case 25:
#line 69 "corba-gram.y"
	{ begin_function(yystack.l_mark[0].name); add_static(1); add_return_type(object_dt, yystack.l_mark[-1].name); }
break;
case 26:
#line 69 "corba-gram.y"
	{ begin_arguments(); }
break;
case 27:
#line 69 "corba-gram.y"
	{ end_arguments(); }
break;
case 28:
#line 69 "corba-gram.y"
	{ end_function(); }
break;
case 37:
#line 86 "corba-gram.y"
	{ add_argument(yystack.l_mark[-2].dir, yystack.l_mark[-1].dtype, "", yystack.l_mark[0].name); }
break;
case 38:
#line 87 "corba-gram.y"
	{ add_argument(yystack.l_mark[-2].dir, object_dt, yystack.l_mark[-1].name, yystack.l_mark[0].name); }
break;
case 39:
#line 90 "corba-gram.y"
	{ yyval.dir = in_dir; }
break;
case 40:
#line 91 "corba-gram.y"
	{ yyval.dir = out_dir; }
break;
case 41:
#line 92 "corba-gram.y"
	{ yyval.dir = inout_dir; }
break;
case 42:
#line 95 "corba-gram.y"
	{ yyval.dtype = yystack.l_mark[0].dtype; }
break;
case 43:
#line 96 "corba-gram.y"
	{ yyval.dtype = yystack.l_mark[0].dtype; }
break;
case 44:
#line 97 "corba-gram.y"
	{ yyval.dtype = char_dt; }
break;
case 45:
#line 98 "corba-gram.y"
	{ yyval.dtype = wchar_dt; }
break;
case 46:
#line 99 "corba-gram.y"
	{ yyval.dtype = boolean_dt; }
break;
case 47:
#line 100 "corba-gram.y"
	{ yyval.dtype = string_dt; }
break;
case 48:
#line 101 "corba-gram.y"
	{ yyval.dtype = wstring_dt; }
break;
case 49:
#line 102 "corba-gram.y"
	{ yyval.dtype = any_dt; }
break;
case 50:
#line 103 "corba-gram.y"
	{ yyval.dtype = void_dt; }
break;
case 51:
#line 104 "corba-gram.y"
	{ yyval.dtype = file_dt; }
break;
case 52:
#line 107 "corba-gram.y"
	{ yyval.dtype = short_dt; }
break;
case 53:
#line 108 "corba-gram.y"
	{ yyval.dtype = long_dt; }
break;
case 54:
#line 109 "corba-gram.y"
	{ yyval.dtype = longlong_dt; }
break;
case 55:
#line 110 "corba-gram.y"
	{ yyval.dtype = int_dt; }
break;
case 56:
#line 111 "corba-gram.y"
	{ yyval.dtype = float_dt; }
break;
case 57:
#line 112 "corba-gram.y"
	{ yyval.dtype = double_dt; }
break;
#line 686 "corba-gram.c"
    }
    yystack.s_mark -= yym;
    yystate = *yystack.s_mark;
    yystack.l_mark -= yym;
    yym = yylhs[yyn];
    if (yystate == 0 && yym == 0)
    {
#if YYDEBUG
        if (yydebug)
            printf("%sdebug: after reduction, shifting from state 0 to\
 state %d\n", YYPREFIX, YYFINAL);
#endif
        yystate = YYFINAL;
        *++yystack.s_mark = YYFINAL;
        *++yystack.l_mark = yyval;
        if (yychar < 0)
        {
            if ((yychar = YYLEX) < 0) yychar = 0;
#if YYDEBUG
            if (yydebug)
            {
                yys = yyname[YYTRANSLATE(yychar)];
                printf("%sdebug: state %d, reading %d (%s)\n",
                        YYPREFIX, YYFINAL, yychar, yys);
            }
#endif
        }
        if (yychar == 0) goto yyaccept;
        goto yyloop;
    }
    if ((yyn = yygindex[yym]) && (yyn += yystate) >= 0 &&
            yyn <= YYTABLESIZE && yycheck[yyn] == yystate)
        yystate = yytable[yyn];
    else
        yystate = yydgoto[yym];
#if YYDEBUG
    if (yydebug)
        printf("%sdebug: after reduction, shifting from state %d \
to state %d\n", YYPREFIX, *yystack.s_mark, yystate);
#endif
    if (yystack.s_mark >= yystack.s_last && yygrowstack(&yystack))
    {
        goto yyoverflow;
    }
    *++yystack.s_mark = (short) yystate;
    *++yystack.l_mark = yyval;
    goto yyloop;

yyoverflow:
    yyerror("yacc stack overflow");

yyabort:
    yyfreestack(&yystack);
    return (1);

yyaccept:
    yyfreestack(&yystack);
    return (0);
}
