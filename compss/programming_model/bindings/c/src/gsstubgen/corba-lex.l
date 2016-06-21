%{
#include "param_metadata.h"
#include "corba-gram.h"

int line = 1;
%}

%option noyywrap

%x comment

identifier [a-zA-Z][0-9a-zA-Z_:]*
%%

[ \t]+		/* Don't output spaces */

\n		line++;

interface	return TOK_INTERFACE;

\{		return TOK_LEFT_CUR_BRAKET;
\}		return TOK_RIGHT_CUR_BRAKET;
\(		return TOK_LEFT_PARENTHESIS;
\)		return TOK_RIGHT_PARENTHESIS;
\,		return TOK_COMMA;
\;		return TOK_SEMICOLON;
\=		return TOK_EQUAL;
\"[^"]*\"	yylval.name = yytext; return TOK_DBLQUOTE;

in			return TOK_IN;
out			return TOK_OUT;
inout		return TOK_INOUT;

File		return TOK_FILE;

unsigned	return TOK_UNSIGNED;

static		return TOK_STATIC;

void		return TOK_VOID;
short		return TOK_SHORT;
longlong	return TOK_LONGLONG;
long		return TOK_LONG;
int			return TOK_INT;
float		return TOK_FLOAT;
double		return TOK_DOUBLE;
char		return TOK_CHAR;
wchar		return TOK_WCHAR;
boolean		return TOK_BOOLEAN;
any			return TOK_ANY;
string		return TOK_STRING;
wstring		return TOK_WSTRING;

\#[^\n]*\n	line++; /* Ignore preprocessor directives */
\@[^\n]*\n	line++; /* Ignore constraints directives */
\/\/[^\n]*\n	line++; /* C++ style comments */

"/*"		{	/* C style comments */
			BEGIN(comment);
		}
<comment>[^*\n]*
<comment>"*"[^*/\n]*
<comment>\n	line++;
<comment>"*"+"/"	BEGIN(INITIAL);

{identifier}	yylval.name = strdup(yytext); return TOK_IDENTIFIER;

.			return TOK_ERROR;
%%