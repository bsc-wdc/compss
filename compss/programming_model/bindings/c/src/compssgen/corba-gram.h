/* A Bison parser, made by GNU Bison 2.7.  */

/* Bison interface for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2012 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_CORBA_GRAM_H_INCLUDED
# define YY_YY_CORBA_GRAM_H_INCLUDED
/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     TOK_INTERFACE = 258,
     TOK_LEFT_CUR_BRAKET = 259,
     TOK_RIGHT_CUR_BRAKET = 260,
     TOK_LEFT_PARENTHESIS = 261,
     TOK_RIGHT_PARENTHESIS = 262,
     TOK_COMMA = 263,
     TOK_SEMICOLON = 264,
     TOK_IN = 265,
     TOK_OUT = 266,
     TOK_INOUT = 267,
     TOK_FILE = 268,
     TOK_STATIC = 269,
     TOK_UNSIGNED = 270,
     TOK_VOID = 271,
     TOK_SHORT = 272,
     TOK_LONG = 273,
     TOK_LONGLONG = 274,
     TOK_INT = 275,
     TOK_FLOAT = 276,
     TOK_DOUBLE = 277,
     TOK_CHAR = 278,
     TOK_WCHAR = 279,
     TOK_BOOLEAN = 280,
     TOK_STRING = 281,
     TOK_WSTRING = 282,
     TOK_ANY = 283,
     TOK_ERROR = 284,
     TOK_EQUAL = 285,
     TOK_DBLQUOTE = 286,
     TOK_IDENTIFIER = 287
   };
#endif
/* Tokens.  */
#define TOK_INTERFACE 258
#define TOK_LEFT_CUR_BRAKET 259
#define TOK_RIGHT_CUR_BRAKET 260
#define TOK_LEFT_PARENTHESIS 261
#define TOK_RIGHT_PARENTHESIS 262
#define TOK_COMMA 263
#define TOK_SEMICOLON 264
#define TOK_IN 265
#define TOK_OUT 266
#define TOK_INOUT 267
#define TOK_FILE 268
#define TOK_STATIC 269
#define TOK_UNSIGNED 270
#define TOK_VOID 271
#define TOK_SHORT 272
#define TOK_LONG 273
#define TOK_LONGLONG 274
#define TOK_INT 275
#define TOK_FLOAT 276
#define TOK_DOUBLE 277
#define TOK_CHAR 278
#define TOK_WCHAR 279
#define TOK_BOOLEAN 280
#define TOK_STRING 281
#define TOK_WSTRING 282
#define TOK_ANY 283
#define TOK_ERROR 284
#define TOK_EQUAL 285
#define TOK_DBLQUOTE 286
#define TOK_IDENTIFIER 287



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{
/* Line 2058 of yacc.c  */
#line 16 "corba-gram.y"

	char		*name;
	char		*classname;
	enum datatype	dtype;
	enum direction	dir;


/* Line 2058 of yacc.c  */
#line 129 "corba-gram.h"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;

#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */

#endif /* !YY_YY_CORBA_GRAM_H_INCLUDED  */
