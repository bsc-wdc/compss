/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_CORBA_GRAM_H_INCLUDED
# define YY_YY_CORBA_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    TOK_INTERFACE = 258,           /* TOK_INTERFACE  */
    TOK_LEFT_CUR_BRAKET = 259,     /* TOK_LEFT_CUR_BRAKET  */
    TOK_RIGHT_CUR_BRAKET = 260,    /* TOK_RIGHT_CUR_BRAKET  */
    TOK_LEFT_PARENTHESIS = 261,    /* TOK_LEFT_PARENTHESIS  */
    TOK_LEFT_BRAKET = 262,         /* TOK_LEFT_BRAKET  */
    TOK_RIGHT_BRAKET = 263,        /* TOK_RIGHT_BRAKET  */
    TOK_RIGHT_PARENTHESIS = 264,   /* TOK_RIGHT_PARENTHESIS  */
    TOK_COMMA = 265,               /* TOK_COMMA  */
    TOK_SEMICOLON = 266,           /* TOK_SEMICOLON  */
    TOK_IN = 267,                  /* TOK_IN  */
    TOK_OUT = 268,                 /* TOK_OUT  */
    TOK_INOUT = 269,               /* TOK_INOUT  */
    TOK_FILE = 270,                /* TOK_FILE  */
    TOK_AT = 271,                  /* TOK_AT  */
    TOK_STATIC = 272,              /* TOK_STATIC  */
    TOK_UNSIGNED = 273,            /* TOK_UNSIGNED  */
    TOK_VOID = 274,                /* TOK_VOID  */
    TOK_SHORT = 275,               /* TOK_SHORT  */
    TOK_LONG = 276,                /* TOK_LONG  */
    TOK_LONGLONG = 277,            /* TOK_LONGLONG  */
    TOK_INT = 278,                 /* TOK_INT  */
    TOK_FLOAT = 279,               /* TOK_FLOAT  */
    TOK_DOUBLE = 280,              /* TOK_DOUBLE  */
    TOK_CHAR = 281,                /* TOK_CHAR  */
    TOK_WCHAR = 282,               /* TOK_WCHAR  */
    TOK_BOOLEAN = 283,             /* TOK_BOOLEAN  */
    TOK_STRING = 284,              /* TOK_STRING  */
    TOK_WSTRING = 285,             /* TOK_WSTRING  */
    TOK_ANY = 286,                 /* TOK_ANY  */
    TOK_ERROR = 287,               /* TOK_ERROR  */
    TOK_EQUAL = 288,               /* TOK_EQUAL  */
    TOK_DBLQUOTE = 289,            /* TOK_DBLQUOTE  */
    TOK_ENUM = 290,                /* TOK_ENUM  */
    TOK_INCLUDE = 291,             /* TOK_INCLUDE  */
    TOK_CONSTRAINTS = 292,         /* TOK_CONSTRAINTS  */
    TOK_IMPLEMENTS = 293,          /* TOK_IMPLEMENTS  */
    TOK_PROCESSORS = 294,          /* TOK_PROCESSORS  */
    TOK_PROCESSOR = 295,           /* TOK_PROCESSOR  */
    TOK_IDENTIFIER = 296,          /* TOK_IDENTIFIER  */
    TOK_HEADER = 297,              /* TOK_HEADER  */
    NUMBER = 298                   /* NUMBER  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 17 "corba-gram.y"

    char		*elements;
	char		*name;
	char		*classname;
	enum datatype	dtype;
	enum direction	dir;

#line 115 "corba-gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


int yyparse (void);


#endif /* !YY_YY_CORBA_GRAM_H_INCLUDED  */
