/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

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

#ifndef YY_YY_Y_TAB_H_INCLUDED
# define YY_YY_Y_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    TOK_INTERFACE = 258,
    TOK_LEFT_CUR_BRAKET = 259,
    TOK_RIGHT_CUR_BRAKET = 260,
    TOK_LEFT_PARENTHESIS = 261,
    TOK_LEFT_BRAKET = 262,
    TOK_RIGHT_BRAKET = 263,
    TOK_RIGHT_PARENTHESIS = 264,
    TOK_COMMA = 265,
    TOK_SEMICOLON = 266,
    TOK_IN = 267,
    TOK_OUT = 268,
    TOK_INOUT = 269,
    TOK_FILE = 270,
    TOK_STATIC = 271,
    TOK_UNSIGNED = 272,
    TOK_VOID = 273,
    TOK_SHORT = 274,
    TOK_LONG = 275,
    TOK_LONGLONG = 276,
    TOK_INT = 277,
    TOK_FLOAT = 278,
    TOK_DOUBLE = 279,
    TOK_CHAR = 280,
    TOK_WCHAR = 281,
    TOK_BOOLEAN = 282,
    TOK_STRING = 283,
    TOK_WSTRING = 284,
    TOK_ANY = 285,
    TOK_ERROR = 286,
    TOK_EQUAL = 287,
    TOK_DBLQUOTE = 288,
    TOK_ENUM = 289,
    TOK_INCLUDE = 290,
    TOK_IDENTIFIER = 291,
    TOK_HEADER = 292,
    NUMBER = 293
  };
#endif
/* Tokens.  */
#define TOK_INTERFACE 258
#define TOK_LEFT_CUR_BRAKET 259
#define TOK_RIGHT_CUR_BRAKET 260
#define TOK_LEFT_PARENTHESIS 261
#define TOK_LEFT_BRAKET 262
#define TOK_RIGHT_BRAKET 263
#define TOK_RIGHT_PARENTHESIS 264
#define TOK_COMMA 265
#define TOK_SEMICOLON 266
#define TOK_IN 267
#define TOK_OUT 268
#define TOK_INOUT 269
#define TOK_FILE 270
#define TOK_STATIC 271
#define TOK_UNSIGNED 272
#define TOK_VOID 273
#define TOK_SHORT 274
#define TOK_LONG 275
#define TOK_LONGLONG 276
#define TOK_INT 277
#define TOK_FLOAT 278
#define TOK_DOUBLE 279
#define TOK_CHAR 280
#define TOK_WCHAR 281
#define TOK_BOOLEAN 282
#define TOK_STRING 283
#define TOK_WSTRING 284
#define TOK_ANY 285
#define TOK_ERROR 286
#define TOK_EQUAL 287
#define TOK_DBLQUOTE 288
#define TOK_ENUM 289
#define TOK_INCLUDE 290
#define TOK_IDENTIFIER 291
#define TOK_HEADER 292
#define NUMBER 293

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 17 "corba-gram.y" /* yacc.c:1909  */

    char		*elements;
	char		*name;
	char		*classname;
	enum datatype	dtype;
	enum direction	dir;

#line 138 "y.tab.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;

int yyparse (void);

#endif /* !YY_YY_Y_TAB_H_INCLUDED  */
