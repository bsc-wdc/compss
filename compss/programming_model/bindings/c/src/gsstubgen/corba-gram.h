/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
extern YYSTYPE yylval;
