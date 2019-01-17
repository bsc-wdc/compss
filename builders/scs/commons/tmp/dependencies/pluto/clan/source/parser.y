
   /*+------- <| --------------------------------------------------------**
    **         A                     Clan                                **
    **---     /.\   -----------------------------------------------------**
    **   <|  [""M#                 parser.y                              **
    **-   A   | #   -----------------------------------------------------**
    **   /.\ [""M#         First version: 30/04/2008                     **
    **- [""M# | #  U"U#U  -----------------------------------------------**
         | #  | #  \ .:/
         | #  | #___| #
 ******  | "--'     .-"  ******************************************************
 *     |"-"-"-"-"-#-#-##   Clan : the Chunky Loop Analyzer (experimental)     *
 ****  |     # ## ######  *****************************************************
 *      \       .::::'/                                                       *
 *       \      ::::'/     Copyright (C) 2008 University Paris-Sud 11         *
 *     :8a|    # # ##                                                         *
 *     ::88a      ###      This is free software; you can redistribute it     *
 *    ::::888a  8a ##::.   and/or modify it under the terms of the GNU Lesser *
 *  ::::::::888a88a[]:::   General Public License as published by the Free    *
 *::8:::::::::SUNDOGa8a::. Software Foundation, either version 2.1 of the     *
 *::::::::8::::888:Y8888:: License, or (at your option) any later version.    *
 *::::':::88::::888::Y88a::::::::::::...                                      *
 *::'::..    .   .....   ..   ...  .                                          *
 * This software is distributed in the hope that it will be useful, but       *
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
 * for more details.                                                          *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with software; if not, write to the Free Software Foundation, Inc.,  *
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
 *                                                                            *
 * Clan, the Chunky Loop Analyzer                                             *
 * Written by Cedric Bastoul, Cedric.Bastoul@u-psud.fr                        *
 *                                                                            *
 ******************************************************************************/

/*
 * Clan's parsing has been derived from Jeff Lee and Jutta Degener's work:
 * Yacc Grammar:      http://www.quut.com/c/ANSI-C-grammar-y.html
 * Lex Specification: http://www.quut.com/c/ANSI-C-grammar-l-1998.html
 * FAQ:               http://www.quut.com/c/ANSI-C-grammar-FAQ.html
 * For those about to Clan, Jeff and Jutta, we salute you!
 */

%{
   #include <stdio.h>
   #include <stdlib.h>
   #include <string.h>
   #include <assert.h>
   
   #include <osl/macros.h>
   #include <osl/int.h>
   #include <osl/vector.h>
   #include <osl/relation.h>
   #include <osl/statement.h>
   #include <osl/strings.h>
   #include <osl/generic.h>
   #include <osl/body.h>
   #include <osl/extensions/arrays.h>
   #include <osl/extensions/extbody.h>
   #include <osl/scop.h>
   #include <clan/macros.h>
   #include <clan/vector.h>
   #include <clan/relation.h>
   #include <clan/relation_list.h>
   #include <clan/domain.h>
   #include <clan/scop.h>
   #include <clan/symbol.h>
   #include <clan/statement.h>
   #include <clan/options.h>

   int  yylex(void);
   void yyerror(char*);
   void yyrestart(FILE*);
   void clan_scanner_initialize();
   void clan_scanner_reinitialize(int, int, int);
   void clan_scanner_free();

   void clan_parser_add_ld();
   int  clan_parser_nb_ld();
   void clan_parser_log(char*);
   void clan_parser_increment_loop_depth();
   void clan_parser_state_print(FILE*);
   int  clan_parser_is_loop_sane(osl_relation_list_p,osl_relation_list_p,int*);
   void clan_parser_state_initialize(clan_options_p);
   osl_scop_p clan_parse(FILE*, clan_options_p);

   extern FILE*   yyin;                 /**< File to be read by Lex */
   extern int     scanner_parsing;      /**< Do we parse or not? */
   extern char*   scanner_latest_text;  /**< Latest text read by Lex */
   extern char*   scanner_clay;         /**< Data for the Clay software */
   extern int     scanner_line;         /**< Current scanned line */
   extern int     scanner_column;       /**< Scanned column (current) */
   extern int     scanner_column_LALR;  /**< Scanned column (before token) */
   extern int     scanner_scop_start;   /**< Scanned SCoP starting line */
   extern int     scanner_scop_end;     /**< Scanned SCoP ending line */
   extern int     scanner_pragma;       /**< Between SCoP pragmas or not? */

   // This is the "parser state", a collection of variables that vary
   // during the parsing and thanks to we can extract all SCoP informations.
   osl_scop_p     parser_scop;          /**< SCoP in construction */
   clan_symbol_p  parser_symbol;        /**< Top of the symbol table */
   int            parser_recording;     /**< Boolean: do we record or not? */
   char*          parser_record;        /**< What we record (statement body)*/
   int            parser_loop_depth;    /**< Current loop depth */
   int            parser_if_depth;      /**< Current if depth */
   int*           parser_scattering;    /**< Current statement scattering */
   clan_symbol_p* parser_iterators;     /**< Current iterator list */
   clan_domain_p  parser_stack;         /**< Iteration domain stack */
   int*           parser_nb_local_dims; /**< Nb of local dims per depth */
   int            parser_nb_parameters; /**< Nb of parameter symbols */
   int*           parser_valid_else;    /**< Boolean: OK for else per depth */
   int            parser_indent;        /**< SCoP indentation */
   int            parser_error;         /**< Boolean: parse error */

   int            parser_xfor_nb_nests; /**< Current number of xfor nests */
   int*           parser_xfor_depths;   /**< Current xfor nest depth list */
   int*           parser_xfor_labels;   /**< Current xfor label list */
   int            parser_xfor_index;    /**< Nb of current (x)for loop index */
   int*           parser_ceild;         /**< Booleans: ith index used ceild */
   int*           parser_floord;        /**< Booleans: ith index used floord */
   int*           parser_min;           /**< Booleans: ith index used min */
   int*           parser_max;           /**< Booleans: ith index used max */

   // Autoscop-relative variables.
   int            parser_autoscop;      /**< Boolean: autoscop in progress */
   int            parser_line_start;    /**< Autoscop start line, inclusive */
   int            parser_line_end;      /**< Autoscop end line, inclusive */
   int            parser_column_start;  /**< Autoscop start column, inclus. */
   int            parser_column_end;    /**< Autoscop end column, exclusive */

   // Ugly global variable to keep/read Clan options during parsing.
   clan_options_p parser_options;

   // Variables to generate the extbody
   osl_extbody_p  parser_access_extbody; /**< The extbody struct */
   int            parser_access_start;   /**< Start coordinates */
   int            parser_access_length;  /**< Length of the access string*/
%}

/* We expect the if-then-else shift/reduce to be there, nothing else. */
%expect 8 // TODO: should be 1 !!! (cause : labeled_statement)

%union { int value;                      /**< An integer value */
         int* vecint;                    /**< A vector of integer values */
         char* symbol;                   /**< A string for identifiers */
         osl_vector_p affex;             /**< An affine expression */
         osl_relation_p setex;           /**< A set of affine expressions */
         osl_relation_list_p list;       /**< List of array accesses */
         osl_statement_p stmt;           /**< List of statements */
       }


%token CONSTANT STRING_LITERAL SIZEOF
%token PTR_OP INC_OP DEC_OP LEFT_OP RIGHT_OP LE_OP GE_OP EQ_OP NE_OP
%token AND_OP OR_OP MUL_ASSIGN DIV_ASSIGN MOD_ASSIGN ADD_ASSIGN
%token SUB_ASSIGN LEFT_ASSIGN RIGHT_ASSIGN AND_ASSIGN
%token XOR_ASSIGN OR_ASSIGN TYPE_NAME

%token TYPEDEF EXTERN STATIC AUTO REGISTER INLINE RESTRICT
%token CHAR SHORT INT LONG SIGNED UNSIGNED FLOAT DOUBLE CONST VOLATILE VOID
%token BOOL COMPLEX IMAGINARY
%token STRUCT UNION ENUM ELLIPSIS

%token CASE DEFAULT IF ELSE SWITCH WHILE DO XFOR FOR GOTO CONTINUE BREAK RETURN

%token IGNORE PRAGMA
%token MIN MAX CEILD FLOORD
%token <symbol> ID
%token <value>  INTEGER

%type <stmt>   statement_list
%type <stmt>   statement_indented
%type <stmt>   statement
%type <stmt>   labeled_statement
%type <stmt>   compound_statement
%type <stmt>   expression_statement
%type <stmt>   selection_else_statement
%type <stmt>   selection_statement
%type <stmt>   iteration_statement
%type <stmt>   loop_body
%type <setex>  loop_initialization
%type <list>   loop_initialization_list
%type <setex>  loop_condition
%type <list>   loop_condition_list
%type <value>  loop_stride
%type <vecint> loop_stride_list
%type <symbol> idparent
%type <symbol> id_or_clan_keyword

%type <setex>  affine_minmax_expression
%type <setex>  affine_min_expression
%type <setex>  affine_max_expression
%type <setex>  affine_relation
%type <setex>  affine_logical_and_expression
%type <setex>  affine_condition
%type <affex>  affine_primary_expression
%type <affex>  affine_unary_expression
%type <affex>  affine_multiplicative_expression
%type <affex>  affine_ceildfloord_expression
%type <affex>  affine_ceild_expression
%type <affex>  affine_floord_expression
%type <affex>  affine_expression

%type <list>   primary_expression
%type <list>   postfix_expression
%type <list>   argument_expression_list
%type <list>   unary_expression
%type <list>   cast_expression
%type <list>   multiplicative_expression
%type <list>   additive_expression
%type <list>   shift_expression
%type <list>   relational_expression
%type <list>   equality_expression
%type <list>   and_expression
%type <list>   exclusive_or_expression
%type <list>   inclusive_or_expression
%type <list>   logical_and_expression
%type <list>   logical_or_expression
%type <list>   conditional_expression
%type <list>   assignment_expression
%type <list>   expression
%type <value>  assignment_operator

%destructor { free($$); } <symbol>
%destructor { osl_vector_free($$); } <affex>
%destructor { osl_relation_list_free($$); } <list>
%destructor { osl_statement_free($$); } <stmt>

%start scop_list
%%

// +--------------------------------------------------------------------------+
// |                              SCoP GRAMMAR                                |
// +--------------------------------------------------------------------------+


// Rules for a scop_list
scop_list:
    scop             { CLAN_debug("rule scop_list.1: scop"); } 
  | scop_list scop   { CLAN_debug("rule scop_list.2: scop_list scop"); } 
  | scop_list IGNORE { CLAN_debug("rule scop_list.3: scop_list IGNORE"); } 
  | IGNORE           { CLAN_debug("rule scop_list.4: IGNORE"); } 
  ;


// Rules for a scop
scop:
    statement_list IGNORE
    {
      int nb_parameters;
      osl_scop_p scop;
      osl_generic_p arrays;

      CLAN_debug("rule scop.1: statement_list IGNORE");
      scop = osl_scop_malloc();
      CLAN_strdup(scop->language, "C");

      // Build the SCoP context.
      nb_parameters = clan_symbol_nb_of_type(parser_symbol,
          CLAN_TYPE_PARAMETER);
      scop->parameters = clan_symbol_to_strings(parser_symbol,
          CLAN_TYPE_PARAMETER);
      scop->context = clan_relation_build_context(nb_parameters,
                                                  parser_options);
      
      // Set the statements.
      scop->statement = $1;

      // Compact the SCoP relations.
      if (CLAN_DEBUG) {
	CLAN_debug("SCoP before compaction:");
	osl_scop_dump(stderr, scop);
      }
      clan_scop_compact(scop);
      if (CLAN_DEBUG) {
	CLAN_debug("SCoP after compaction:");
	osl_scop_dump(stderr, scop);
      }

      // Simplify the SCoP iteration domains.
      if (!parser_options->nosimplify)
        clan_scop_simplify(scop);

      // Add extensions.
      scop->registry = osl_interface_get_default_registry();
      clan_scop_generate_scatnames(scop);
      arrays = clan_symbol_to_arrays(parser_symbol);
      osl_generic_add(&scop->extension, arrays);
      clan_scop_generate_coordinates(scop, parser_options->name);
      clan_scop_generate_clay(scop, scanner_clay);

      // Add the SCoP to parser_scop and prepare the state for the next SCoP.
      osl_scop_add(&parser_scop, scop);
      clan_symbol_free(parser_symbol);
      clan_parser_state_initialize(parser_options);
      CLAN_debug_call(osl_scop_dump(stderr, scop));
    } 
  ;


// Rules for a statement list
// Return <stmt>
statement_list:
    statement_indented       { $$ = $1; }
  | statement_list
    statement_indented       { $$ = $1; osl_statement_add(&$$, $2); }
  ;


// Rules for an indented statement
// Return <stmt>
statement_indented:
    { 
      if (parser_indent == CLAN_UNDEFINED)
        parser_indent = scanner_column_LALR - 1;
    }
    statement
    {
      $$ = $2;
    }
  ; 


// Rules for a statement
// Return <stmt>
statement:
    labeled_statement        { $$ = $1; }
  | compound_statement       { $$ = $1; }
  | expression_statement     { $$ = $1; }
  | selection_statement      { $$ = $1; }
  | {
      if (parser_options->autoscop && !parser_autoscop && !parser_loop_depth) {
        parser_line_start = scanner_line;
        parser_column_start = scanner_column_LALR;
        parser_autoscop = CLAN_TRUE;
        // Reinitialize the symbol table.
        clan_symbol_free(parser_symbol);
        parser_symbol = NULL;
        if (CLAN_DEBUG)
          fprintf(stderr, "Autoscop start: line %3d column %3d\n",
                  parser_line_start, parser_column_start);
      }
    }    
    iteration_statement
    {
      $$ = $2;
      if (parser_options->autoscop && parser_autoscop && !parser_loop_depth) {
        parser_line_end = scanner_line;
        parser_column_end = scanner_column;
        if (CLAN_DEBUG)
          fprintf(stderr, "Autoscop found: line %3d column %3d\n",
                  parser_line_end, parser_column_end);
      }
    }
;


labeled_statement:
    INTEGER ':' 
    {
      int i;
      clan_domain_p labeled_domain;
      osl_relation_list_p labeled_constraints;

      CLAN_debug("labeled_statement.1.1: <int> : ...");
     
      if (parser_stack == NULL)
        printf("NULL stack, label %d\n", $1);
      if (parser_stack->constraints == NULL)
        printf("NULL constraints\n");
      if (($1 < 0) ||
	  ($1 >= clan_relation_list_nb_elements(parser_stack->constraints))) {
	yyerror("label out of range");
        YYABORT;
      }

      labeled_domain = clan_domain_malloc();
      labeled_domain->constraints = osl_relation_list_malloc();
      labeled_constraints = parser_stack->constraints;
      for (i = 0; i < $1; i++)
	labeled_constraints = labeled_constraints->next;
      labeled_domain->constraints->elt =
	  osl_relation_clone(labeled_constraints->elt);

      clan_domain_push(&parser_stack, labeled_domain);
      parser_xfor_labels[parser_xfor_nb_nests] = $1;
      parser_xfor_depths[parser_xfor_nb_nests + 1] = 0;
      parser_xfor_nb_nests++;
    }
    statement
    {
      clan_domain_drop(&parser_stack);
      parser_xfor_nb_nests--;
      parser_xfor_labels[parser_xfor_nb_nests] = CLAN_UNDEFINED;
      $$ = $4;
      CLAN_debug("labeled_statement.1.2: ... <stmt>");
    }
  ;


// Rules for a compound statement
// Return <stmt>
compound_statement:
    '{' '}'                  { $$ = NULL; }
  | '{' statement_list '}'   { $$ = $2; }
  ;


// +--------------------------------------------------------------------------+
// |                           AFFINE CONTROL PART                            |
// +--------------------------------------------------------------------------+


selection_else_statement:
    ELSE 
    {
      if (!parser_valid_else[parser_if_depth]) {
	yyerror("unsupported negation of a condition involving a modulo");
	YYABORT;
      }
    }
    statement
    {
      CLAN_debug("rule selection_else_statement.1: else <stmt>");
      $$ = $3;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  |
    {
      CLAN_debug("rule selection_else_statement.2: <void>");
      $$ = NULL;
    }
  ;


selection_statement:
    IF '(' affine_condition ')'
    {
      CLAN_debug("rule selection_statement.1.1: if ( condition ) ...");
      clan_domain_dup(&parser_stack);
      clan_domain_and(parser_stack, $3);
      parser_if_depth++;
      if ((parser_loop_depth + parser_if_depth) > CLAN_MAX_DEPTH)
	CLAN_error("CLAN_MAX_DEPTH reached, recompile with a higher value");
    }
    statement
    {
      osl_relation_p not_if;
      
      CLAN_debug("rule selection_statement.1.2: if ( condition ) <stmt> ...");
      clan_domain_drop(&parser_stack);
      clan_domain_dup(&parser_stack);
      if (!clan_relation_existential($3)) {
	not_if = clan_relation_not($3);
	clan_domain_and(parser_stack, not_if);
	osl_relation_free(not_if);
	parser_valid_else[parser_if_depth] = 1;
      } else {
	parser_valid_else[parser_if_depth] = 0;
      }
      osl_relation_free($3);
    }
    selection_else_statement
    {
      CLAN_debug("rule selection_statement.1.3: if ( condition ) <stmt>"
	         "[else <stmt>]");
      clan_domain_drop(&parser_stack);
      $$ = $6;
      osl_statement_add(&$$, $8);
      parser_if_depth--;
      parser_nb_local_dims[parser_loop_depth + parser_if_depth] = 0;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  ;


iteration_statement:
    XFOR '(' loop_initialization_list loop_condition_list loop_stride_list ')'
    {
      CLAN_debug("rule iteration_statement.1.1: xfor ( init cond stride ) ...");
      parser_xfor_labels[parser_loop_depth] = CLAN_UNDEFINED;
       
      // Check loop bounds and stride consistency and reset sanity sentinels.
      if (!clan_parser_is_loop_sane($3, $4, $5))
        YYABORT;

      // Check that either an xfor loop is the first one or have the same
      // number of indices than the previous one.
      if ((clan_relation_list_nb_elements(parser_stack->constraints) != 1) &&
	  (clan_relation_list_nb_elements(parser_stack->constraints) !=
	   clan_relation_list_nb_elements($3))) {
	yyerror("consecutive xfor loops without the same number of indices");
	osl_relation_list_free($3);
        osl_relation_list_free($4);
	free($5);
        YYABORT;
      }

      // Add the constraints contributed by the xfor loop to the domain stack.
      clan_domain_dup(&parser_stack);
      clan_domain_xfor(parser_stack, parser_loop_depth + 1, parser_symbol,
	               $3, $4, $5, parser_options);

      clan_parser_increment_loop_depth();
      parser_xfor_depths[parser_xfor_nb_nests]++;
      parser_xfor_index = 0;
      osl_relation_list_free($3);
      osl_relation_list_free($4);
      $3 = NULL; // To avoid conflicts with the destructor TODO: avoid that.
      $4 = NULL;
      parser_scattering[2*parser_loop_depth-1] = ($5[0] > 0) ? 1 : -1;
      parser_scattering[2*parser_loop_depth] = 0;
      free($5);
    }
    loop_body
    {
      CLAN_debug("rule iteration_statement.1.2: xfor ( init cond stride ) "
	         "body");
      parser_xfor_depths[parser_xfor_nb_nests]--;
      $$ = $8;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  |
    FOR '(' loop_initialization_list loop_condition_list loop_stride_list ')'
    {
      CLAN_debug("rule iteration_statement.2.1: for ( init cond stride ) ...");
      parser_xfor_labels[parser_loop_depth] = 0;
     
      // Check there is only one element in each list
      if (parser_xfor_index != 1) {
	yyerror("unsupported element list in a for loop");
	osl_relation_list_free($3);
        osl_relation_list_free($4);
	free($5);
        YYABORT;
      }

      // Check loop bounds and stride consistency and reset sanity sentinels.
      if (!clan_parser_is_loop_sane($3, $4, $5))
        YYABORT;

      // Add the constraints contributed by the for loop to the domain stack.
      clan_domain_dup(&parser_stack);
      clan_domain_for(parser_stack, parser_loop_depth + 1, parser_symbol,
	              $3->elt, $4->elt, $5[0], parser_options);

      clan_parser_increment_loop_depth();
      parser_xfor_index = 0;
      osl_relation_list_free($3);
      osl_relation_list_free($4);
      $3 = NULL; // To avoid conflicts with the destructor TODO: avoid that.
      $4 = NULL;
      parser_scattering[2*parser_loop_depth-1] = ($5[0] > 0) ? 1 : -1;
      parser_scattering[2*parser_loop_depth] = 0;
      free($5);
    }
    loop_body
    {
      CLAN_debug("rule iteration_statement.2.2: for ( init cond stride ) "
	         "body");
      $$ = $8;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  | loop_infinite
    {
      osl_vector_p   iterator_term;
      osl_relation_p iterator_relation;

      CLAN_debug("rule iteration_statement.3.1: loop_infinite ...");
      if (!clan_symbol_new_iterator(&parser_symbol, parser_iterators,
	                            "clan_infinite_loop", parser_loop_depth))
	YYABORT;

      parser_xfor_labels[parser_loop_depth] = 0;
      clan_parser_increment_loop_depth();
      
      // Generate the constraint clan_infinite_loop >= 0.
      iterator_term = clan_vector_term(parser_symbol, 0, NULL,
                                       parser_options->precision);
      osl_int_set_si(parser_options->precision,
                     &iterator_term->v[parser_loop_depth], 1); 
      osl_int_set_si(parser_options->precision, &iterator_term->v[0], 1); 
      iterator_relation = osl_relation_from_vector(iterator_term);
      
      // Add it to the domain stack.
      clan_domain_dup(&parser_stack);
      clan_domain_and(parser_stack, iterator_relation);
      osl_vector_free(iterator_term);
      osl_relation_free(iterator_relation);
      parser_scattering[2*parser_loop_depth-1] = 1;
      parser_scattering[2*parser_loop_depth] = 0;
    }
    loop_body
    {
      CLAN_debug("rule iteration_statement.3.2: loop_infinite body");
      $$ = $3;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  ;


loop_initialization_list:
    loop_initialization ',' loop_initialization_list
    {
      osl_relation_list_p new = osl_relation_list_malloc();
      CLAN_debug("rule initialization_list.1: initialization , "
	         "initialization_list");
      new->elt = $1;
      osl_relation_list_push(&$3, new);
      $$ = $3;
    }
  | loop_initialization ';'
    {
      CLAN_debug("rule initialization_list.2: initialization ;");
      parser_xfor_index = 0;
      $$ = osl_relation_list_malloc();
      $$->elt = $1;
    }
  ;


loop_initialization:
    loop_declaration ID
    {
      if (!clan_symbol_new_iterator(&parser_symbol, parser_iterators, $2,
	                            parser_loop_depth))
	YYABORT;
    }
    '=' affine_minmax_expression
    {
      CLAN_debug("rule initialization: ID = <setex>");
      parser_xfor_index++;
      free($2);
      $$ = $5;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


loop_declaration:
    INT
  |
  ;


loop_condition_list:
    loop_condition ',' loop_condition_list
    {
      osl_relation_list_p new = osl_relation_list_malloc();
      new->elt = $1;
      osl_relation_list_push(&$3, new);
      $$ = $3;
    }
  | loop_condition ';'
    {
      parser_xfor_index = 0;
      $$ = osl_relation_list_malloc();
      $$->elt = $1;
    }
  ;


loop_condition:
    affine_condition
    {
      CLAN_debug("rule condition.1: <setex>");
      parser_xfor_index++;
      $$ = $1;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


loop_stride_list:
    loop_stride ',' loop_stride_list
    {
      int i;
      $$ = malloc((parser_xfor_index) * sizeof(int));
      for (i = 0; i < parser_xfor_index - 1; i++)
        $$[i + 1] = $3[i];
      free($3);
      $$[0] = $1;
    }
  | loop_stride
    {
      $$ = malloc(sizeof(int));
      $$[0] = $1;
    }
  ;


// Rules for the for loop increment.
// Handled cases (with s an integer):
// i++, i--; ++i, --i, i = i + v, i = i - s, i += s, i -= s
// return <value>
// TODO: we should check that ID corresponds to the current loop iterator.
//
loop_stride:
    idparent INC_OP             { parser_xfor_index++; $$ =  1;  free($1); }
  | idparent DEC_OP             { parser_xfor_index++; $$ = -1;  free($1); }
  | INC_OP idparent             { parser_xfor_index++; $$ =  1;  free($2); }
  | DEC_OP idparent             { parser_xfor_index++; $$ = -1;  free($2); }
  | idparent '=' idparent '+' INTEGER
    { parser_xfor_index++; $$ =  $5; free($1); free($3); }
  | idparent '=' idparent '-' INTEGER
    { parser_xfor_index++; $$ = -$5; free($1); free($3); }
  | idparent ADD_ASSIGN INTEGER { parser_xfor_index++; $$ =  $3; free($1); }
  | idparent SUB_ASSIGN INTEGER { parser_xfor_index++; $$ = -$3; free($1); }
  ;

idparent:
    ID { $$ = $1; }
  | '(' idparent ')'
    { $$ = $2; }
 ;

loop_infinite:
    WHILE '(' INTEGER ')'
  | FOR '(' ';' ';' ')'
  ;


loop_body:
    statement
    {
      CLAN_debug("rule loop_body.1: <stmt>");
      parser_loop_depth--;
      clan_symbol_free(parser_iterators[parser_loop_depth]);
      parser_iterators[parser_loop_depth] = NULL;
      clan_domain_drop(&parser_stack);
      $$ = $1;
      parser_scattering[2*parser_loop_depth]++;
      parser_nb_local_dims[parser_loop_depth + parser_if_depth] = 0;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  ;


// +--------------------------------------------------------------------------+
// |                             AFFINE EXPRESSIONS                           |
// +--------------------------------------------------------------------------+


affine_minmax_expression:
    affine_ceildfloord_expression
    {
      CLAN_debug("rule affine_minmax_expression.1: <affex>");
      $$ = osl_relation_from_vector($1);
      osl_vector_free($1);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  | minmax '(' affine_minmax_expression ',' affine_minmax_expression ')'
    {
      CLAN_debug("rule affine_minmax_expression.2: "
                 "MAX (affine_minmaxexpression , affine_minmax_expression )");
      $$ = osl_relation_concat_constraints($3, $5);
      osl_relation_free($3);
      osl_relation_free($5);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


minmax:
    MIN { parser_min[parser_xfor_index] = 1; }
  | MAX { parser_max[parser_xfor_index] = 1; }
  ;


//
// Rules for min(... operators.
// return <setex>
//
affine_min_expression:
//
// Rule affine_min_expression.1: <affex>
//
    affine_floord_expression
    {
      CLAN_debug("rule affine_min_expression.1: <affex>");
      $$ = osl_relation_from_vector($1);
      osl_vector_free($1);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_min_expression.2 MIN ( aff_min_expr , aff_min_expr )
//
  | MIN '(' affine_min_expression ',' affine_min_expression ')'
    {
      CLAN_debug("rule affine_min_expression.2: "
                 "MIN ( affine_min_expression , affine_min_expresssion");
      $$ = osl_relation_concat_constraints($3, $5);
      osl_relation_free($3);
      osl_relation_free($5);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


//
// Rules for max(... operators.
// return <setex>
//
affine_max_expression:
//
// Rule affine_max_expression.1: <affex>
//
    affine_ceild_expression
    {
      CLAN_debug("rule affine_max_expression.1: <affex>");
      $$ = osl_relation_from_vector($1);
      osl_vector_free($1);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_max_expression.2: MAX ( aff_max_expr , aff_max_expr )
//
  | MAX '(' affine_max_expression ',' affine_max_expression ')'
    {
      CLAN_debug("rule affine_max_expression.2: "
                 "MAX ( affine_max_expression , affine_max_expression )");
      $$ = osl_relation_concat_constraints($3, $5);
      osl_relation_free($3);
      osl_relation_free($5);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


//
// Rules for defining an affine condition. A condition is an affine relation
// (possibly with min/max operator(s)) of the form 'affex1 op affex2'
// where affex2 may contain min operators iff op is '<' or '<=', and
// max operators iff op is '>' or '>='.
// return: <setex>
//
affine_relation:
//
// Rule affine_relation.1: max_affex < min_affex
//
    affine_max_expression '<' affine_min_expression
    {
      CLAN_debug("rule affine_relation.1: max_affex < min_affex");
      $$ = clan_relation_greater($3, $1, 1);
      osl_relation_free($1);
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.2: min_affex > max_affex
//
  | affine_min_expression '>' affine_max_expression
    {
      CLAN_debug("rule affine_relation.2: min_affex > max_affex");
      $$ = clan_relation_greater($1, $3, 1);
      osl_relation_free($1);
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.3: max_affex <= min_affex
//
  | affine_max_expression LE_OP affine_min_expression
    {
      CLAN_debug("rule affine_relation.3: max_affex <= min_affex");
      $$ = clan_relation_greater($3, $1, 0);
      osl_relation_free($1);
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.4: min_affex >= max_affex
//
  | affine_min_expression GE_OP affine_max_expression
    {
      CLAN_debug("rule affine_relation.4: min_affex >= max_affex");
      $$ = clan_relation_greater($1, $3, 0);
      osl_relation_free($1);
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.5: <affex> == <affex>
//
  | affine_expression EQ_OP affine_expression
    {
      // a==b translates to a-b==0.
      osl_vector_p res;

      CLAN_debug("rule affine_relation.5: <affex> == <affex>");
      // Warning: cases like ceild(M,32) == ceild(N,32) are not handled.
      // Assert if we encounter such a case.
      assert ((osl_int_zero(parser_options->precision, $1->v[0]) ||
	       osl_int_one(parser_options->precision,  $1->v[0])) &&
	      (osl_int_zero(parser_options->precision, $3->v[0]) ||
	       osl_int_one(parser_options->precision,  $3->v[0])));
      res = osl_vector_sub($1, $3);
      osl_vector_tag_equality(res);
      $$ = osl_relation_from_vector(res);
      osl_vector_free(res);
      osl_vector_free($1);
      osl_vector_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.6: ( affine_relation )
//
  | '(' affine_condition ')'
    {
      CLAN_debug("rule affine_relation.6: ( condition )");
      $$ = $2;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.7: ! ( affine_condition )
//
  | '!' '(' affine_condition ')'
    {
      CLAN_debug("rule affine_relation.7: ! ( condition )");
      if (clan_relation_existential($3)) {
        osl_relation_free($3);
	yyerror("unsupported negation of a condition involving a modulo");
	YYABORT;
      }
      $$ = clan_relation_not($3);
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
//
// Rule affine_relation.8: affine_expression % INTEGER == INTEGER
//
  | affine_expression '%' INTEGER EQ_OP INTEGER
    {
      CLAN_debug("rule affine_relation.8: "
	         "affine_expression %% INTEGER == INTEGER");
      osl_int_set_si(parser_options->precision,
                     &($1->v[CLAN_MAX_DEPTH + 1 + clan_parser_nb_ld()]), -$3);
      osl_int_add_si(parser_options->precision,
	             &($1->v[$1->size - 1]), $1->v[$1->size - 1], -$5);
      clan_parser_add_ld();
      $$ = osl_relation_from_vector($1);
      osl_vector_free($1);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


affine_logical_and_expression:
    affine_relation
    {
      CLAN_debug("rule affine_logical_and_expression.1: affine_relation");
      $$ = $1;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  | affine_logical_and_expression AND_OP affine_relation
    {
      CLAN_debug("rule affine_logical_and_expression.2: "
	         "affine_logical_and_expression && affine_relation");
      clan_relation_and($1, $3);
      $$ = $1;
      osl_relation_free($3);
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


affine_condition:
    affine_logical_and_expression
    {
      CLAN_debug("rule affine_condition.1: affine_logical_and_expression");
      $$ = $1;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  | affine_condition OR_OP affine_logical_and_expression
    {
      CLAN_debug("rule affine_condition.2: "
	         "affine_condition || affine_logical_and_expression");
      osl_relation_add(&$1, $3);
      $$ = $1;
      CLAN_debug_call(osl_relation_dump(stderr, $$));
    }
  ;


affine_primary_expression:
    ID
    {
      clan_symbol_p id;

      CLAN_debug("rule affine_primary_expression.1: id");
      id = clan_symbol_add(&parser_symbol, $1, CLAN_UNDEFINED);
      // An id in an affex can be either an iterator or a parameter. If it is
      // an unknown (embeds read-only variables), it is updated to a parameter.
      if (id->type == CLAN_UNDEFINED) {
        if ((parser_nb_parameters + 1) > CLAN_MAX_PARAMETERS)
	        CLAN_error("CLAN_MAX_PARAMETERS reached,"
                             "recompile with a higher value");
        id->type = CLAN_TYPE_PARAMETER;
        id->rank = ++parser_nb_parameters;
      }

      if ((id->type != CLAN_TYPE_ITERATOR) &&
          (id->type != CLAN_TYPE_PARAMETER)) {
        free($1);
	if (id->type == CLAN_TYPE_ARRAY)
	  yyerror("variable or array reference in an affine expression");
	else
          yyerror("function call in an affine expression");
	YYABORT;
      }
      
      $$ = clan_vector_term(parser_symbol, 1, $1, parser_options->precision);
      free($1);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | INTEGER
    {
      CLAN_debug("rule affine_primary_expression.2: INTEGER");
      $$ = clan_vector_term(parser_symbol, $1, NULL, parser_options->precision);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | '(' affine_expression ')'
    {
      CLAN_debug("rule affine_primary_expression.3: "
                 "affine_additive_expression");
      $$ = $2;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


affine_unary_expression:
    affine_primary_expression
    {
      CLAN_debug("rule affine_unary_expression.1: affine_primary_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | '+' affine_primary_expression
    {
      CLAN_debug("rule affine_unary_expression.2: +affine_primary_expression");
      $$ = $2;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | '-' affine_primary_expression
    {
      CLAN_debug("rule affine_unary_expression.2: -affine_primary_expression");
      $$ = osl_vector_mul_scalar($2, -1);
      osl_vector_free($2);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


affine_multiplicative_expression:
    affine_unary_expression
    { 
      CLAN_debug("rule affine_multiplicative_expression.1: "
                 "affine_unary_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | affine_multiplicative_expression '*' affine_unary_expression
    {
      int coef;
      
      CLAN_debug("rule affine_multiplicative_expression.2: "
                 "affine_multiplicative_expression * affine_unary_expression");
      if (!osl_vector_is_scalar($1) && !osl_vector_is_scalar($3)) {
        osl_vector_free($1);
        osl_vector_free($3);
        yyerror("non-affine expression");
	YYABORT;
      }

      if (osl_vector_is_scalar($1)) {
        coef = osl_int_get_si($1->precision, $1->v[$1->size - 1]);
        $$ = osl_vector_mul_scalar($3, coef);
      } else {
        coef = osl_int_get_si($3->precision, $3->v[$3->size - 1]);
        $$ = osl_vector_mul_scalar($1, coef);
      }
      osl_vector_free($1);
      osl_vector_free($3);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | affine_multiplicative_expression '/' affine_unary_expression
    {
      int val1, val2;
      
      CLAN_debug("rule affine_multiplicative_expression.3: "
                 "affine_multiplicative_expression / affine_unary_expression");
      if (!osl_vector_is_scalar($1) || !osl_vector_is_scalar($3)) {
        osl_vector_free($1);
        osl_vector_free($3);
        yyerror("non-affine expression");
	YYABORT;
      }
      val1 = osl_int_get_si($1->precision, $1->v[$1->size - 1]);
      val2 = osl_int_get_si($3->precision, $3->v[$3->size - 1]);
      $$ = clan_vector_term(parser_symbol, val1 / val2, NULL,
                            parser_options->precision);
      osl_vector_free($1);
      osl_vector_free($3);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


affine_expression:
    affine_multiplicative_expression
    { 
      CLAN_debug("rule affine_expression.1: "
                 "affine_multiplicative_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | affine_expression '+' affine_multiplicative_expression
    {
      CLAN_debug("rule affine_expression.2: "
          "affine_expression + affine_multiplicative_expression");
      $$ = osl_vector_add($1, $3);
      osl_vector_free($1);
      osl_vector_free($3);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | affine_expression '-' affine_multiplicative_expression
    {
      CLAN_debug("rule affine_expression.3: "
          "affine_expression - affine_multiplicative_expression");
      $$ = osl_vector_sub($1, $3);
      osl_vector_free($1);
      osl_vector_free($3);
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


affine_ceildfloord_expression:
    affine_expression
    {
      CLAN_debug("affine_ceildloord_expression.1: affine_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | ceildfloord '(' affine_expression ',' INTEGER ')'
    {
      CLAN_debug("affine_ceildfloord_expression.2: "
                 "ceildfloord ( affine_expression , INTEGER )");
      osl_int_set_si(parser_options->precision, &($3->v[0]), $5);
      $$ = $3;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


ceildfloord:
    CEILD  { parser_ceild[parser_xfor_index]  = 1; }
  | FLOORD { parser_floord[parser_xfor_index] = 1; }
  ;


affine_ceild_expression:
    affine_expression
    {
      CLAN_debug("affine_ceil_expression.1: affine_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | CEILD '(' affine_expression ',' INTEGER ')'
    {
      CLAN_debug("affine_ceil_expression.2: "
                 "CEILD ( affine_expression , INTEGER )");
      osl_int_set_si(parser_options->precision, &($3->v[0]), $5);
      $$ = $3;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;


affine_floord_expression:
    affine_expression
    {
      CLAN_debug("affine_floor_expression.1: affine_expression");
      $$ = $1;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  | FLOORD '(' affine_expression ',' INTEGER ')'
    {
      CLAN_debug("affine_floor_expression.2: "
                 "FLOORD ( affine_expression , INTEGER )");
      osl_int_set_si(parser_options->precision, &($3->v[0]), $5);
      $$ = $3;
      CLAN_debug_call(osl_vector_dump(stderr, $$));
    }
  ;

id_or_clan_keyword:
    ID { $$ = $1; }
  | MIN { $$ = strdup("min"); }
  | MAX { $$ = strdup("max"); }
  | CEILD { $$ = strdup("ceild"); }
  | FLOORD { $$ = strdup("floord"); }
  ;

// +--------------------------------------------------------------------------+
// |                          QUASI-ANSI C STATEMENTS                         |
// +--------------------------------------------------------------------------+


primary_expression:
    id_or_clan_keyword
    {
      int nb_columns;
      osl_relation_p id;
      osl_relation_list_p list;
      clan_symbol_p symbol;

      CLAN_debug("rule primary_expression.1: id_or_clan_keyword");
      symbol = clan_symbol_add(&parser_symbol, $1, CLAN_UNDEFINED);
      nb_columns = CLAN_MAX_DEPTH + CLAN_MAX_LOCAL_DIMS +
	                 CLAN_MAX_PARAMETERS + 2;
      id = osl_relation_pmalloc(parser_options->precision, 0, nb_columns);
      osl_relation_set_attributes(id, 0, parser_loop_depth, 0,
                                  CLAN_MAX_PARAMETERS);
      clan_relation_tag_array(id, symbol->key);
      list = osl_relation_list_malloc();
      list->elt = id;

      // add the id to the extbody
      if (parser_options->extbody) {
        if (parser_access_start != -1) {
          osl_extbody_add(parser_access_extbody,
                          parser_access_start,
                          parser_access_length);
        }

        int len = strlen(parser_record);
        parser_access_start = len - strlen($1);
        parser_access_length = len - parser_access_start;
      }

      free($1);
      $$ = list;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  | CONSTANT
    { $$ = NULL; }
  | INTEGER
    { $$ = NULL; }
  | STRING_LITERAL
    { $$ = NULL; }
  | '(' expression ')'
    { $$ = $2; }
  ;


postfix_expression:
    primary_expression
    { $$ = $1; }
  | postfix_expression '[' affine_expression ']' // ANSI: expression
    {
      if (parser_options->extbody)
        parser_access_length = strlen(parser_record) - parser_access_start;

      CLAN_debug("rule postfix_expression.2: postfix_expression [ <affex> ]");
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_ARRAY))
        YYABORT;
      clan_relation_new_output_vector($1->elt, $3);
      osl_vector_free($3);
      $$ = $1;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  | postfix_expression '(' ')'
    { 
      // don't save access name of a function
      if (parser_options->extbody) {
        parser_access_extbody->nb_access -= osl_relation_list_count($1) - 1;
        parser_access_start = -1;
      }

      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_FUNCTION))
        YYABORT;
      osl_relation_list_free($1);
      $$ = NULL;
    }
  | postfix_expression '('
    {
      // don't save access name of a function
      if (parser_options->extbody) {
        parser_access_extbody->nb_access -= osl_relation_list_count($1) - 1;
        parser_access_start = -1;
      }
    }
    argument_expression_list ')'
    {
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_FUNCTION))
        YYABORT;
      osl_relation_list_free($1);
      $$ = $4;
    }
  | postfix_expression '.' id_or_clan_keyword
    {
      if (parser_options->extbody)
        parser_access_length = strlen(parser_record) - parser_access_start;

      clan_symbol_p symbol;

      CLAN_debug("rule postfix_expression.4: postfix_expression . "
                 "id_or_clan_keyword");
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_ARRAY))
        YYABORT;
      symbol = clan_symbol_add(&parser_symbol, $3, CLAN_TYPE_FIELD);
      clan_relation_new_output_scalar($1->elt, symbol->key);
      free($3);
      $$ = $1;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  | postfix_expression PTR_OP id_or_clan_keyword
    {
      if (parser_options->extbody)
        parser_access_length = strlen(parser_record) - parser_access_start;

      clan_symbol_p symbol;

      CLAN_debug("rule postfix_expression.5: postfix_expression -> "
                 "id_or_clan_keyword");
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_ARRAY))
        YYABORT;
      symbol = clan_symbol_add(&parser_symbol, $3, CLAN_TYPE_FIELD);
      clan_relation_new_output_scalar($1->elt, symbol->key);
      free($3);
      $$ = $1;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  | postfix_expression unary_increment_operator
    { 
      osl_relation_list_p list;

      CLAN_debug("rule postfix_expression.6: postfix_expression -> "
	         "postfix_expression ++/--");
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_ARRAY))
        YYABORT;
      list = $1;
      // The last reference in the list is also written.
      if (list != NULL) {
        while (list->next != NULL)
          list = list->next;
        list->next = osl_relation_list_node(list->elt);
        list->next->elt->type = OSL_TYPE_WRITE;
      }
      $$ = $1;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));

      // add an empty line in the extbody
      if (parser_options->extbody) {
        osl_extbody_add(parser_access_extbody, -1, -1);
      }
    }
  ;

argument_expression_list:
    assignment_expression
    { $$ = $1; }
  | argument_expression_list ',' assignment_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

unary_expression:
    postfix_expression
    { $$ = $1; }
  | unary_increment_operator unary_expression
    {
      osl_relation_list_p list;

      CLAN_debug("rule unary_expression.2: unary_expression -> "
	         "++/-- unary_expression");
      if (!clan_symbol_update_type(parser_symbol, $2, CLAN_TYPE_ARRAY))
        YYABORT;
      list = $2;
      // The last reference in the list is also written.
      if (list != NULL) {
        while (list->next != NULL)
          list = list->next;
        list->next = osl_relation_list_node(list->elt);
        list->next->elt->type = OSL_TYPE_WRITE;
      }
      $$ = $2;
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));

      // add an empty line in the extbody
      if (parser_options->extbody) {
        osl_extbody_add(parser_access_extbody, -1, -1);
      }
    }
  | unary_operator cast_expression
    { $$ = $2; }
  | SIZEOF unary_expression
    { $$ = $2; }
  | SIZEOF '(' type_name ')'
    { $$ = NULL; }
  ;

unary_operator:
    '&'
  | '*'
  | '+'
  | '-'
  | '~'
  | '!'
  ;

unary_increment_operator:
    INC_OP
  | DEC_OP
  ;

cast_expression:
    unary_expression
    { $$ = $1; }
  | '(' type_name ')' cast_expression
    { $$ = $4; }
  ;

multiplicative_expression:
    cast_expression
    { $$ = $1; }
  | multiplicative_expression '*' cast_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | multiplicative_expression '/' cast_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | multiplicative_expression '%' cast_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

additive_expression:
    multiplicative_expression
    { $$ = $1; }
  | additive_expression '+' multiplicative_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | additive_expression '-' multiplicative_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

shift_expression:
    additive_expression
    { $$ = $1; }
  | shift_expression LEFT_OP additive_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | shift_expression RIGHT_OP additive_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

relational_expression:
    shift_expression
    { $$ = $1; }
  | relational_expression '<' shift_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | relational_expression '>' shift_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | relational_expression LE_OP shift_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | relational_expression GE_OP shift_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

equality_expression:
    relational_expression
    { $$ = $1; }
  | equality_expression EQ_OP relational_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  | equality_expression NE_OP relational_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

and_expression:
    equality_expression
    { $$ = $1; }
  | and_expression '&' equality_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

exclusive_or_expression:
    and_expression
    { $$ = $1; }
  | exclusive_or_expression '^' and_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

inclusive_or_expression:
    exclusive_or_expression
    { $$ = $1; }
  | inclusive_or_expression '|' exclusive_or_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

logical_and_expression:
    inclusive_or_expression
    { $$ = $1; }
  | logical_and_expression AND_OP inclusive_or_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

logical_or_expression:
    logical_and_expression
    { $$ = $1; }
  | logical_or_expression OR_OP logical_and_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

conditional_expression:
    logical_or_expression
    { $$ = $1; }
  | logical_or_expression '?' expression ':' conditional_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
      osl_relation_list_add(&$$, $5);
    }
  ;

assignment_expression:
    conditional_expression
    {
      CLAN_debug("rule assignment_expression.1: conditional_expression;");
      $$ = $1;
      clan_relation_list_define_type($$, OSL_TYPE_READ);
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  | unary_expression assignment_operator assignment_expression
    {
      osl_relation_list_p list;

      CLAN_debug("rule assignment_expression.2: unary_expression "
	         "assignment_operator assignment_expression;");
      if (!clan_symbol_update_type(parser_symbol, $1, CLAN_TYPE_ARRAY))
        YYABORT;
      $$ = $1;
      // Accesses of $1 are READ except the last one which is a WRITE or both.
      clan_relation_list_define_type($$, OSL_TYPE_READ);
      list = $$;
      while (list->next != NULL)
        list = list->next;
      if ($2 == CLAN_TYPE_RDWR) {
        list->next = osl_relation_list_node(list->elt);
        list = list->next;

        // add an empty line in the extbody
        if (parser_options->extbody) {
          osl_extbody_add(parser_access_extbody, -1, -1);
        }
      }
      osl_relation_set_type(list->elt, OSL_TYPE_WRITE);
      osl_relation_list_add(&$$, $3);
      CLAN_debug_call(osl_relation_list_dump(stderr, $$));
    }
  ;

assignment_operator:
    '='
    { $$ = CLAN_TYPE_WRITE; }
  | assignment_rdwr_operator
    { $$ = CLAN_TYPE_RDWR; }
  ;

assignment_rdwr_operator:
    MUL_ASSIGN
  | DIV_ASSIGN
  | MOD_ASSIGN
  | ADD_ASSIGN
  | SUB_ASSIGN
  | LEFT_ASSIGN
  | RIGHT_ASSIGN
  | AND_ASSIGN
  | XOR_ASSIGN
  | OR_ASSIGN
  ;

expression:
    assignment_expression
    { $$ = $1; }
  | expression ',' assignment_expression
    {
      $$ = $1;
      osl_relation_list_add(&$$, $3);
    }
  ;

expression_statement:
    ';'
    {
      CLAN_debug("rule expression_statement.1: ;");
      $$ = NULL;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  | 
    {
      if (parser_options->extbody) {
        parser_access_start = -1;
        parser_access_extbody = osl_extbody_malloc();
      }

      CLAN_strdup(parser_record, scanner_latest_text);
      parser_recording = CLAN_TRUE;
    }
    expression ';'
    {
      osl_statement_p statement;
      osl_body_p body;
      osl_generic_p gen;
      
      CLAN_debug("rule expression_statement.2: expression ;");
      statement = osl_statement_malloc();

      // - 1. Domain
      if (clan_relation_list_nb_elements(parser_stack->constraints) != 1) {
	yyerror("missing label on a statement inside an xfor loop");
        YYABORT;
      }
      statement->domain = osl_relation_clone(parser_stack->constraints->elt);
      osl_relation_set_type(statement->domain, OSL_TYPE_DOMAIN);
      osl_relation_set_attributes(statement->domain, parser_loop_depth, 0,
	                          clan_parser_nb_ld(), CLAN_MAX_PARAMETERS);

      // - 2. Scattering
      statement->scattering = clan_relation_scattering(parser_scattering,
          parser_loop_depth, parser_options->precision);

      // - 3. Array accesses
      statement->access = $2;

      // - 4. Body.
      body = osl_body_malloc();
      body->iterators = clan_symbol_array_to_strings(parser_iterators,
	  parser_loop_depth, parser_xfor_depths, parser_xfor_labels);
      body->expression = osl_strings_encapsulate(parser_record);
      gen = osl_generic_shell(body, osl_body_interface());
      osl_generic_add(&statement->extension, gen);

      if (parser_options->extbody) {
        // Extended body

        // add the last access
        if (parser_access_start != -1) {
          osl_extbody_add(parser_access_extbody,
                          parser_access_start,
                          parser_access_length);
        }

        parser_access_extbody->body = osl_body_clone(body);
        gen = osl_generic_shell(parser_access_extbody, osl_extbody_interface());
        osl_generic_add(&statement->extension, gen);
      }

      parser_recording = CLAN_FALSE;
      parser_record = NULL;
      
      parser_scattering[2*parser_loop_depth]++;

      $$ = statement;
      CLAN_debug_call(osl_statement_dump(stderr, $$));
    }
  ;


// +--------------------------------------------------------------------------+
// |                              ANSI C CASTING                              |
// +--------------------------------------------------------------------------+


constant_expression:
    conditional_expression
  ;

declaration_specifiers:
    storage_class_specifier
  | storage_class_specifier declaration_specifiers
  | type_specifier
  | type_specifier declaration_specifiers
  | type_qualifier
  | type_qualifier declaration_specifiers
  ;

storage_class_specifier:
    TYPEDEF
  | EXTERN
  | STATIC
  | AUTO
  | REGISTER
  ;

type_specifier:
    VOID
  | CHAR
  | SHORT
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | SIGNED
  | UNSIGNED
  | struct_or_union_specifier
  | enum_specifier
  | TYPE_NAME
  ;

struct_or_union_specifier:
    struct_or_union id_or_clan_keyword '{' struct_declaration_list '}' { free($2); }
  | struct_or_union '{' struct_declaration_list '}'
  | struct_or_union id_or_clan_keyword { free($2); }
  ;

struct_or_union:
    STRUCT
  | UNION
  ;

struct_declaration_list:
    struct_declaration
  | struct_declaration_list struct_declaration
  ;

struct_declaration:
    specifier_qualifier_list struct_declarator_list ';'
  ;

specifier_qualifier_list:
    type_specifier specifier_qualifier_list
  | type_specifier
  | type_qualifier specifier_qualifier_list
  | type_qualifier
  ;

struct_declarator_list:
    struct_declarator
  | struct_declarator_list ',' struct_declarator
  ;

struct_declarator:
    declarator
  | ':' constant_expression
  | declarator ':' constant_expression
  ;

enum_specifier:
    ENUM '{' enumerator_list '}'
  | ENUM id_or_clan_keyword '{' enumerator_list '}' { free($2); }
  | ENUM id_or_clan_keyword { free($2); }
  ;

enumerator_list:
    enumerator
  | enumerator_list ',' enumerator
  ;

enumerator:
    id_or_clan_keyword { free($1); }
  | id_or_clan_keyword '=' constant_expression { free($1); }
  ;

type_qualifier:
    CONST
  | VOLATILE
  ;

declarator:
    pointer direct_declarator
  | direct_declarator
  ;

direct_declarator:
    id_or_clan_keyword { free($1); }
  | '(' declarator ')'
  | direct_declarator '[' constant_expression ']'
  | direct_declarator '[' ']'
  | direct_declarator '(' parameter_type_list ')'
  | direct_declarator '(' identifier_list ')'
  | direct_declarator '(' ')'
  ;

pointer:
    '*'
  | '*' type_qualifier_list
  | '*' pointer
  | '*' type_qualifier_list pointer
  ;

type_qualifier_list:
    type_qualifier
  | type_qualifier_list type_qualifier
  ;


parameter_type_list:
    parameter_list
  | parameter_list ',' ELLIPSIS
  ;

parameter_list:
    parameter_declaration
  | parameter_list ',' parameter_declaration
  ;

parameter_declaration:
    declaration_specifiers declarator
  | declaration_specifiers abstract_declarator
  | declaration_specifiers
  ;

identifier_list:
    id_or_clan_keyword { free($1); }
  | identifier_list ',' id_or_clan_keyword { free($3); }
  ;

type_name:
    specifier_qualifier_list
  | specifier_qualifier_list abstract_declarator
  ;

abstract_declarator:
    pointer
  | direct_abstract_declarator
  | pointer direct_abstract_declarator
  ;

direct_abstract_declarator:
    '(' abstract_declarator ')'
  | '[' ']'
  | '[' constant_expression ']'
  | direct_abstract_declarator '[' ']'
  | direct_abstract_declarator '[' constant_expression ']'
  | '(' ')'
  | '(' parameter_type_list ')'
  | direct_abstract_declarator '(' ')'
  | direct_abstract_declarator '(' parameter_type_list ')'
  ;

%%


void yyerror(char *s) {
  int i, line = 1;
  char c = 'C';
  FILE* file;
 
  CLAN_debug("parse error notified");

  if (!parser_options->autoscop) {
    fprintf(stderr, "[Clan] Error: %s at line %d, column %d.\n", s,
        scanner_line, scanner_column - 1);

    // Print a message to show where is the problem.
    if ((parser_options != NULL) && (parser_options->name != NULL)) {
      file = fopen(parser_options->name, "r");
      if (file != NULL) {
        // Go to the right line.
        while (line != scanner_line) {
          c = fgetc(file);
          if (c == '\n')
            line++;
        }

        // Print the line.
        while (c != EOF) {
          c = fgetc(file);
          fprintf(stderr, "%c", c);
          if (c == '\n')
            break;
        }

        // Print the situation line.
        for (i = 0; i < scanner_column - 1; i++) {
          if (i < scanner_column - 5)
            fprintf(stderr, " ");
          else if (i < scanner_column - 2)
            fprintf(stderr, "~");
          else
            fprintf(stderr, "^\n");
        }
        fclose(file);
      } else {
        CLAN_warning("cannot open input file");
      }
    }
  }
  parser_error = CLAN_TRUE;
}


/**
 * clan_parser_state_print function:
 * this function "pretty" prints the parser state to a file.
 */
void clan_parser_state_print(FILE* file) {
  int i;

  fprintf(file, "+-- clan parser state\n");
  fprintf(file, "|\t|\n");

  // SCoP.
  fprintf(file, "|\tparser_scop [SCoP in construction]\n");
  fprintf(file, "|\t|\t|\n");
  osl_scop_idump(file, parser_scop, 2);
  fprintf(file, "|\t|\n");

  // Symbol table.
  fprintf(file, "|\tparser_symbol [Symbol table]\n");
  fprintf(file, "|\t|\t|\n");
  clan_symbol_print_structure(file, parser_symbol, 2);
  fprintf(file, "|\t|\n");

  // Recording boolean.
  fprintf(file, "|\tparser_recording [Boolean: do we record or not?] = %d\n",
          parser_recording);
  fprintf(file, "|\t|\n");

  // Recorded body.
  fprintf(file, "|\tparser_record [Statement body] = ");
  if (parser_record == NULL)
    fprintf(file, "(NULL)\n");
  else
    fprintf(file, "%s\n", parser_record);
  fprintf(file, "|\t|\n");

  // Loop depth.
  fprintf(file, "|\tparser_loop_depth [Current loop depth] = %d\n",
          parser_loop_depth);
  fprintf(file, "|\t|\n");

  // If depth.
  fprintf(file, "|\tparser_if_depth [Current if depth] = %d\n",
          parser_if_depth);
  fprintf(file, "|\t|\n");

  // Scattering.
  fprintf(file, "|\tparser_scattering [Current statement scattering]\n");
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\t+-- ");
  for (i = 0; i < 2 * parser_loop_depth + 1; i++)
    printf("%d ", parser_scattering[i]);
  fprintf(file, "\n");
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\n");

  // Iterators.
  fprintf(file, "|\tparser_iterators [Current iterator list]\n");
  fprintf(file, "|\t|\t|\n");
  if (parser_loop_depth > 0) {
    for (i = 0; i < parser_loop_depth; i++) {
      fprintf(file, "|\t|\tparser_iterators[%d]\n", i);
      fprintf(file, "|\t|\t|\t|\n");
      clan_symbol_print_structure(file, parser_iterators[i], 3);
      if (i == parser_loop_depth - 1)
	fprintf(file, "|\t|\t|\n");
    }
  } else {
    fprintf(file, "|\t|\t+-- (none)\n");
    fprintf(file, "|\t|\t|\n");
  }
  fprintf(file, "|\t|\n");

  // Iteration domain stack.
  fprintf(file, "|\tparser_domain [Iteration domain stack]\n");
  fprintf(file, "|\t|\t|\n");
  clan_domain_idump(file, parser_stack, 2);
  fprintf(file, "|\t|\n");

  // Number of local dimensions per depth.
  fprintf(file, "|\tparser_nb_local_dims [Nb of local dims per depth]\n");
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\t+-- ");
  if (parser_loop_depth > 0) {
    for (i = 0; i < parser_loop_depth; i++)
      printf("%d ", parser_nb_local_dims[i]);
      fprintf(file, "\n");
    } else {
    fprintf(file, "(none)\n");
  }
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\n");

  // Number of parameters.
  fprintf(file, "|\tparser_nb_parameters [Nb of parameter symbols] = %d\n",
          parser_nb_parameters);
  fprintf(file, "|\t|\n");

  // Boolean valid else per if depth.
  fprintf(file, "|\tparser_valid_else [Boolean: OK for else per depth]\n");
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\t+-- ");
  if (parser_if_depth > 0) {
    for (i = 0; i < parser_if_depth; i++)
      printf("%d ", parser_valid_else[i]);
    fprintf(file, "\n");
  } else {
    fprintf(file, "(none)\n");
  }
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\n");

  // Indentation.
  fprintf(file, "|\tparser_indent [SCoP indentation] = %d\n", parser_indent);
  fprintf(file, "|\t|\n");

  // Parse error boolean.
  fprintf(file, "|\tparser_error [Boolean: parse error] = %d\n", parser_error);
  fprintf(file, "|\t|\n");

  // xfor number of nests, depths and labels.
  fprintf(file, "|\txfor management [nb of nests, depths and labels]\n");
  fprintf(file, "|\t|\t|\n");
  if (parser_xfor_nb_nests > 0) {
    fprintf(file, "|\t|\t|  nest | depth | label\n");
    for (i = 0; i < parser_xfor_nb_nests; i++) {
      printf("|\t|\t|   [%d] |     %d |     %d\n",
	     i, parser_xfor_depths[i], parser_xfor_labels[i]);
    }
  } else {
    fprintf(file, "|\t|\t|  (no xfor loop)\n");
  }
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\n");
  
  // loop sanity sentinels
  fprintf(file, "|\tloop sanity sentinels [booleans min/max/floord/ceild]\n");
  fprintf(file, "|\t|\t|\n");
  if (parser_xfor_index > 0) {
    fprintf(file, "|\t|\t|  index | min | max | floord | ceild\n");
    for (i = 0; i < parser_xfor_index; i++) {
      printf("|\t|\t|  [%d] |   %d |   %d |      %d |     %d\n",
	     i, parser_min[i], parser_max[i],
	     parser_floord[i], parser_ceild[i]);
    }
  } else {
    fprintf(file, "|\t|\t|  (no (x)for loop indices)\n");
  }
  fprintf(file, "|\t|\t|\n");
  fprintf(file, "|\t|\n");
  
  fprintf(file, "|\n");
}


void clan_parser_add_ld() {
  parser_nb_local_dims[parser_loop_depth + parser_if_depth]++;

  if (CLAN_DEBUG) {
    int i;
    CLAN_debug("parser_nb_local_dims updated");
    for (i = 0; i <= parser_loop_depth + parser_if_depth; i++)
      fprintf(stderr, "%d:%d ", i, parser_nb_local_dims[i]);
    fprintf(stderr, "\n");
  }
  
  if (clan_parser_nb_ld() > CLAN_MAX_LOCAL_DIMS)
    CLAN_error("CLAN_MAX_LOCAL_DIMS reached, recompile with a higher value");
}


int clan_parser_nb_ld() {
  int i, nb_ld = 0;

  for (i = 0; i <= parser_loop_depth + parser_if_depth; i++)
    nb_ld += parser_nb_local_dims[i]; 
  return nb_ld;
}


void clan_parser_increment_loop_depth() {
  parser_loop_depth++;
  if ((parser_loop_depth + parser_if_depth) > CLAN_MAX_DEPTH)
    CLAN_error("CLAN_MAX_DEPTH reached, recompile with a higher value");
}


int clan_parser_is_loop_sane(osl_relation_list_p initialization,
                             osl_relation_list_p condition, int* stride) {
  int i, step;

  // Check there is the same number of elements in all for parts.
  if ((clan_relation_list_nb_elements(initialization) != parser_xfor_index) ||
      (clan_relation_list_nb_elements(condition) != parser_xfor_index)) {
    yyerror("not the same number of elements in all loop parts");
    return 0;
  }

  // Check that all bounds and strides are consistent.
  for (i = 0; i < parser_xfor_index; i++) {
    step = stride[i];
    if ((step == 0) ||
	((step > 0) && parser_min[i])    ||
	((step > 0) && parser_floord[i]) ||
	((step < 0) && parser_max[i])    ||
	((step < 0) && parser_ceild[i])) {
      osl_relation_list_free(initialization);
      osl_relation_list_free(condition);
      free(stride);
      if (step == 0)
	yyerror("unsupported zero loop stride");
      else if (step > 0)
	yyerror("illegal min or floord in forward loop initialization");
      else
	yyerror("illegal max or ceild in backward loop initialization");
      return 0;
    }
    parser_ceild[i]  = 0;
    parser_floord[i] = 0;
    parser_min[i]    = 0;
    parser_max[i]    = 0;
  }
  return 1;
}


/**
 * clan_parser_state_malloc function:
 * this function achieves the memory allocation for the "parser state".
 * \param[in] precision Precision of the integer elements.
 */
void clan_parser_state_malloc(int precision) {
  int nb_columns, depth;

  nb_columns        = CLAN_MAX_DEPTH + CLAN_MAX_LOCAL_DIMS +
                      CLAN_MAX_PARAMETERS + 2;
  depth             = CLAN_MAX_DEPTH;
  parser_stack      = clan_domain_malloc();
  parser_stack->constraints = osl_relation_list_malloc();
  parser_stack->constraints->elt = osl_relation_pmalloc(precision,
      0, nb_columns);
  CLAN_malloc(parser_nb_local_dims, int*, depth * sizeof(int));
  CLAN_malloc(parser_valid_else, int*, depth * sizeof(int));
  CLAN_malloc(parser_scattering, int*, (2 * depth + 1) * sizeof(int));
  CLAN_malloc(parser_iterators, clan_symbol_p*, depth * sizeof(clan_symbol_p));
  CLAN_malloc(parser_ceild,  int*, CLAN_MAX_XFOR_INDICES * sizeof(int));
  CLAN_malloc(parser_floord, int*, CLAN_MAX_XFOR_INDICES * sizeof(int));
  CLAN_malloc(parser_min,    int*, CLAN_MAX_XFOR_INDICES * sizeof(int));
  CLAN_malloc(parser_max,    int*, CLAN_MAX_XFOR_INDICES * sizeof(int));
  CLAN_malloc(parser_xfor_depths, int*, CLAN_MAX_DEPTH * sizeof(int));
  CLAN_malloc(parser_xfor_labels, int*, CLAN_MAX_DEPTH * sizeof(int));
}


/**
 * clan_parser_state_free function:
 * this function frees the memory allocated for the "parser state", with the
 * exception of the parser_scop.
 */
void clan_parser_state_free() {
  clan_symbol_free(parser_symbol);
  free(parser_scattering);
  free(parser_iterators);
  free(parser_nb_local_dims);
  free(parser_valid_else);
  free(parser_ceild);
  free(parser_floord);
  free(parser_min);
  free(parser_max);
  free(parser_xfor_depths);
  free(parser_xfor_labels);
  clan_domain_drop(&parser_stack);
}


/**
 * clan_parser_state_initialize function:
 * this function achieves the initialization of the "parser state", with
 * the exception of parser_scop.
 */
void clan_parser_state_initialize(clan_options_p options) {
  int i;

  parser_symbol        = NULL;
  parser_loop_depth    = 0;
  parser_options       = options;
  parser_recording     = CLAN_FALSE;
  parser_record        = NULL;
  parser_if_depth      = 0;
  parser_xfor_nb_nests = 0;
  parser_xfor_index    = 0;
  parser_indent        = CLAN_UNDEFINED;
  parser_error         = CLAN_FALSE;
  parser_autoscop      = CLAN_FALSE;
  parser_line_start    = 1;
  parser_line_end      = 1;
  parser_column_start  = 1;
  parser_column_end    = 1;
  parser_nb_parameters = 0;

  for (i = 0; i < CLAN_MAX_XFOR_INDICES; i++) {
    parser_ceild[i]  = 0;
    parser_floord[i] = 0;
    parser_min[i]    = 0;
    parser_max[i]    = 0;
  }

  for (i = 0; i < CLAN_MAX_DEPTH; i++) {
    parser_nb_local_dims[i] = 0;
    parser_valid_else[i] = 0;
    parser_iterators[i] = NULL;
    parser_xfor_depths[i] = 0;
    parser_xfor_labels[i] = CLAN_UNDEFINED;
  }

  for (i = 0; i < 2 * CLAN_MAX_DEPTH + 1; i++)
    parser_scattering[i] = 0;
}


/**
 * clan_parser_reinitialize function:
 * this function frees the temporary dynamic variables of the parser and
 * reset the variables to default values. It is meant to be used for a
 * clean restart after a parse error.
 */
void clan_parser_reinitialize() {
  int i;
  
  free(parser_record);
  clan_symbol_free(parser_symbol);
  for (i = 0; i < parser_loop_depth; i++)
    clan_symbol_free(parser_iterators[i]);
  while (parser_stack->next != NULL)
    clan_domain_drop(&parser_stack);
  osl_scop_free(parser_scop);
  clan_parser_state_initialize(parser_options);
}


/**
 * clan_parser_autoscop function:
 * this functions performs the automatic extraction of SCoPs from the input
 * file. It leaves the SCoP pragmas already set by the user intact (note that
 * as a consequence, user-SCoPs cannot be inserted to a larger SCoP).
 * It writes a file (named by the CLAN_AUTOPRAGMA_FILE macro) with the input
 * code where new SCoP pragmas have been inserted. If the option -autoscop
 * is set, it puts the list of SCoPs (including automatically discovered
 * SCoPs and user-SCoPs) in parser_scop.
 */
void clan_parser_autoscop() {
  int new_scop, nb_scops = 0;
  int line, column, restart_line, restart_column;
  long position;
  char c;
  int coordinates[5][CLAN_MAX_SCOPS]; // 0, 1: line start, end
                                      // 2, 3: column start, end
				      // 4: autoscop or not
 
  while (1) {
    // For the automatic extraction, we parse everything except user-SCoPs.
    if (!scanner_pragma)
      scanner_parsing = CLAN_TRUE;
    yyparse();

    new_scop = (parser_line_end != 1) || (parser_column_end != 1);
    restart_line = (new_scop) ? parser_line_end : scanner_line;
    restart_column = (new_scop) ? parser_column_end : scanner_column;
    if (CLAN_DEBUG) {
      if (new_scop)
	fprintf(stderr, "[Clan] Debug: new autoscop, ");
      else
	fprintf(stderr, "[Clan] Debug: no autoscop, ");
      fprintf(stderr, "restart at line %d, column %d\n",
	      restart_line, restart_column);
    }
 
    if (parser_error || new_scop) {
      if (new_scop) {
        // If a new SCoP has been found, store its coordinates.
        if (nb_scops == CLAN_MAX_SCOPS)
          CLAN_error("too many SCoPs! Change CLAN_MAX_SCOPS and recompile.");
        coordinates[0][nb_scops] = parser_line_start;
        coordinates[1][nb_scops] = parser_line_end;
        coordinates[2][nb_scops] = parser_column_start;
        coordinates[3][nb_scops] = parser_column_end;
        coordinates[4][nb_scops] = CLAN_TRUE;
        if (CLAN_DEBUG) {
          fprintf(stderr, "[Clan] Debug: AutoSCoP [%d,%d -> %d,%d]\n",
                  coordinates[0][nb_scops], coordinates[2][nb_scops],
                  coordinates[1][nb_scops], coordinates[3][nb_scops] - 1);
        }
        // Let's go for the next SCoP.
        parser_autoscop = CLAN_FALSE;
        nb_scops++;
      } else if (scanner_scop_start != CLAN_UNDEFINED) {
        // If the start of a user-SCoP is detected, store its coordinate.
	coordinates[0][nb_scops] = scanner_scop_start;
      } else if (scanner_scop_end != CLAN_UNDEFINED) {
        // If the end of a user-SCoP is detected, store its coordinate.
	coordinates[1][nb_scops] = scanner_scop_end;
	coordinates[2][nb_scops] = 0;
	coordinates[3][nb_scops] = 0;
	coordinates[4][nb_scops] = CLAN_FALSE;
        if (CLAN_DEBUG) {
          fprintf(stderr, "[Clan] Debug: user-SCoP [%d,%d -> %d,%d]\n",
                  coordinates[0][nb_scops], coordinates[2][nb_scops],
                  coordinates[1][nb_scops], coordinates[3][nb_scops]);
        }
	nb_scops++;
      }

      // Restart after the SCoP or after the error.
      rewind(yyin);
      line = 1;
      column = 1;
      while ((line != restart_line) || (column != restart_column)) {
        c = fgetc(yyin);
        column++;
        if (c == '\n') {
          line++;
          column = 1;
        }
      }
    }

    // Reinitialize the scanner and the parser for a clean restart.
    clan_scanner_free();
    clan_scanner_reinitialize(scanner_pragma, restart_line, restart_column);
    clan_parser_reinitialize();
    yyrestart(yyin);

    // Check whether we reached the end of file or not.
    position = ftell(yyin);
    c = fgetc(yyin);
    if (fgetc(yyin) == EOF)
      break;
    else 
      fseek(yyin, position, SEEK_SET);
  }
 
  // Write the code with the inserted SCoP pragmas in CLAN_AUTOPRAGMA_FILE.
  rewind(yyin);
  clan_scop_print_autopragma(yyin, nb_scops, coordinates);

  // Use the temporary file for usual parsing.
  scanner_line = 1;
  scanner_column = 1;
  scanner_pragma = CLAN_FALSE;
  parser_options->autoscop = CLAN_FALSE;
  if ((yyin = fopen(CLAN_AUTOPRAGMA_FILE, "r")) == NULL)
    CLAN_error("cannot create the temporary file");
  yyparse();
  fclose(yyin);

  // Update the SCoP coordinates with those of the original file.
  clan_scop_update_coordinates(parser_scop, coordinates);
  parser_options->autoscop = CLAN_TRUE;
  
  if (remove(CLAN_AUTOPRAGMA_FILE))
    CLAN_warning("cannot delete temporary file");
}


/**
 * clan_parse function:
 * this function parses a file to extract a SCoP and returns, if successful,
 * a pointer to the osl_scop_t structure.
 * \param input   The file to parse (already open).
 * \param options Options for file parsing.
 */
osl_scop_p clan_parse(FILE* input, clan_options_p options) {
  osl_scop_p scop;
  yyin = input;

  clan_parser_state_malloc(options->precision);
  clan_parser_state_initialize(options);
  clan_scanner_initialize();
  yyrestart(yyin);  //restart scanning another file
  parser_scop = NULL;

  if (!options->autoscop)
    yyparse();
  else
    clan_parser_autoscop();

  CLAN_debug("parsing done");

  clan_scanner_free();
  
  if (!parser_error)
    scop = parser_scop;
  else
    scop = NULL;

  clan_parser_state_free();
  CLAN_debug("parser state successfully freed");

  return scop;
}
