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

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "corba-gram.y"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "param_metadata.h"
#include "semantic.h"

#if 0
#define YYERROR_VERBOSE
#endif
#define YYERROR_VERBOSE

int yylex(void);
void yyerror(char *s);

#line 87 "corba-gram.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "corba-gram.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_TOK_INTERFACE = 3,              /* TOK_INTERFACE  */
  YYSYMBOL_TOK_LEFT_CUR_BRAKET = 4,        /* TOK_LEFT_CUR_BRAKET  */
  YYSYMBOL_TOK_RIGHT_CUR_BRAKET = 5,       /* TOK_RIGHT_CUR_BRAKET  */
  YYSYMBOL_TOK_LEFT_PARENTHESIS = 6,       /* TOK_LEFT_PARENTHESIS  */
  YYSYMBOL_TOK_LEFT_BRAKET = 7,            /* TOK_LEFT_BRAKET  */
  YYSYMBOL_TOK_RIGHT_BRAKET = 8,           /* TOK_RIGHT_BRAKET  */
  YYSYMBOL_TOK_RIGHT_PARENTHESIS = 9,      /* TOK_RIGHT_PARENTHESIS  */
  YYSYMBOL_TOK_COMMA = 10,                 /* TOK_COMMA  */
  YYSYMBOL_TOK_SEMICOLON = 11,             /* TOK_SEMICOLON  */
  YYSYMBOL_TOK_IN = 12,                    /* TOK_IN  */
  YYSYMBOL_TOK_OUT = 13,                   /* TOK_OUT  */
  YYSYMBOL_TOK_INOUT = 14,                 /* TOK_INOUT  */
  YYSYMBOL_TOK_FILE = 15,                  /* TOK_FILE  */
  YYSYMBOL_TOK_AT = 16,                    /* TOK_AT  */
  YYSYMBOL_TOK_STATIC = 17,                /* TOK_STATIC  */
  YYSYMBOL_TOK_UNSIGNED = 18,              /* TOK_UNSIGNED  */
  YYSYMBOL_TOK_VOID = 19,                  /* TOK_VOID  */
  YYSYMBOL_TOK_SHORT = 20,                 /* TOK_SHORT  */
  YYSYMBOL_TOK_LONG = 21,                  /* TOK_LONG  */
  YYSYMBOL_TOK_LONGLONG = 22,              /* TOK_LONGLONG  */
  YYSYMBOL_TOK_INT = 23,                   /* TOK_INT  */
  YYSYMBOL_TOK_FLOAT = 24,                 /* TOK_FLOAT  */
  YYSYMBOL_TOK_DOUBLE = 25,                /* TOK_DOUBLE  */
  YYSYMBOL_TOK_CHAR = 26,                  /* TOK_CHAR  */
  YYSYMBOL_TOK_WCHAR = 27,                 /* TOK_WCHAR  */
  YYSYMBOL_TOK_BOOLEAN = 28,               /* TOK_BOOLEAN  */
  YYSYMBOL_TOK_STRING = 29,                /* TOK_STRING  */
  YYSYMBOL_TOK_WSTRING = 30,               /* TOK_WSTRING  */
  YYSYMBOL_TOK_ANY = 31,                   /* TOK_ANY  */
  YYSYMBOL_TOK_ERROR = 32,                 /* TOK_ERROR  */
  YYSYMBOL_TOK_EQUAL = 33,                 /* TOK_EQUAL  */
  YYSYMBOL_TOK_DBLQUOTE = 34,              /* TOK_DBLQUOTE  */
  YYSYMBOL_TOK_ENUM = 35,                  /* TOK_ENUM  */
  YYSYMBOL_TOK_INCLUDE = 36,               /* TOK_INCLUDE  */
  YYSYMBOL_TOK_CONSTRAINTS = 37,           /* TOK_CONSTRAINTS  */
  YYSYMBOL_TOK_IMPLEMENTS = 38,            /* TOK_IMPLEMENTS  */
  YYSYMBOL_TOK_PROCESSORS = 39,            /* TOK_PROCESSORS  */
  YYSYMBOL_TOK_PROCESSOR = 40,             /* TOK_PROCESSOR  */
  YYSYMBOL_TOK_IDENTIFIER = 41,            /* TOK_IDENTIFIER  */
  YYSYMBOL_TOK_HEADER = 42,                /* TOK_HEADER  */
  YYSYMBOL_NUMBER = 43,                    /* NUMBER  */
  YYSYMBOL_YYACCEPT = 44,                  /* $accept  */
  YYSYMBOL_start = 45,                     /* start  */
  YYSYMBOL_includes = 46,                  /* includes  */
  YYSYMBOL_47_1 = 47,                      /* $@1  */
  YYSYMBOL_interface = 48,                 /* interface  */
  YYSYMBOL_49_2 = 49,                      /* $@2  */
  YYSYMBOL_prototypes = 50,                /* prototypes  */
  YYSYMBOL_annotated_prototype = 51,       /* annotated_prototype  */
  YYSYMBOL_annotations = 52,               /* annotations  */
  YYSYMBOL_annotation = 53,                /* annotation  */
  YYSYMBOL_54_3 = 54,                      /* $@3  */
  YYSYMBOL_55_4 = 55,                      /* $@4  */
  YYSYMBOL_56_5 = 56,                      /* $@5  */
  YYSYMBOL_constraints_list = 57,          /* constraints_list  */
  YYSYMBOL_constraint = 58,                /* constraint  */
  YYSYMBOL_59_6 = 59,                      /* $@6  */
  YYSYMBOL_processor_list = 60,            /* processor_list  */
  YYSYMBOL_processor = 61,                 /* processor  */
  YYSYMBOL_62_7 = 62,                      /* $@7  */
  YYSYMBOL_processor_params = 63,          /* processor_params  */
  YYSYMBOL_processor_param = 64,           /* processor_param  */
  YYSYMBOL_prototype = 65,                 /* prototype  */
  YYSYMBOL_66_8 = 66,                      /* $@8  */
  YYSYMBOL_67_9 = 67,                      /* $@9  */
  YYSYMBOL_68_10 = 68,                     /* $@10  */
  YYSYMBOL_69_11 = 69,                     /* $@11  */
  YYSYMBOL_70_12 = 70,                     /* $@12  */
  YYSYMBOL_71_13 = 71,                     /* $@13  */
  YYSYMBOL_72_14 = 72,                     /* $@14  */
  YYSYMBOL_73_15 = 73,                     /* $@15  */
  YYSYMBOL_74_16 = 74,                     /* $@16  */
  YYSYMBOL_75_17 = 75,                     /* $@17  */
  YYSYMBOL_76_18 = 76,                     /* $@18  */
  YYSYMBOL_77_19 = 77,                     /* $@19  */
  YYSYMBOL_78_20 = 78,                     /* $@20  */
  YYSYMBOL_79_21 = 79,                     /* $@21  */
  YYSYMBOL_80_22 = 80,                     /* $@22  */
  YYSYMBOL_81_23 = 81,                     /* $@23  */
  YYSYMBOL_82_24 = 82,                     /* $@24  */
  YYSYMBOL_83_25 = 83,                     /* $@25  */
  YYSYMBOL_84_26 = 84,                     /* $@26  */
  YYSYMBOL_85_27 = 85,                     /* $@27  */
  YYSYMBOL_86_28 = 86,                     /* $@28  */
  YYSYMBOL_87_29 = 87,                     /* $@29  */
  YYSYMBOL_88_30 = 88,                     /* $@30  */
  YYSYMBOL_89_31 = 89,                     /* $@31  */
  YYSYMBOL_90_32 = 90,                     /* $@32  */
  YYSYMBOL_91_33 = 91,                     /* $@33  */
  YYSYMBOL_92_34 = 92,                     /* $@34  */
  YYSYMBOL_93_35 = 93,                     /* $@35  */
  YYSYMBOL_94_36 = 94,                     /* $@36  */
  YYSYMBOL_95_37 = 95,                     /* $@37  */
  YYSYMBOL_96_38 = 96,                     /* $@38  */
  YYSYMBOL_97_39 = 97,                     /* $@39  */
  YYSYMBOL_arguments0 = 98,                /* arguments0  */
  YYSYMBOL_arguments1 = 99,                /* arguments1  */
  YYSYMBOL_argument = 100,                 /* argument  */
  YYSYMBOL_direction = 101,                /* direction  */
  YYSYMBOL_data_type = 102,                /* data_type  */
  YYSYMBOL_enum_type = 103,                /* enum_type  */
  YYSYMBOL_numeric_type = 104,             /* numeric_type  */
  YYSYMBOL_array_type = 105                /* array_type  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  2
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   182

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  44
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  62
/* YYNRULES -- Number of rules.  */
#define YYNRULES  111
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  203

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   298


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,    43,    43,    44,    47,    48,    48,    51,    51,    56,
      57,    61,    64,    66,    70,    70,    71,    71,    71,    75,
      76,    80,    81,    82,    82,    86,    87,    91,    91,    95,
      96,   100,   101,   104,   104,   104,   104,   104,   105,   105,
     105,   105,   105,   106,   106,   106,   106,   106,   107,   107,
     107,   107,   107,   108,   108,   108,   108,   108,   109,   109,
     109,   109,   109,   110,   110,   110,   110,   110,   111,   111,
     111,   111,   111,   112,   116,   117,   121,   122,   123,   124,
     127,   128,   129,   130,   131,   134,   135,   136,   139,   140,
     141,   142,   143,   144,   145,   146,   147,   148,   151,   154,
     155,   156,   157,   158,   159,   162,   163,   164,   165,   166,
     167,   168
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "TOK_INTERFACE",
  "TOK_LEFT_CUR_BRAKET", "TOK_RIGHT_CUR_BRAKET", "TOK_LEFT_PARENTHESIS",
  "TOK_LEFT_BRAKET", "TOK_RIGHT_BRAKET", "TOK_RIGHT_PARENTHESIS",
  "TOK_COMMA", "TOK_SEMICOLON", "TOK_IN", "TOK_OUT", "TOK_INOUT",
  "TOK_FILE", "TOK_AT", "TOK_STATIC", "TOK_UNSIGNED", "TOK_VOID",
  "TOK_SHORT", "TOK_LONG", "TOK_LONGLONG", "TOK_INT", "TOK_FLOAT",
  "TOK_DOUBLE", "TOK_CHAR", "TOK_WCHAR", "TOK_BOOLEAN", "TOK_STRING",
  "TOK_WSTRING", "TOK_ANY", "TOK_ERROR", "TOK_EQUAL", "TOK_DBLQUOTE",
  "TOK_ENUM", "TOK_INCLUDE", "TOK_CONSTRAINTS", "TOK_IMPLEMENTS",
  "TOK_PROCESSORS", "TOK_PROCESSOR", "TOK_IDENTIFIER", "TOK_HEADER",
  "NUMBER", "$accept", "start", "includes", "$@1", "interface", "$@2",
  "prototypes", "annotated_prototype", "annotations", "annotation", "$@3",
  "$@4", "$@5", "constraints_list", "constraint", "$@6", "processor_list",
  "processor", "$@7", "processor_params", "processor_param", "prototype",
  "$@8", "$@9", "$@10", "$@11", "$@12", "$@13", "$@14", "$@15", "$@16",
  "$@17", "$@18", "$@19", "$@20", "$@21", "$@22", "$@23", "$@24", "$@25",
  "$@26", "$@27", "$@28", "$@29", "$@30", "$@31", "$@32", "$@33", "$@34",
  "$@35", "$@36", "$@37", "$@38", "$@39", "arguments0", "arguments1",
  "argument", "direction", "data_type", "enum_type", "numeric_type",
  "array_type", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-86)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-112)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     -86,     6,   -86,     1,   -32,    -7,   -86,   -86,   -86,    40,
      93,   -86,   -86,    98,    94,   -86,     2,   -86,    95,   -86,
     -22,    69,    58,   -86,   100,   101,   102,   104,   105,   106,
     107,   -86,   -86,   -86,   -86,   -86,    74,   -86,   -86,    75,
     -86,   110,   -86,   112,   113,    79,    80,   115,   -86,   -86,
     -86,   -86,   -86,   -86,   -86,   -86,   -86,   -36,   -86,    82,
     -86,   -86,   -29,   118,   119,   120,   121,    -5,   117,   124,
     125,   126,   127,   -86,   -86,    86,    91,   -86,   103,    61,
     -86,   -86,   -86,   -86,    96,    97,    60,    60,   -86,   -86,
     108,    -3,   -86,    -5,   122,    60,    60,   -86,   -86,   -86,
     -86,   -86,   -86,   -86,   129,   -86,    27,   -86,   134,   136,
     139,   -86,   -86,   133,   -86,   -86,   -86,   -86,   140,   141,
     142,    63,   -86,   109,   111,   114,   138,   144,   -86,   -86,
     132,   -86,   145,   147,   -86,   -86,   -86,   -86,   -86,   -86,
     -86,   116,    -2,   -86,    60,    60,   123,     3,   -86,   -86,
     -86,    60,    60,   148,   -86,   150,   152,   151,   -86,   -86,
     143,   -86,   132,   153,   154,   -86,   -86,   -86,   128,   130,
     -86,   157,   158,   -86,   -86,   -86,   -86,   159,   161,   -86,
     -86,   -86,   -86,   131,   -86,   -86,   162,   163,   146,    92,
     -86,   164,   165,   -86,   -86,    22,   -86,   131,   -86,   -86,
     -86,   -86,   -86
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       2,     4,     1,     0,     0,     0,     3,     7,     5,     0,
       0,     9,     6,    12,     0,    10,     0,     8,     0,    97,
       0,     0,     0,    96,    99,   100,   101,   102,   103,   104,
      90,    91,    92,    93,    94,    95,     0,    13,    11,     0,
      89,     0,    73,     0,     0,     0,     0,     0,    99,   100,
     101,   102,   103,   104,    88,    48,    33,     0,    16,     0,
      53,    68,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    49,    34,     0,     0,    23,     0,     0,
      19,    14,    54,    69,     0,     0,     0,     0,    38,    43,
       0,     0,    17,     0,     0,     0,     0,    58,    63,    79,
      85,    86,    87,    50,    75,    76,     0,    35,     0,     0,
       0,    21,    22,     0,    20,    15,    55,    70,     0,     0,
       0,     0,    98,     0,     0,     0,     0,     0,    39,    44,
       0,    18,     0,     0,    59,    64,    51,    78,    77,    83,
      80,     0,     0,    36,     0,     0,     0,     0,    25,    56,
      71,     0,     0,     0,    84,     0,     0,     0,    40,    45,
       0,    24,     0,     0,     0,    60,    65,    52,     0,     0,
      37,     0,     0,    27,    26,    57,    72,     0,     0,    81,
      82,    41,    46,     0,    61,    66,     0,     0,     0,     0,
      29,     0,     0,    42,    47,     0,    28,     0,    62,    67,
      31,    32,    30
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
     -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,
     -86,   -86,   -86,   -86,    68,   -86,   -86,    15,   -86,   -86,
     -19,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,
     -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,
     -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,   -86,
     -86,   -86,   -86,   -86,   -85,   -86,    59,   -86,   -21,   -86,
     160,   -20
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     1,     3,    10,     6,     9,    13,    15,    16,    37,
      94,    67,   113,    79,    80,    90,   147,   148,   183,   189,
     190,    38,    64,    87,   127,   157,   108,   144,   171,   186,
     109,   145,   172,   187,    63,    86,   120,   153,    69,    95,
     132,   163,   118,   151,   177,   191,   119,   152,   178,   192,
      70,    96,   133,   164,   103,   104,   105,   106,    39,   125,
      40,    41
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      46,    47,   107,    18,     4,    65,     2,    66,   161,     7,
     116,   117,    71,   162,    72,    43,    44,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    77,     8,    78,     5,   111,   155,
     112,   156,    19,    36,    11,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,   158,
     159,    99,   122,   200,   137,   201,   165,   166,   123,   -74,
      92,    93,   100,   101,   102,   100,   101,   102,    48,    49,
      50,    51,    52,    53,    19,   124,   126,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,   196,   197,    14,    12,    17,    42,  -105,  -106,  -107,
      45,  -108,  -109,  -110,  -111,    55,    56,    57,    58,    59,
      60,    61,    62,    68,    73,    74,    81,    88,    75,    76,
      82,    83,    89,   115,    84,    85,    91,    97,    98,   121,
     128,   110,   129,   130,   131,   142,   134,   135,   146,   173,
     139,   136,   140,   143,   149,   141,   150,   154,   168,   167,
     169,   114,   170,   160,   175,   176,   181,   182,   184,   179,
     185,   180,   188,   193,   194,   198,   199,   174,   202,   195,
     138,     0,    54
};

static const yytype_int16 yycheck[] =
{
      21,    21,    87,     1,     3,    41,     0,    43,     5,    41,
      95,    96,    41,    10,    43,    37,    38,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    39,    42,    41,    36,    41,    41,
      43,    43,    15,    41,     4,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,   144,
     145,     1,    35,    41,     1,    43,   151,   152,    41,     9,
       9,    10,    12,    13,    14,    12,    13,    14,    20,    21,
      22,    23,    24,    25,    15,   106,   106,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,     9,    10,     5,    11,    11,    11,     7,     7,     7,
      41,     7,     7,     7,     7,    41,    41,     7,     6,     6,
      41,    41,     7,    41,     6,     6,     9,    41,     8,     8,
       6,     6,    41,    11,     8,     8,    33,    41,    41,    10,
       6,    33,     6,     4,    11,     7,     6,     6,    16,     6,
      41,     9,    41,     9,     9,    41,     9,    41,     8,    11,
       8,    93,    11,    40,    11,    11,     9,     9,     9,    41,
       9,    41,    41,    11,    11,    11,    11,   162,   197,    33,
     121,    -1,    22
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,    45,     0,    46,     3,    36,    48,    41,    42,    49,
      47,     4,    11,    50,     5,    51,    52,    11,     1,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    41,    53,    65,   102,
     104,   105,    11,    37,    38,    41,   102,   105,    20,    21,
      22,    23,    24,    25,   104,    41,    41,     7,     6,     6,
      41,    41,     7,    78,    66,    41,    43,    55,    41,    82,
      94,    41,    43,     6,     6,     8,     8,    39,    41,    57,
      58,     9,     6,     6,     8,     8,    79,    67,    41,    41,
      59,    33,     9,    10,    54,    83,    95,    41,    41,     1,
      12,    13,    14,    98,    99,   100,   101,    98,    70,    74,
      33,    41,    43,    56,    58,    11,    98,    98,    86,    90,
      80,    10,    35,    41,   102,   103,   105,    68,     6,     6,
       4,    11,    84,    96,     6,     6,     9,     1,   100,    41,
      41,    41,     7,     9,    71,    75,    16,    60,    61,     9,
       9,    87,    91,    81,    41,    41,    43,    69,    98,    98,
      40,     5,    10,    85,    97,    98,    98,    11,     8,     8,
      11,    72,    76,     6,    61,    11,    11,    88,    92,    41,
      41,     9,     9,    62,     9,     9,    73,    77,    41,    63,
      64,    89,    93,    11,    11,    33,     9,    10,    11,    11,
      41,    43,    64
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    44,    45,    45,    46,    47,    46,    49,    48,    50,
      50,    51,    52,    52,    54,    53,    55,    56,    53,    57,
      57,    58,    58,    59,    58,    60,    60,    62,    61,    63,
      63,    64,    64,    66,    67,    68,    69,    65,    70,    71,
      72,    73,    65,    74,    75,    76,    77,    65,    78,    79,
      80,    81,    65,    82,    83,    84,    85,    65,    86,    87,
      88,    89,    65,    90,    91,    92,    93,    65,    94,    95,
      96,    97,    65,    65,    98,    98,    99,    99,    99,    99,
     100,   100,   100,   100,   100,   101,   101,   101,   102,   102,
     102,   102,   102,   102,   102,   102,   102,   102,   103,   104,
     104,   104,   104,   104,   104,   105,   105,   105,   105,   105,
     105,   105
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     0,     3,     0,     0,     5,     0,     7,     0,
       2,     2,     0,     2,     0,     7,     0,     0,     8,     1,
       3,     3,     3,     0,     6,     1,     3,     0,     6,     1,
       3,     3,     3,     0,     0,     0,     0,    10,     0,     0,
       0,     0,    13,     0,     0,     0,     0,    13,     0,     0,
       0,     0,    10,     0,     0,     0,     0,    11,     0,     0,
       0,     0,    14,     0,     0,     0,     0,    14,     0,     0,
       0,     0,    11,     2,     0,     1,     1,     3,     3,     1,
       3,     6,     6,     3,     4,     1,     1,     1,     2,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep)
{
  YY_USE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 5: /* $@1: %empty  */
#line 48 "corba-gram.y"
                                          { add_header((yyvsp[0].name)); }
#line 1328 "corba-gram.c"
    break;

  case 7: /* $@2: %empty  */
#line 51 "corba-gram.y"
                                        { begin_interface((yyvsp[0].name)); }
#line 1334 "corba-gram.c"
    break;

  case 8: /* interface: TOK_INTERFACE TOK_IDENTIFIER $@2 TOK_LEFT_CUR_BRAKET prototypes TOK_RIGHT_CUR_BRAKET TOK_SEMICOLON  */
#line 53 "corba-gram.y"
                                                   { end_interface(); }
#line 1340 "corba-gram.c"
    break;

  case 14: /* $@3: %empty  */
#line 70 "corba-gram.y"
                                                                                      { add_implements((yyvsp[-1].name)); }
#line 1346 "corba-gram.c"
    break;

  case 16: /* $@4: %empty  */
#line 71 "corba-gram.y"
                                                  { begin_constraints(); }
#line 1352 "corba-gram.c"
    break;

  case 17: /* $@5: %empty  */
#line 71 "corba-gram.y"
                                                                                                                  { end_constraints(); }
#line 1358 "corba-gram.c"
    break;

  case 21: /* constraint: TOK_IDENTIFIER TOK_EQUAL TOK_IDENTIFIER  */
#line 80 "corba-gram.y"
                                              { add_constraint((yyvsp[-2].name), (yyvsp[0].name)); }
#line 1364 "corba-gram.c"
    break;

  case 22: /* constraint: TOK_IDENTIFIER TOK_EQUAL NUMBER  */
#line 81 "corba-gram.y"
                                      { add_constraint((yyvsp[-2].name), (yyvsp[0].elements)); }
#line 1370 "corba-gram.c"
    break;

  case 23: /* $@6: %empty  */
#line 82 "corba-gram.y"
                     { begin_processors(); }
#line 1376 "corba-gram.c"
    break;

  case 24: /* constraint: TOK_PROCESSORS $@6 TOK_EQUAL TOK_LEFT_CUR_BRAKET processor_list TOK_RIGHT_CUR_BRAKET  */
#line 82 "corba-gram.y"
                                                                                                               { end_processors(); }
#line 1382 "corba-gram.c"
    break;

  case 27: /* $@7: %empty  */
#line 91 "corba-gram.y"
                                                { begin_processor(); }
#line 1388 "corba-gram.c"
    break;

  case 28: /* processor: TOK_AT TOK_PROCESSOR TOK_LEFT_PARENTHESIS $@7 processor_params TOK_RIGHT_PARENTHESIS  */
#line 91 "corba-gram.y"
                                                                                                              { end_processor(); }
#line 1394 "corba-gram.c"
    break;

  case 31: /* processor_param: TOK_IDENTIFIER TOK_EQUAL TOK_IDENTIFIER  */
#line 100 "corba-gram.y"
                                              { add_processor_param((yyvsp[-2].name), (yyvsp[0].name)); }
#line 1400 "corba-gram.c"
    break;

  case 32: /* processor_param: TOK_IDENTIFIER TOK_EQUAL NUMBER  */
#line 101 "corba-gram.y"
                                      { add_processor_param((yyvsp[-2].name), (yyvsp[0].elements)); }
#line 1406 "corba-gram.c"
    break;

  case 33: /* $@8: %empty  */
#line 104 "corba-gram.y"
                                    {  begin_function((yyvsp[0].name)); add_static(0); add_return_type((yyvsp[-1].dtype), "", NULL); }
#line 1412 "corba-gram.c"
    break;

  case 34: /* $@9: %empty  */
#line 104 "corba-gram.y"
                                                                                                                                { begin_arguments(); }
#line 1418 "corba-gram.c"
    break;

  case 35: /* $@10: %empty  */
#line 104 "corba-gram.y"
                                                                                                                                                                  { end_arguments(); }
#line 1424 "corba-gram.c"
    break;

  case 36: /* $@11: %empty  */
#line 104 "corba-gram.y"
                                                                                                                                                                                                              { end_function(); }
#line 1430 "corba-gram.c"
    break;

  case 38: /* $@12: %empty  */
#line 105 "corba-gram.y"
                                                                                            {  begin_function((yyvsp[0].name)); add_static(0); add_return_type((yyvsp[-4].dtype), "", (yyvsp[-2].name)); }
#line 1436 "corba-gram.c"
    break;

  case 39: /* $@13: %empty  */
#line 105 "corba-gram.y"
                                                                                                                                                                                      { begin_arguments(); }
#line 1442 "corba-gram.c"
    break;

  case 40: /* $@14: %empty  */
#line 105 "corba-gram.y"
                                                                                                                                                                                                                        { end_arguments(); }
#line 1448 "corba-gram.c"
    break;

  case 41: /* $@15: %empty  */
#line 105 "corba-gram.y"
                                                                                                                                                                                                                                                                      { end_function(); }
#line 1454 "corba-gram.c"
    break;

  case 43: /* $@16: %empty  */
#line 106 "corba-gram.y"
                                                                                    {  begin_function((yyvsp[0].name)); add_static(0); add_return_type((yyvsp[-4].dtype), "", (yyvsp[-2].elements)); }
#line 1460 "corba-gram.c"
    break;

  case 44: /* $@17: %empty  */
#line 106 "corba-gram.y"
                                                                                                                                                                              { begin_arguments(); }
#line 1466 "corba-gram.c"
    break;

  case 45: /* $@18: %empty  */
#line 106 "corba-gram.y"
                                                                                                                                                                                                                { end_arguments(); }
#line 1472 "corba-gram.c"
    break;

  case 46: /* $@19: %empty  */
#line 106 "corba-gram.y"
                                                                                                                                                                                                                                                              { end_function(); }
#line 1478 "corba-gram.c"
    break;

  case 48: /* $@20: %empty  */
#line 107 "corba-gram.y"
                                                { begin_function((yyvsp[0].name)); add_static(0); add_return_type(object_dt, (yyvsp[-1].name), NULL); }
#line 1484 "corba-gram.c"
    break;

  case 49: /* $@21: %empty  */
#line 107 "corba-gram.y"
                                                                                                                                                  { begin_arguments(); }
#line 1490 "corba-gram.c"
    break;

  case 50: /* $@22: %empty  */
#line 107 "corba-gram.y"
                                                                                                                                                                                    { end_arguments(); }
#line 1496 "corba-gram.c"
    break;

  case 51: /* $@23: %empty  */
#line 107 "corba-gram.y"
                                                                                                                                                                                                                                      { end_function(); }
#line 1502 "corba-gram.c"
    break;

  case 53: /* $@24: %empty  */
#line 108 "corba-gram.y"
                                                           { begin_function((yyvsp[0].name)); add_static(1); add_return_type(object_dt, (yyvsp[-1].name), NULL); }
#line 1508 "corba-gram.c"
    break;

  case 54: /* $@25: %empty  */
#line 108 "corba-gram.y"
                                                                                                                                                             { begin_arguments(); }
#line 1514 "corba-gram.c"
    break;

  case 55: /* $@26: %empty  */
#line 108 "corba-gram.y"
                                                                                                                                                                                               { end_arguments(); }
#line 1520 "corba-gram.c"
    break;

  case 56: /* $@27: %empty  */
#line 108 "corba-gram.y"
                                                                                                                                                                                                                                              { end_function(); }
#line 1526 "corba-gram.c"
    break;

  case 58: /* $@28: %empty  */
#line 109 "corba-gram.y"
                                                                                                       { begin_function((yyvsp[0].name)); add_static(1); add_return_type((yyvsp[-4].dtype), "", (yyvsp[-2].name)); }
#line 1532 "corba-gram.c"
    break;

  case 59: /* $@29: %empty  */
#line 109 "corba-gram.y"
                                                                                                                                                                                                { begin_arguments(); }
#line 1538 "corba-gram.c"
    break;

  case 60: /* $@30: %empty  */
#line 109 "corba-gram.y"
                                                                                                                                                                                                                                  { end_arguments(); }
#line 1544 "corba-gram.c"
    break;

  case 61: /* $@31: %empty  */
#line 109 "corba-gram.y"
                                                                                                                                                                                                                                                                               { end_function(); }
#line 1550 "corba-gram.c"
    break;

  case 63: /* $@32: %empty  */
#line 110 "corba-gram.y"
                                                                                               { begin_function((yyvsp[0].name)); add_static(1); add_return_type((yyvsp[-4].dtype), "", (yyvsp[-2].elements)); }
#line 1556 "corba-gram.c"
    break;

  case 64: /* $@33: %empty  */
#line 110 "corba-gram.y"
                                                                                                                                                                                        { begin_arguments(); }
#line 1562 "corba-gram.c"
    break;

  case 65: /* $@34: %empty  */
#line 110 "corba-gram.y"
                                                                                                                                                                                                                          { end_arguments(); }
#line 1568 "corba-gram.c"
    break;

  case 66: /* $@35: %empty  */
#line 110 "corba-gram.y"
                                                                                                                                                                                                                                                                       { end_function(); }
#line 1574 "corba-gram.c"
    break;

  case 68: /* $@36: %empty  */
#line 111 "corba-gram.y"
                                                      { begin_function((yyvsp[0].name)); add_static(1); add_return_type((yyvsp[-1].dtype), "", NULL); }
#line 1580 "corba-gram.c"
    break;

  case 69: /* $@37: %empty  */
#line 111 "corba-gram.y"
                                                                                                                                                 { begin_arguments(); }
#line 1586 "corba-gram.c"
    break;

  case 70: /* $@38: %empty  */
#line 111 "corba-gram.y"
                                                                                                                                                                                   { end_arguments(); }
#line 1592 "corba-gram.c"
    break;

  case 71: /* $@39: %empty  */
#line 111 "corba-gram.y"
                                                                                                                                                                                                                                { end_function(); }
#line 1598 "corba-gram.c"
    break;

  case 80: /* argument: direction data_type TOK_IDENTIFIER  */
#line 127 "corba-gram.y"
                                                   { add_argument((yyvsp[-2].dir), (yyvsp[-1].dtype), "", (yyvsp[0].name), NULL); }
#line 1604 "corba-gram.c"
    break;

  case 81: /* argument: direction array_type TOK_LEFT_BRAKET TOK_IDENTIFIER TOK_RIGHT_BRAKET TOK_IDENTIFIER  */
#line 128 "corba-gram.y"
                                                                                                            { add_argument((yyvsp[-5].dir), (yyvsp[-4].dtype), "", (yyvsp[0].name), (yyvsp[-2].name));}
#line 1610 "corba-gram.c"
    break;

  case 82: /* argument: direction array_type TOK_LEFT_BRAKET NUMBER TOK_RIGHT_BRAKET TOK_IDENTIFIER  */
#line 129 "corba-gram.y"
                                                                                                    { add_argument((yyvsp[-5].dir), (yyvsp[-4].dtype), "", (yyvsp[0].name), (yyvsp[-2].elements));}
#line 1616 "corba-gram.c"
    break;

  case 83: /* argument: direction TOK_IDENTIFIER TOK_IDENTIFIER  */
#line 130 "corba-gram.y"
                                                                { add_argument((yyvsp[-2].dir), object_dt, (yyvsp[-1].name), (yyvsp[0].name), NULL); }
#line 1622 "corba-gram.c"
    break;

  case 84: /* argument: direction enum_type TOK_IDENTIFIER TOK_IDENTIFIER  */
#line 131 "corba-gram.y"
                                                              { add_argument((yyvsp[-3].dir), (yyvsp[-2].dtype), (yyvsp[-1].name), (yyvsp[0].name), NULL); }
#line 1628 "corba-gram.c"
    break;

  case 85: /* direction: TOK_IN  */
#line 134 "corba-gram.y"
                                        { (yyval.dir) = in_dir; }
#line 1634 "corba-gram.c"
    break;

  case 86: /* direction: TOK_OUT  */
#line 135 "corba-gram.y"
                                        { (yyval.dir) = out_dir; }
#line 1640 "corba-gram.c"
    break;

  case 87: /* direction: TOK_INOUT  */
#line 136 "corba-gram.y"
                                        { (yyval.dir) = inout_dir; }
#line 1646 "corba-gram.c"
    break;

  case 88: /* data_type: TOK_UNSIGNED numeric_type  */
#line 139 "corba-gram.y"
                                                { (yyval.dtype) = (yyvsp[0].dtype); }
#line 1652 "corba-gram.c"
    break;

  case 89: /* data_type: numeric_type  */
#line 140 "corba-gram.y"
                                        { (yyval.dtype) = (yyvsp[0].dtype); }
#line 1658 "corba-gram.c"
    break;

  case 90: /* data_type: TOK_CHAR  */
#line 141 "corba-gram.y"
                                                { (yyval.dtype) = char_dt; }
#line 1664 "corba-gram.c"
    break;

  case 91: /* data_type: TOK_WCHAR  */
#line 142 "corba-gram.y"
                                                { (yyval.dtype) = wchar_dt; }
#line 1670 "corba-gram.c"
    break;

  case 92: /* data_type: TOK_BOOLEAN  */
#line 143 "corba-gram.y"
                                        { (yyval.dtype) = boolean_dt; }
#line 1676 "corba-gram.c"
    break;

  case 93: /* data_type: TOK_STRING  */
#line 144 "corba-gram.y"
                                        { (yyval.dtype) = string_dt; }
#line 1682 "corba-gram.c"
    break;

  case 94: /* data_type: TOK_WSTRING  */
#line 145 "corba-gram.y"
                                        { (yyval.dtype) = wstring_dt; }
#line 1688 "corba-gram.c"
    break;

  case 95: /* data_type: TOK_ANY  */
#line 146 "corba-gram.y"
                                                { (yyval.dtype) = any_dt; }
#line 1694 "corba-gram.c"
    break;

  case 96: /* data_type: TOK_VOID  */
#line 147 "corba-gram.y"
                                                { (yyval.dtype) = void_dt; }
#line 1700 "corba-gram.c"
    break;

  case 97: /* data_type: TOK_FILE  */
#line 148 "corba-gram.y"
                                                { (yyval.dtype) = file_dt; }
#line 1706 "corba-gram.c"
    break;

  case 98: /* enum_type: TOK_ENUM  */
#line 151 "corba-gram.y"
                    { (yyval.dtype) = enum_dt; }
#line 1712 "corba-gram.c"
    break;

  case 99: /* numeric_type: TOK_SHORT  */
#line 154 "corba-gram.y"
                                { (yyval.dtype) = short_dt; }
#line 1718 "corba-gram.c"
    break;

  case 100: /* numeric_type: TOK_LONG  */
#line 155 "corba-gram.y"
                                        { (yyval.dtype) = long_dt; }
#line 1724 "corba-gram.c"
    break;

  case 101: /* numeric_type: TOK_LONGLONG  */
#line 156 "corba-gram.y"
                                { (yyval.dtype) = longlong_dt; }
#line 1730 "corba-gram.c"
    break;

  case 102: /* numeric_type: TOK_INT  */
#line 157 "corba-gram.y"
                                        { (yyval.dtype) = int_dt; }
#line 1736 "corba-gram.c"
    break;

  case 103: /* numeric_type: TOK_FLOAT  */
#line 158 "corba-gram.y"
                                        { (yyval.dtype) = float_dt; }
#line 1742 "corba-gram.c"
    break;

  case 104: /* numeric_type: TOK_DOUBLE  */
#line 159 "corba-gram.y"
                                { (yyval.dtype) = double_dt; }
#line 1748 "corba-gram.c"
    break;

  case 105: /* array_type: TOK_SHORT  */
#line 162 "corba-gram.y"
                        { (yyval.dtype) = short_dt; }
#line 1754 "corba-gram.c"
    break;

  case 106: /* array_type: TOK_LONG  */
#line 163 "corba-gram.y"
                                        { (yyval.dtype) = long_dt; }
#line 1760 "corba-gram.c"
    break;

  case 107: /* array_type: TOK_LONGLONG  */
#line 164 "corba-gram.y"
                                { (yyval.dtype) = longlong_dt; }
#line 1766 "corba-gram.c"
    break;

  case 108: /* array_type: TOK_INT  */
#line 165 "corba-gram.y"
                                        { (yyval.dtype) = int_dt; }
#line 1772 "corba-gram.c"
    break;

  case 109: /* array_type: TOK_FLOAT  */
#line 166 "corba-gram.y"
                                        { (yyval.dtype) = float_dt; }
#line 1778 "corba-gram.c"
    break;

  case 110: /* array_type: TOK_DOUBLE  */
#line 167 "corba-gram.y"
                                { (yyval.dtype) = double_dt; }
#line 1784 "corba-gram.c"
    break;

  case 111: /* array_type: TOK_CHAR  */
#line 168 "corba-gram.y"
                                { (yyval.dtype) = char_dt; }
#line 1790 "corba-gram.c"
    break;


#line 1794 "corba-gram.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 171 "corba-gram.y"


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
