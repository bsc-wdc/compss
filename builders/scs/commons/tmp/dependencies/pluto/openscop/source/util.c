
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                            util.c                               **
 **-----------------------------------------------------------------**
 **                   First version: 08/10/2010                     **
 **-----------------------------------------------------------------**


*****************************************************************************
* OpenScop: Structures and formats for polyhedral tools to talk together    *
*****************************************************************************
*    ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,                *
*    /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,             *
*   /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\             *
*  |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \            *
*  | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\           *
*  | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\          *
*  | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \         *
*  | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \        *
*  | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \       *
*  | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \      *
*  | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \     *
*  | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \    *
*  | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \   *
*  | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ /   *
*  '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'    *
*                                                                           *
* Copyright (C) 2008 University Paris-Sud 11 and INRIA                      *
*                                                                           *
* (3-clause BSD license)                                                    *
* Redistribution and use in source  and binary forms, with or without       *
* modification, are permitted provided that the following conditions        *
* are met:                                                                  *
*                                                                           *
* 1. Redistributions of source code must retain the above copyright notice, *
*    this list of conditions and the following disclaimer.                  *
* 2. Redistributions in binary form must reproduce the above copyright      *
*    notice, this list of conditions and the following disclaimer in the    *
*    documentation and/or other materials provided with the distribution.   *
* 3. The name of the author may not be used to endorse or promote products  *
*    derived from this software without specific prior written permission.  *
*                                                                           *
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      *
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES *
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   *
* IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          *
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  *
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, *
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     *
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       *
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  *
* THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         *
*                                                                           *
* OpenScop Library, a library to manipulate OpenScop formats and data       *
* structures. Written by:                                                   *
* Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                         *
* Louis-Noel Pouchet <Louis-Noel.pouchet@inria.fr>                          *
*                                                                           *
*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

#include <osl/macros.h>
#include <osl/util.h>


/*+***************************************************************************
 *                             Utility functions                             *
 *****************************************************************************/


/**
 * osl_util_skip_blank_and_comments "file skip" function:
 * this function reads the open file 'file' line by line and skips
 * blank/comment lines and spaces. The first line where there is some
 * useful information is stored at the address 'str' (the memory to
 * store the line must be allocated before the call to this function
 * and must be at least OSL_MAX_STRING * sizeof(char)). The pointer
 * to the first useful information in this line is returned by the
 * function.
 * \param[in] file The (opened) file to read.
 * \param[in] str  Address of an allocated space to store the first line
 *                 that contains useful information.
 * \return The address of the first useful digit in str.
 */
char * osl_util_skip_blank_and_comments(FILE * file, char * str) {
    char * start;

    do {
        start = fgets(str, OSL_MAX_STRING, file);
        while ((start != NULL) && isspace(*start) && (*start != '\n'))
            start++;
    } while (start != NULL && (*start == '#' || *start == '\n'));

    return start;
}


/**
 * osl_util_sskip_blank_and_comments "string skip" function:
 * this function updates the str pointer, which initialy points to a string,
 * to the first character in this string which is not a space or a comment
 * (comments start at '#' and end at '\n'), or to the end of string.
 * \param[in,out] str Address of a string, updated to the address of
 *                    the first non-space or comment character.
 */
void osl_util_sskip_blank_and_comments(char ** str) {
    do {
        // Skip spaces/blanc lines.
        while (*str && **str && isspace(**str))
            (*str)++;

        // Skip the comment if any.
        if (*str && **str && **str == '#') {
            while (**str && **str != '\n') {
                (*str)++;
            }
        }
    } while (*str && **str && **str == '\n');
}


/**
 * osl_util_read_int function:
 * reads an int on the input 'file' or the input string 'str' depending on
 * which one is not NULL (exactly one of them must not be NULL).
 * \param[in]     file The file where to read an int (if not NULL).
 * \param[in,out] str  The string where to read an int (if not NULL). This
 *                     pointer is updated to reflect the read and points
 *                     after the int in the input string.
 * \return The int that has been read.
 */
int osl_util_read_int(FILE * file, char ** str) {
    char s[OSL_MAX_STRING], * start;
    int res;
    int i = 0;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    if (file != NULL) {
        // Parse from a file.
        start = osl_util_skip_blank_and_comments(file, s);
        if (sscanf(start, " %d", &res) != 1)
            OSL_error("an int was expected");
    } else {
        // Parse from a string.
        // Skip blank/commented lines.
        osl_util_sskip_blank_and_comments(str);

        // Build the chain to analyze.
        while (**str && !isspace(**str) && **str != '\n' && **str != '#')
            s[i++] = *((*str)++);
        s[i] = '\0';
        if (sscanf(s, "%d", &res) != 1)
            OSL_error("an int was expected");
    }

    return res;
}


/**
 * osl_util_read_string function:
 * reads a string on the input 'file' or the input string 'str' depending on
 * which one is not NULL (exactly one of them must not be NULL).
 * \param[in]     file The file where to read a string (if not NULL).
 * \param[in,out] str  The string where to read a string (if not NULL). This
 *                     pointer is updated to reflect the read and points
 *                     after the string in the input string.
 * \return The string that has been read.
 */
char * osl_util_read_string(FILE * file, char ** str) {
    char s[OSL_MAX_STRING], * start;
    char * res;
    int i = 0;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    OSL_malloc(res, char *, OSL_MAX_STRING * sizeof(char));
    if (file != NULL) {
        // Parse from a file.
        start = osl_util_skip_blank_and_comments(file, s);
        if (sscanf(start, " %s", res) != 1)
            OSL_error("a string was expected");
    } else {
        // Parse from a string.
        // Skip blank/commented lines.
        osl_util_sskip_blank_and_comments(str);

        // Build the chain to analyze.
        while (**str && !isspace(**str) && **str != '\n' && **str != '#')
            s[i++] = *((*str)++);
        s[i] = '\0';
        if (sscanf(s, "%s", res) != 1)
            OSL_error("a string was expected");
    }

    OSL_realloc(res, char *, strlen(res) + 1);
    return res;
}


/**
 * osl_util_read_line function:
 * reads a line on the input 'file' or the input string 'str' depending on
 * which one is not NULL (exactly one of them must not be NULL). A line
 * is defined as the array of characters before the comment tag or the end of
 * line (it may include spaces).
 * \param[in]     file The file where to read a line (if not NULL).
 * \param[in,out] str  The string where to read a line (if not NULL). This
 *                     pointer is updated to reflect the read and points
 *                     after the line in the input string.
 * \return The line that has been read.
 */
char * osl_util_read_line(FILE * file, char ** str) {
    char s[OSL_MAX_STRING], * start;
    char * res;
    int i = 0;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    OSL_malloc(res, char *, OSL_MAX_STRING * sizeof(char));
    if (file != NULL) {
        // Parse from a file.
        start = osl_util_skip_blank_and_comments(file, s);
        while (*start && *start != '\n' && *start != '#' && i < OSL_MAX_STRING)
            res[i++] = *start++;
    } else {
        // Parse from a string.
        osl_util_sskip_blank_and_comments(str);
        while (**str && **str != '\n' && **str != '#' && i < OSL_MAX_STRING)
            res[i++] = *((*str)++);
    }

    res[i] = '\0';
    OSL_realloc(res, char *, strlen(res) + 1);
    return res;
}


/**
 * osl_util_read_int internal function:
 * reads a tag (the form of a tag with name "name" is \<name\>) on the input
 * 'file' or the input string 'str' depending on which one is not NULL (exactly
 * one of them must not be NULL). It returns the name of the tag (thus without
 * the < and > as a string. Note that in the case of an ending tag, e.g.,
 * \</foo\>, the slash is returned as a part of the name, e.g., /foo. If no
 * tag is found the function returns NULL.
 * \param[in]     file The file where to read a tag (if not NULL).
 * \param[in,out] str  The string where to read a tag (if not NULL). This
 *                     pointer is updated to reflect the read and points
 *                     after the tag in the input string.
 * \return The tag name that has been read.
 */
char * osl_util_read_tag(FILE * file, char ** str) {
    char s[OSL_MAX_STRING], * start;
    char * res;
    int i = 0;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    // Skip blank/commented lines.
    if (file != NULL) {
        start = osl_util_skip_blank_and_comments(file, s);
        str = &start;
    } else {
        osl_util_sskip_blank_and_comments(str);
    }

    // If the end of the input has been reached, return NULL.
    if (((file != NULL) && (feof(file))) ||
            ((str  != NULL) && (**str == '\0')))
        return NULL;

    // Pass the starting '<'.
    if (**str != '<')
        OSL_error("a \"<\" to start a tag was expected");
    (*str)++;

    // Read the tag.
    OSL_malloc(res, char *, (OSL_MAX_STRING + 1) * sizeof(char));
    res[OSL_MAX_STRING] = '\0';

    while (**str && **str != '>') {
        if (((**str >= 'A') && (**str <= 'Z')) ||
                ((**str >= 'a') && (**str <= 'z')) ||
                ((**str == '/') && (i == 0))       ||
                (**str == '_')) {
            res[i++] = *((*str)++);
            res[i] = '\0';
        } else {
            OSL_error("illegal character in the tag name");
        }
    }

    // Check we actually end up with a '>' and pass it.
    if (**str != '>')
        OSL_error("a \">\" to end a tag was expected");
    (*str)++;

    return res;
}


/**
 * osl_util_read_uptoflag function:
 * this function reads a string up to a given flag (the flag is read)
 * on the input 'file' or the input string 'str' depending on which one is
 * not NULL (exactly one of them must not be NULL) and returns that string
 * without the flag. It returns NULL if the flag is not found.
 * \param[in]     file The file where to read up to flag (if not NULL).
 * \param[in,out] str  The string where to read up to flag (if not NULL). This
 *                     pointer is updated to reflect the read and points
 *                     after the flag in the input string.
 * \param[in]     flag The flag which, when reached, stops the reading.
 * \return The string that has been read.
 */
char * osl_util_read_uptoflag(FILE * file, char ** str, char * flag) {
    size_t high_water_mark = OSL_MAX_STRING;
    size_t nb_chars = 0;
    size_t lenflag = strlen(flag), lenstr;
    int flag_found = 0;
    char * res;

    if ((file != NULL && str != NULL) || (file == NULL && str == NULL))
        OSL_error("one and only one of the two parameters can be non-NULL");

    OSL_malloc(res, char *, high_water_mark * sizeof(char));

    // Copy everything to the res string.
    lenstr = str != NULL ? strlen(*str) : 0;
    while (((str  != NULL) && (nb_chars != lenstr)) ||
            ((file != NULL) && (!feof(file)))) {
        res[nb_chars++] = (str != NULL) ? *((*str)++) : (char)fgetc(file);

        if ((nb_chars >= lenflag) &&
                (!strncmp(&res[nb_chars - lenflag], flag, lenflag))) {
            flag_found = 1;
            break;
        }

        if (nb_chars >= high_water_mark) {
            high_water_mark += high_water_mark;
            OSL_realloc(res, char *, high_water_mark * sizeof(char));
        }
    }

    if (!flag_found) {
        OSL_debug("flag was not found, end of input reached");
        free(res);
        return NULL;
    }

    // - 0-terminate the string.
    OSL_realloc(res, char *, (nb_chars - strlen(flag) + 1) * sizeof(char));
    res[nb_chars - strlen(flag)] = '\0';

    return res;
}


/**
 * osl_util_read_uptotag function:
 * this function reads a string up to a given (start) tag (the tag is read)
 * on the input 'file' or the input string 'str' depending on which one is
 * not NULL (exactly one of them must not be NULL) and returns that string
 * without the tag. It returns NULL if the tag is not found.
 * \param[in]     file The file where to read up to tag (if not NULL).
 * \param[in,out] str  The string where to read up to tag (if not NULL).
 *                     This pointer is updated to reflect the read and points
 *                     after the tag in the input string.
 * \param[in]     name The name of the tag to the file reading.
 * \return The string that has been read from the file.
 */
char * osl_util_read_uptotag(FILE * file, char ** str, char * name) {
    char tag[strlen(name) + 3];

    sprintf(tag, "<%s>", name);
    return osl_util_read_uptoflag(file, str, tag);
}


/**
 * osl_util_read_uptoendtag function:
 * this function reads a string up to a given end tag (the end tag is read)
 * on the input 'file' or the input string 'str' depending on which one is
 * not NULL (exactly one of them must not be NULL) and returns that string
 * without the end tag. It returns NULL if the end tag is not found.
 * \param[in]     file The file where to read up to end tag (if not NULL).
 * \param[in,out] str  The string where to read up to end tag (if not NULL).
 *                     This pointer is updated to reflect the read and points
 *                     after the end tag in the input string.
 * \param[in]     name The name of the end tag to the file reading.
 * \return The string that has been read from the file.
 */
char * osl_util_read_uptoendtag(FILE * file, char ** str, char * name) {
    char endtag[strlen(name) + 4];

    sprintf(endtag, "</%s>", name);
    return osl_util_read_uptoflag(file, str, endtag);
}


/**
 * osl_util_tag_content function:
 * this function returns a freshly allocated string containing the
 * content, in the given string 'str', between the tag '\<name\>' and
 * the tag '\</name\>'. If the tag '\<name\>' is not found, it returns NULL.
 * \param[in] str    The string where to find a given content.
 * \param[in] name   The name of the tag we are looking for.
 * \return The string between '\<name\>' and '\</name\>' in 'str'.
 */
char * osl_util_tag_content(char * str, char * name) {
    int i;
    char * start;
    char * stop;
    char tag[strlen(name) + 3];
    char endtag[strlen(name) + 4];
    size_t size = 0;
    size_t lentag;
    char * res = NULL;

    sprintf(tag, "<%s>", name);
    sprintf(endtag, "</%s>", name);

    if (str) {
        start = str;
        lentag = strlen(tag);
        for (; start && *start && strncmp(start, tag, lentag); ++start)
            continue;

        // The tag 'tag' was not found.
        if (! *start)
            return NULL;
        start += lentag;
        stop = start;
        lentag = strlen(endtag);
        for (size = 0; *stop && strncmp(stop, endtag, lentag); ++stop, ++size)
            continue;

        // the tag 'endtag' was not found.
        if (! *stop)
            return NULL;
        OSL_malloc(res, char *, (size + 1) * sizeof(char));

        // Copy the chain between the two tags.
        for (++start, i = 0; start != stop; ++start, ++i)
            res[i] = *start;
        res[i] = '\0';
    }

    return res;
}


/**
 * osl_util_safe_strcat function:
 * this function concatenates the string src to the string *dst
 * and reallocates *dst if necessary. The current size of the
 * *dst buffer must be *hwm (high water mark), if there is some
 * reallocation, this value is updated.
 * \param[in,out] dst pointer to the destination string (may be reallocated).
 * \param[in]     src string to concatenate to dst.
 * \param[in,out] hwm pointer to the size of the *dst buffer (may be updated).
 */
void osl_util_safe_strcat(char ** dst, char * src, size_t * hwm) {

    while ((strlen(*dst) + strlen(src)) >= *hwm) {
        *hwm += OSL_MAX_STRING;
        OSL_realloc(*dst, char *, *hwm * sizeof(char));
    }

    strcat(*dst, src);
}


/**
 * \brief String duplicate
 *
 * osl_util_strdup function:
 * this function return a copy of the string str.
 *
 * \param[in] str string to be copied.
 *
 * \return a copy of the string
 */
char * osl_util_strdup(char const * str) {
    char * dup = NULL;
    OSL_malloc(dup, char *, (strlen(str) + 1) * sizeof(char));
    if (dup) {
        strcpy(dup, str);
    }
    return dup;
}


/**
 * osl_util_get_precision function:
 * this function returns the precision defined by the precision environment
 * variable or the highest available precision if it is not defined.
 * \return environment precision if defined or highest available precision.
 */
int osl_util_get_precision(void) {
    int precision = OSL_PRECISION_DP;
    char * precision_env;

#ifdef OSL_GMP_IS_HERE
    precision = OSL_PRECISION_MP;
#endif

    precision_env = getenv(OSL_PRECISION_ENV);
    if (precision_env != NULL) {
        if (!strcmp(precision_env, OSL_PRECISION_ENV_SP))
            precision = OSL_PRECISION_SP;
        else if (!strcmp(precision_env, OSL_PRECISION_ENV_DP))
            precision = OSL_PRECISION_DP;
        else if (!strcmp(precision_env, OSL_PRECISION_ENV_MP)) {
#ifndef OSL_GMP_IS_HERE
            OSL_warning("$OSL_PRECISION says GMP but osl not compiled with "
                        "GMP support, switching to double precision");
            precision = OSL_PRECISION_DP;
#else
            precision = OSL_PRECISION_MP;
#endif
        } else
            OSL_warning("bad OSL_PRECISION environment value, see osl's manual");
    }

    return precision;
}


/**
 * osl_util_print_provided function:
 * this function prints a "provided" boolean in a file (file, possibly stdout),
 * with a comment title according to the OpenScop specification.
 * \param[in] file     File where the information has to be printed.
 * \param[in] provided The provided boolean to print.
 * \param[in] title    A string to use as a title for the provided booblean.
 */
void osl_util_print_provided(FILE * file, int provided, char * title) {
    if (provided) {
        fprintf(file, "# %s provided\n", title);
        fprintf(file, "1\n");
    } else {
        fprintf(file, "# %s not provided\n", title);
        fprintf(file, "0\n\n");
    }
}


/**
 * osl_util_identifier_is_here function:
 * this function returns 1 if the input "identifier" is found at the
 * "index" position in the "expression" input string, 0 otherwise.
 * \param[in] expression The input expression.
 * \param[in] identifier The identifier to look for.
 * \param[in] index      The position in the expression where to look.
 * \return 1 if the identifier is found at the position in the expression.
 */
static
int osl_util_identifier_is_here(char * expression, char * identifier,
                                size_t index) {
    size_t identifier_len = strlen(identifier);
    size_t expression_len = strlen(expression);

    // If there is no space enough to find the identifier: no.
    if (identifier_len + index > expression_len)
        return 0;

    // If there is a character before and it is in [A-Za-z0-9_]: no.
    if ((index > 0) &&
            (((expression[index - 1] >= 'A') && (expression[index - 1] <= 'Z')) ||
             ((expression[index - 1] >= 'a') && (expression[index - 1] <= 'z')) ||
             ((expression[index - 1] >= '0') && (expression[index - 1] <= '9')) ||
             (expression[index - 1] == '_')))
        return 0;

    // If there is a character after and it is in [A-Za-z0-9_]: no.
    if ((identifier_len + index < expression_len) &&
            (((expression[identifier_len + index] >= 'A') &&
              (expression[identifier_len + index] <= 'Z'))   ||
             ((expression[identifier_len + index] >= 'a') &&
              (expression[identifier_len + index] <= 'z'))   ||
             ((expression[identifier_len + index] >= '0') &&
              (expression[identifier_len + index] <= '9'))   ||
             ( expression[identifier_len + index] == '_')))
        return 0;

    // If the identifier string is not here: no.
    if (strncmp(expression + index, identifier, identifier_len))
        return 0;

    return 1;
}


/**
 * osl_util_lazy_isolated_identifier function:
 * this function returns 1 if the identifier at the "index" position in the
 * "expression" is guaranteed not to need parenthesis around is we
 * substitute it with anything. For instance the identifier "i" can be
 * always substituted in "A[i]" with no need of parenthesis but not in
 * "A[2*i]". This function is lazy in the sense that it just check obvious
 * cases, not all of them. The identifier must already be at the indicated
 * position, this function does not check that.
 * \param[in] expression The input expression.
 * \param[in] identifier The identifier to check.
 * \param[in] index      The position of the identifier in the expression.
 * \return 1 if the identifier is isolated, 0 if unsure.
 */
static
int osl_util_lazy_isolated_identifier(char * expression, char * identifier,
                                      size_t index) {
    size_t look;
    size_t expression_len = strlen(expression);
    size_t identifier_len = strlen(identifier);

    // If the first non-space character before is not in [\[(,\+=]: no.
    look = index - 1;
    while (look < index) {
        if (isspace(expression[look]))
            look--;
        else
            break;
    }

    if ((look < index) &&
            (expression[look] != '[') &&
            (expression[look] != '(') &&
            (expression[look] != '+') &&
            (expression[look] != '=') &&
            (expression[look] != ','))
        return 0;

    // If the first non-space character after is not in [\]),;\+]: no.
    look = index + identifier_len;
    while (look < expression_len) {
        if (isspace(expression[look]))
            look++;
        else
            break;
    }

    if ((look < expression_len) &&
            (expression[look] != ']')   &&
            (expression[look] != ')')   &&
            (expression[look] != '+')   &&
            (expression[look] != ',')   &&
            (expression[look] != ';'))
        return 0;

    return 1;
}


/**
 * osl_util_identifier_substitution function:
 * this function replaces some identifiers in an input expression string and
 * returns the final string. The list of identifiers to replace are provided
 * as an array of strings. They are replaced from the input string with the
 * new substring "@i@" or "(@i@)" where i is the rank of the identifier in the
 * array of identifiers. The parentheses are added when it is not obvious that
 * the identifier can be replaced with an arbitrary expression without the
 * need of parentheses. For instance, let us consider the input expression
 * "C[i+j]+=A[2*i]*B[j];" and the array of strings {"i", "j"}: the resulting
 * string would be "C[@0@+@1@]+=A[2*(@0@)]*B[@1@];".
 * \param[in] expression The original expression.
 * \param[in] identifiers NULL-terminated array of identifiers.
 * \return A new string where the ith identifier is replaced by \@i\@.
 */
char * osl_util_identifier_substitution(char * expression,
                                        char ** identifiers) {
    size_t index;
    int j, found;
    size_t high_water_mark = OSL_MAX_STRING;
    char buffer[OSL_MAX_STRING];
    char * string;

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    index = 0;
    while (index < strlen(expression)) {
        j = 0;
        found = 0;
        while (identifiers[j] != NULL) {
            if (osl_util_identifier_is_here(expression, identifiers[j], index)) {
                if (osl_util_lazy_isolated_identifier(expression,identifiers[j],index))
                    sprintf(buffer, "@%d@", j);
                else
                    sprintf(buffer, "(@%d@)", j);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                index += strlen(identifiers[j]);
                found = 1;
                break;
            }
            j++;
        }
        if (!found) {
            sprintf(buffer, "%c", expression[index]);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            index++;
        }
    }

    return string;
}



