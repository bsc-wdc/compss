
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 relation.h                            **
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
* for more details.							      *
*                                                                            *
* You should have received a copy of the GNU Lesser General Public License   *
* along with software; if not, write to the Free Software Foundation, Inc.,  *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* Clan, the Chunky Loop Analyzer                                             *
* Written by Cedric Bastoul, Cedric.Bastoul@u-psud.fr                        *
*                                                                            *
******************************************************************************/


#ifndef CLAN_relation_H
# define CLAN_relation_H

# include <clan/options.h>

# if defined(__cplusplus)
extern "C"
{
# endif


struct osl_relation;
struct osl_vector;


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/
void                 clan_relation_tag_array(struct osl_relation*, int);
struct osl_relation* clan_relation_build_context(int, clan_options_p);
struct osl_relation* clan_relation_scattering(int*, int, int);
void                 clan_relation_new_output_vector(struct osl_relation*,
        struct osl_vector*);
void                 clan_relation_new_output_scalar(struct osl_relation*, int);
void                 clan_relation_compact(struct osl_relation*, int);
struct osl_relation* clan_relation_greater(struct osl_relation*,
        struct osl_relation*, int);
struct osl_relation* clan_relation_not(struct osl_relation*);
void                 clan_relation_and(struct osl_relation*,
                                       struct osl_relation*);
int                  clan_relation_existential(struct osl_relation*);
void                 clan_relation_oppose_row(struct osl_relation*, int);
struct osl_relation* clan_relation_stride(struct osl_relation*, int, int);
void                 clan_relation_simplify(struct osl_relation*);
void                 clan_relation_loop_context(struct osl_relation*,
        struct osl_relation*, int);

# if defined(__cplusplus)
}
# endif
#endif /* define CLAN_relation_H */
