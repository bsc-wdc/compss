/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                   tab.h                                    *
 ******************************************************************************
 *                                                                            *
 * Copyright Paul Feautrier, 1988, 1993, 1994, 1996, 2002                     *
 *                                                                            *
 * This library is free software; you can redistribute it and/or modify it    *
 * under the terms of the GNU Lesser General Public License as published by   *
 * the Free Software Foundation; either version 2.1 of the License, or (at    *
 * your option) any later version.                                            *
 *                                                                            *
 * This software is distributed in the hope that it will be useful, but       *
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
 * for more details.							      *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with this library; if not, write to the Free Software Foundation,    *
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA         *
 *                                                                            *
 * Written by Paul Feautrier                                                  *
 *                                                                            *
 ******************************************************************************/

#ifndef TAB_H
#define TAB_H
#if defined(__cplusplus)
extern "C"
{
#endif

#define A_xx PIPLIB_NAME(A)
struct A_xx {
    struct A_xx *precedent;
    char *bout;
    char *free;
};

#define L_xx PIPLIB_NAME(L)
struct L_xx {
    int flags;
    piplib_int_t_xx d;
    float size;
    union {
        int unit;
        piplib_int_t_xx * val;
    } objet;
};


#define high_water_mark_xx PIPLIB_NAME(high_water_mark)
struct high_water_mark_xx {
    int chunk;
    void * top;
};

#define Unit 1
#define Plus 2
#define Minus 4
#define Zero 8
#define Critic 16
#define Unknown 32

#define Sign 62

#define Index(p,i,j) (p)->row[i].objet.val[j]
#define Flag(p,i)    (p)->row[i].flags
#define Denom(p,i)   (p)->row[i].d
#define MAX_DETERMINANT 4

#define T_xx PIPLIB_NAME(T)
#if defined(PIPLIB_ONE_DETERMINANT)
struct T_xx {
    int height, width, taille;
    piplib_int_t_xx determinant;
    struct L_xx row[1];
};
#else
struct T_xx {
    int height, width;
    piplib_int_t_xx determinant[MAX_DETERMINANT];
    int l_determinant;
    struct L_xx row[1];
};
#endif

#define Tableau_xx PIPLIB_NAME(Tableau)
typedef struct T_xx Tableau_xx;

/* Ced : ajouts specifiques a la PipLib pour funcall. */
#define tab_Matrix2Tableau_xx PIPLIB_NAME(tab_Matrix2Tableau)
Tableau_xx * tab_Matrix2Tableau_xx(
    PipMatrix_xx *, int, int, int, int, int, int);

#if defined(__cplusplus)
}
#endif
#endif /* define _H */

