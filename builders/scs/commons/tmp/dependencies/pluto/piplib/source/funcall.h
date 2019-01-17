/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                 funcall.h                                  *
 ******************************************************************************
 *                                                                            *
 * Copyright Paul Feautrier, 1988-2005                                        *
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
 *****************************************************************************/

#ifndef FUNCALL_H
#define FUNCALL_H
#if defined(__cplusplus)
extern "C"
{
#endif

#define TRAITER_INT  (1 << 0) /* Compute integer optimum */
#define TRAITER_DUAL (1 << 1) /* Compute dual variables */

#define traiter_xx PIPLIB_NAME(traiter)
void traiter_xx(
    Tableau_xx *tp, Tableau_xx *ctxt,
    int nvar, int nparm, int ni, int nc, int bigparm, int flags);

#define integrer_xx PIPLIB_NAME(integrer)
int integrer_xx(
    Tableau_xx **, Tableau_xx **,
    int *, int *, int *, int *, int);

#define dgetc_xx PIPLIB_NAME(dgetc)
#define pip_create_dump_file_xx PIPLIB_NAME(pip_create_dump_file)
#define sol_hwm_xx PIPLIB_NAME(sol_hwm)
#define sol_simplify_xx PIPLIB_NAME(sol_simplify)
#define is_not_Nil_xx PIPLIB_NAME(is_not_Nil)
#define sol_edit_xx PIPLIB_NAME(sol_edit)
#define tab_reset_xx PIPLIB_NAME(tab_reset)
#define sol_reset_xx PIPLIB_NAME(sol_reset)
#define tab_hwm_xx PIPLIB_NAME(tab_hwm)
#define tab_get_xx PIPLIB_NAME(tab_get)
#define tab_simplify_xx PIPLIB_NAME(tab_simplify)
#define sol_init_xx PIPLIB_NAME(sol_init)
#define sol_close_xx PIPLIB_NAME(sol_close)
#define tab_init_xx PIPLIB_NAME(tab_init)
#define tab_close_xx PIPLIB_NAME(tab_close)
#define sol_if_xx PIPLIB_NAME(sol_if)
#define sol_forme_xx PIPLIB_NAME(sol_forme)
#define sol_val_xx PIPLIB_NAME(sol_val)
#define sol_val_one_xx PIPLIB_NAME(sol_val_one)
#define sol_val_zero_one_xx PIPLIB_NAME(sol_val_zero_one)
#define sol_nil_xx PIPLIB_NAME(sol_nil)
#define sol_error_xx PIPLIB_NAME(sol_error)
#define tab_alloc_xx PIPLIB_NAME(tab_alloc)
#define sol_list_xx PIPLIB_NAME(sol_list)
#define tab_display_xx PIPLIB_NAME(tab_display)
#define expanser_xx PIPLIB_NAME(expanser)
#define sol_new_xx PIPLIB_NAME(sol_new)
#define sol_div_xx PIPLIB_NAME(sol_div)
int dgetc_xx(FILE *foo);
FILE *pip_create_dump_file_xx();
int sol_hwm_xx(void);
void sol_simplify_xx(int);
int is_not_Nil_xx(int);
int sol_edit_xx(FILE *, int);
void tab_reset_xx(struct high_water_mark_xx);
void sol_reset_xx(int);
struct high_water_mark_xx tab_hwm_xx(void);
Tableau_xx *tab_get_xx(FILE *, int,int,int);
int tab_simplify_xx(Tableau_xx *tp, int cst);
void sol_init_xx(void);
void sol_close_xx(void);
void tab_init_xx(void);
void tab_close_xx(void);
void sol_if_xx(void);
void sol_forme_xx(int);
void sol_val_xx(piplib_int_t_xx, piplib_int_t_xx);
void sol_val_one_xx(piplib_int_t_xx);
void sol_val_zero_one_xx();
void sol_nil_xx(void);
void sol_error_xx(int);
Tableau_xx * tab_alloc_xx(int, int, int);
void sol_list_xx(int);
void tab_display_xx(Tableau_xx *, FILE *);
Tableau_xx * expanser_xx(Tableau_xx *,
                         int, int, int, int, int, int);
void sol_new_xx(int);
void sol_div_xx(void);

#if defined(__cplusplus)
}
#endif
#endif /* define _H */
