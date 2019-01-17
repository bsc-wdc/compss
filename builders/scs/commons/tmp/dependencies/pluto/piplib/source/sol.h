/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                  sol.h                                     *
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

#ifndef SOL_H
#define SOL_H
#if defined(__cplusplus)
extern "C"
{
#endif


/**< Shift solution over -bigparam */
#define SOL_SHIFT  (1 << 0)

/**< Negate solution */
#define SOL_NEGATE (1 << 1)

/**< Remove big parameter */
#define SOL_REMOVE (1 << 2)

/**< Maximum was computed */
#define SOL_MAX    (SOL_SHIFT | SOL_NEGATE)

/**< Create dual leaf */
#define SOL_DUAL   (1 << 3)


void sol_init_xx(void);
int sol_hwm_xx(void);
void sol_reset_xx(int);
void sol_nil_xx(void);
void sol_if_xx(void);
void sol_list_xx(int);
void sol_new_xx(int);
void sol_div_xx(void);
void sol_val_xx(piplib_int_t_xx, piplib_int_t_xx);
int sol_edit_xx(FILE *, int);
int is_not_Nil_xx(int);
void sol_simplify_xx(int);

#if defined(__cplusplus)
}
#endif
#endif /* define _H */
