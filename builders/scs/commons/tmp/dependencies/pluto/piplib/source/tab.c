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
 * for more details.                                                          *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with this library; if not, write to the Free Software Foundation,    *
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA         *
 *                                                                            *
 * Written by Paul Feautrier and Cedric Bastoul                               *
 *                                                                            *
 *****************************************************************************/


#include <stdio.h>
#include <stdlib.h>

#include "pip.h"

#define TAB_CHUNK 4096*sizeof(piplib_int_t_xx)

#define tab_free_xx PIPLIB_NAME(tab_free)
#define tab_top_xx PIPLIB_NAME(tab_top)
#define tab_base_xx PIPLIB_NAME(tab_base)
static char *tab_free_xx, *tab_top_xx;
static struct A_xx *tab_base_xx;

/*extern long int cross_product;*/
#define chunk_count_xx PIPLIB_NAME(chunk_count)
static int chunk_count_xx;

int dgetc_xx(FILE *);

extern FILE * dump_xx;

#define sizeof_struct_A ((sizeof(struct A_xx) % sizeof(piplib_int_t_xx)) ?		    \
			 (sizeof(struct A_xx) + sizeof(piplib_int_t_xx)		    \
				- (sizeof(struct A_xx) % sizeof(piplib_int_t_xx))) :    \
			  sizeof(struct A_xx))

void tab_init_xx(void) {
    tab_free_xx = malloc(sizeof_struct_A);
    if(tab_free_xx == NULL) {
        fprintf(stderr, "Your computer doesn't have enough memory\n");
        exit(1);
    }
    tab_top_xx = tab_free_xx + sizeof_struct_A;
    tab_base_xx = (struct A_xx *)tab_free_xx;
    tab_free_xx += sizeof_struct_A;
    tab_base_xx->precedent = NULL;
    tab_base_xx->bout = tab_top_xx;
    tab_base_xx->free = tab_free_xx;
    chunk_count_xx = 1;
}


void tab_close_xx(void) {
    if (tab_base_xx) free(tab_base_xx);
}


struct high_water_mark_xx tab_hwm_xx(void) {
    struct high_water_mark_xx p;
    p.chunk = chunk_count_xx;
    p.top = tab_free_xx;
    return p;
}


#if defined(PIPLIB_ONE_DETERMINANT)
/* the clear_tab routine clears the GMP objects which may be referenced
   in the given Tableau_xx.
*/
#define tab_clear_xx PIPLIB_NAME(tab_clear)
void tab_clear_xx(Tableau_xx *tp) {
    int i, j;
    /* clear the determinant */
    piplib_int_clear(tp->determinant);

    for(i=0; i<tp->height; i++) {
        /* clear the denominator */
        piplib_int_clear(Denom(tp, i));
        if((Flag(tp, i) & Unit) == 0)
            for(j=0; j<tp->width; j++)
                piplib_int_clear(Index(tp,i,j));
    }
}
#endif

void tab_reset_xx(struct high_water_mark_xx by_the_mark) {
    struct A_xx *g;
#if defined(PIPLIB_ONE_DETERMINANT)
    char *p;
#endif
    while(chunk_count_xx > by_the_mark.chunk) {
        g = tab_base_xx->precedent;

#if defined(PIPLIB_ONE_DETERMINANT)
        /* Before actually freeing the memory, one has to clear the
         * included Tableau_xxx. If this is not done, the GMP objects
         * referenced in the Tableau_xxx will be orphaned.
         */

        /* Enumerate the included tableaux. */
        p = (char *)tab_base_xx + sizeof_struct_A;
        while(p < tab_base_xx->free) {
            Tableau_xx *pt;
            pt = (Tableau_xx *) p;
            tab_clear_xx(pt);
            p += pt->taille;
        }
#endif

        free(tab_base_xx);
        tab_base_xx = g;
        tab_top_xx = tab_base_xx->bout;
        chunk_count_xx--;
    }
    if(chunk_count_xx > 0) {
#if defined(PIPLIB_ONE_DETERMINANT)
        /* Do not forget to clear the tables in the current chunk above the
           high water mark */
        p = (char *)by_the_mark.top;
        while(p < tab_base_xx->free) {
            Tableau_xx *pt;
            pt = (Tableau_xx *) p;
            tab_clear_xx(pt);
            p += pt->taille;
        }
#endif
        tab_free_xx = by_the_mark.top;
        tab_base_xx->free = tab_free_xx;
    } else {
        fprintf(stderr,
                "Syserr: tab_reset_xx : error in memory allocation\n");
        exit(1);
    }
}

Tableau_xx * tab_alloc_xx(int h, int w, int n)

/* h : le nombre de ligne reelles;
   n : le nombre de lignes virtuelles
*/
{
    char *p;
    Tableau_xx *tp;
    piplib_int_t_xx *q;
    unsigned long taille;
    int i, j;
    taille = sizeof(Tableau_xx)
             + (h+n-1) * sizeof (struct L_xx)
             + h * w * sizeof (piplib_int_t_xx);
    if(tab_free_xx + taille >= tab_top_xx) {
        struct A_xx * g;
        unsigned long d;
        d = taille + sizeof_struct_A;
        if(d < TAB_CHUNK) d = TAB_CHUNK;
        tab_free_xx = malloc(d);
        if(tab_free_xx == NULL) {
            printf("Memory overflow\n");
            exit(23);
        }
        chunk_count_xx++;
        g = (struct A_xx *)tab_free_xx;
        g->precedent = tab_base_xx;
        tab_top_xx = tab_free_xx + d;
        tab_free_xx += sizeof_struct_A;
        tab_base_xx = g;
        g->bout = tab_top_xx;
    }
    p = tab_free_xx;
    tab_free_xx += taille;
    tab_base_xx->free = tab_free_xx;
    tp = (Tableau_xx *)p;
    q = (piplib_int_t_xx *)(p +  sizeof(Tableau_xx)
                            + (h+n-1) * sizeof (struct L_xx));
#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_init_set_si(tp->determinant,1);
#else
    tp->determinant[0] = (piplib_int_t_xx) 1;
    tp->l_determinant = 1;
#endif
    for(i = 0; i<n; i++) {
        tp->row[i].flags = Unit;
        tp->row[i].objet.unit = i;
        piplib_int_init_set_si(Denom(tp, i), 1);
    }
    for(i = n; i < (h+n); i++) {
        tp->row[i].flags = 0;
        tp->row[i].objet.val = q;
        tp->row[i].size = 0;
        for(j = 0; j < w; j++)
            piplib_int_init_set_si(*q++, 0); /* loop body. */
        piplib_int_init_set_si(Denom(tp, i), 0);
    }
    tp->height = h + n;
    tp->width = w;
#if defined(PIPLIB_ONE_DETERMINANT)
    tp->taille = taille ;
#endif

    return(tp);
}

Tableau_xx * tab_get_xx(foo, h, w, n)
FILE * foo;
int h, w, n;
{
    Tableau_xx *p;
    int i, j, c;
    piplib_int_t_xx x;
    piplib_int_init(x);

    p = tab_alloc_xx(h, w, n);
    while((c = dgetc_xx(foo)) != EOF)
        if(c == '(')break;
    for(i = n; i<h+n; i++) {
        p->row[i].flags = Unknown;
        piplib_int_set_si(Denom(p, i), 1);
        while((c = dgetc_xx(foo)) != EOF)if(c == '[')break;
        for(j = 0; j<w; j++) {
            if(dscanf_xx(foo, &x) < 0) return NULL;
            else piplib_int_assign(p->row[i].objet.val[j], x);
        }
    }
    while((c = dgetc_xx(foo)) != EOF)if(c == ']')break;

    piplib_int_clear(x);

    return(p);
}


/* Fonction tab_Matrix2Tableau :
 * Cette fonction effectue la conversion du format de matrice de la polylib
 * vers le format de traitement de Pip. matrix est la matrice a convertir.
 * Nineq est le nombre d'inequations necessaires (dans le format de la
 * polylib, le premier element d'une ligne indique si l'equation decrite
 * est une inequation ou une egalite. Pip ne gere que les inequations. On
 * compte donc le nombre d'inequations total pour reserver la place
 * necessaire, et on scinde toute egalite p(x)=0 en p(x)>=0 et -p(x)>=0).
 * Nv est le nombre de variables dans la premiere serie de variables (c'est
 * a dire que si les premiers coefficients dans les lignes de la matrice
 * sont ceux des inconnues, Nv est le nombre d'inconnues, resp. parametres).
 * n est le nombre de lignes 'virtuelles' contenues dans la matrice (c'est
 * a dire en fait le nombre d'inconnues). Si Shift vaut 0, on va rechercher
 * le minimum lexicographique non-negatif, sinon on recherche le maximum
 * (Shift = 1) ou bien le minimum tout court (Shift = -1). La fonction
 * met alors en place le bignum s'il n'y est pas deja et prepare les
 * contraintes au calcul du maximum lexicographique.
 *
 * This function is called both for both the context (only parameters)
 * and the actual domain (variables + parameters).
 * Let Np be the number of parameters and Nn the number of variables.
 *
 * For the context, the columns in matrix are
 *		1 Np 1
 * while the result has
 *		Np Bg Urs_parms 1
 * Nv = Np + Bg; n = -1
 *
 * For the domain, matrix has
 *		1 Nn Np 1
 * while the result has
 *		Nn 1 Np Bg Urs_parms
 * Nv = Nn; n >= 0
 *
 * 27 juillet 2001 : Premiere version, Ced.
 * 30 juillet 2001 : Nombreuses modifications. Le calcul du nombre total
 *                   d'inequations (Nineq) se fait a present a l'exterieur.
 *  3 octobre 2001 : Pas mal d'ameliorations.
 * 18 octobre 2003 : Mise en place de la possibilite de calculer le
 *                   maximum lexicographique (parties 'if (Max)').
 */
Tableau_xx * tab_Matrix2Tableau_xx(matrix, Nineq, Nv, n,
                                   Shift, Bg, Urs_parms)
PipMatrix_xx * matrix ;
int Nineq, Nv, n, Shift, Bg, Urs_parms;
{
    Tableau_xx * p ;
    unsigned i, k, current, new, nb_columns, decal=0, bignum_is_new;
    int j;
    unsigned cst;
    int inequality, ctx;
    piplib_int_t_xx bignum;

    /* Are we dealing with the context? */
    ctx = n == -1;
    if (ctx)
        n = 0;
    piplib_int_init(bignum);
    nb_columns = matrix->NbColumns - 1 ;

    /* S'il faut un BigNum et qu'il n'existe pas, on lui reserve sa place. */
    bignum_is_new = Shift
                    && (Bg+ctx > 0)
                    && ((unsigned int)(Bg+ctx) > (matrix->NbColumns - 2));

    if (bignum_is_new)
        nb_columns++;
    if (ctx) {
        Shift = 0;
        cst = Nv + Urs_parms;
    } else
        cst = Nv;

    p = tab_alloc_xx(Nineq,nb_columns+Urs_parms,n) ;

    /* La variable decal sert a prendre en compte les lignes supplementaires
     * issues des egalites.
     */
    for (i = 0; i < matrix->NbRows; i++) {
        current = i + n + decal;
        Flag(p,current) = Unknown ;
        piplib_int_set_si(Denom(p,current), 1);
        if (Shift)
            piplib_int_set_si(bignum, 0);
        /* Pour passer l'indicateur d'egalite/inegalite. */
        inequality = (piplib_int_zero(matrix->p[i][0]) == 0);

        /* Dans le format de la polylib, l'element constant est place en
         * dernier. Dans le format de Pip, il se trouve apres la premiere
         * serie de variables (inconnues ou parametres). On remet donc les
         * choses dans l'ordre de Pip. Ici pour p(x) >= 0.
         */
        for (j=0; j<Nv; j++) {
            if (bignum_is_new && j == Bg)
                continue;
            if (Shift)
                piplib_int_add(bignum, bignum, matrix->p[i][1+j]);
            if (Shift > 0)
                piplib_int_oppose(p->row[current].objet.val[j], matrix->p[i][1+j]);
            else
                piplib_int_assign(p->row[current].objet.val[j], matrix->p[i][1+j]);
        }
        for (k=j=Nv+1; (unsigned int)j<nb_columns; j++) {
            if (bignum_is_new && j == Bg)
                continue;
            piplib_int_assign(p->row[current].objet.val[j], matrix->p[i][k]);
            k++;
        }
        for (j=0; j < Urs_parms; ++j) {
            int pos_n = nb_columns - ctx + j;
            int pos = pos_n - Urs_parms;
            if (pos <= Bg)
                --pos;
            piplib_int_oppose(p->row[current].objet.val[pos_n],
                              p->row[current].objet.val[pos]);
        }
        piplib_int_assign(p->row[current].objet.val[cst],
                          matrix->p[i][matrix->NbColumns-1]);
        if (Shift) {
            if (Shift < 0)
                piplib_int_oppose(bignum, bignum);

            if (bignum_is_new)
                piplib_int_assign(p->row[current].objet.val[Bg], bignum);
            else
                piplib_int_add(p->row[current].objet.val[Bg],
                               p->row[current].objet.val[Bg], bignum);
        }

        /* Et ici lors de l'ajout de -p(x) >= 0 quand on traite une egalite. */
        if (!inequality) {
            decal ++ ;
            new = current + 1 ;
            Flag(p,new)= Unknown ;
            piplib_int_set_si(Denom(p,new), 1);

            for (j=0; (unsigned int)j<nb_columns+Urs_parms; j++)
                piplib_int_oppose(p->row[new].objet.val[j], p->row[current].objet.val[j]);
        }
    }
    piplib_int_clear(bignum);

    return(p);
}


int tab_simplify_xx(Tableau_xx *tp, int cst) {
    int i, j;
    piplib_int_t_xx gcd;

    piplib_int_init(gcd);
    for (i = 0; i < tp->height; ++i) {
        if (Flag(tp, i) & Unit)
            continue;
        piplib_int_set_si(gcd, 0);
        for (j = 0; j < tp->width; ++j) {
            if (j == cst)
                continue;
            piplib_int_gcd(gcd, gcd, Index(tp, i, j));
            if (piplib_int_one(gcd))
                break;
        }
        if (piplib_int_zero(gcd))
            continue;
        if (piplib_int_one(gcd))
            continue;
        for (j = 0; j < tp->width; ++j) {
            if (j == cst)
                piplib_int_floor_div_q(Index(tp, i, j), Index(tp, i, j), gcd);
            else
                piplib_int_div_exact(Index(tp, i, j), Index(tp, i, j), gcd);
        }
    }
    piplib_int_clear(gcd);

    return 0;
}


void tab_display_xx(p, foo)
FILE *foo;
Tableau_xx *p;
{
    char const * const Attr[] = {"Unit", "+", "-", "0", "*", "?"};

    int i, j, ff, fff, n;
    piplib_int_t_xx d;
    piplib_int_init(d);

    fprintf(foo, "cross_product (%ld) /[%d * %d]\n", 0L/*cross_product*/,
            p->height, p->width);
    for(i = 0; i<p->height; i++) {
        fff = ff = p->row[i].flags;
        /* if(fff ==0) continue; */
        piplib_int_assign(d, Denom(p, i));
        n = 0;
        while(fff) {
            if(fff & 1) fprintf(foo, "%s ",Attr[n]);
            n++;
            fff >>= 1;
        }
        fprintf(foo, "%f #[", p->row[i].size);
        if(ff & Unit)
            for(j = 0; j<p->width; j++)
                fprintf(foo, " /%d/",(j == p->row[i].objet.unit)? 1: 0);
        else
            for(j = 0; j<p->width; j++) {
                piplib_int_print(foo, Index(p, i, j));
                putc(' ', foo);
            }
        fprintf(foo, "]/");
        piplib_int_print(foo, d);
        putc('\n', foo);
    }
    piplib_int_clear(d);
}
