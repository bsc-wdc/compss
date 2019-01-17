/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                 traiter.c                                  *
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
 * for more details.                                                          *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with this library; if not, write to the Free Software Foundation,    *
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA         *
 *                                                                            *
 * Written by Paul Feautrier                                                  *
 *                                                                            *
 *****************************************************************************/

#include <stdio.h>
#include <stdlib.h>

#include "pip.h"


/*extern long int cross_product;*/
extern int verbose_xx;
extern FILE *dump_xx;
/*extern int compa_count;*/

#define chercher_xx PIPLIB_NAME(chercher)
int chercher_xx(Tableau_xx *p, int masque, int n) {
    int i;
    for(i = 0; i<n; i++)
        if(p->row[i].flags & masque) break;
    return(i);
}

/* il est convenu que traiter ne doit modifier ni le tableau, ni le contexte;
   le tableau peut grandir en cas de coupure (+1 en hauteur et +1 en largeur
   si nparm != 0) et en cas de partage (+1 en hauteur)(seulement si nparm != 0).
   le contexte peut grandir en cas de coupure (+2 en hauteur et +1 en largeur)
   (seulement si nparm !=0) et en cas de partage (+1 en hauteur)(nparm !=0).
   On estime le nombre de coupures a llog(D) et le nombre de partages a
   ni.
*/

Tableau_xx *expanser_xx(
    Tableau_xx *tp, int virt, int reel,
    int ncol, int off, int dh, int dw) {
    int i, j, ff;
    piplib_int_t_xx *pq;
    piplib_int_t_xx *pp, *qq;
    Tableau_xx *rp;
    if(tp == NULL) return(NULL);
    rp = tab_alloc_xx(reel+dh, ncol+dw, virt);

#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_assign(rp->determinant, tp->determinant);
#else
    rp->l_determinant = tp->l_determinant;
    for(i=0; i<tp->l_determinant; i++)
        rp->determinant[i] = tp->determinant[i];
#endif
    pq = (piplib_int_t_xx *) & (rp->row[virt+reel+dh]);
    for(i = off; i<virt + reel; i++) {
        ff = Flag(rp, i) = Flag(tp, i-off);
        piplib_int_assign(Denom(rp, i), Denom(tp, i-off));
        if(ff & Unit) rp->row[i].objet.unit = tp->row[i-off].objet.unit;
        else {
            rp->row[i].objet.val = pq;
            pq +=(ncol + dw);
            pp = tp->row[i-off].objet.val;
            qq = rp->row[i].objet.val;
            for(j = 0; j<ncol; j++)
                piplib_int_assign(*qq++, *pp++);
        }
    }
    return(rp);
}

/* Check for "obvious" signs of the parametric constant terms
 * of the inequalities.  As soon as a negative sign is found
 * we return from this function and handle this constraint
 * in the calling function.  The signs of the other constraints
 * are then mostly irrelevant.
 * If any of the negative signs is due to the "big parameter",
 * then we want to use this constraint first.
 * We therefore check for signs determined by the coefficient
 * of the big parameter first.
 */
#define exam_coef_xx PIPLIB_NAME(exam_coef)
int exam_coef_xx(Tableau_xx *tp,
                 int nvar, int ncol, int bigparm) {
    int i, j ;
    int ff, fff;
    piplib_int_t_xx *p;

    if (bigparm >= 0)
        for (i = 0; i<tp->height; i++) {
            if (Flag(tp, i) != Unknown)
                continue;
            if (piplib_int_neg(Index(tp,i, bigparm))) {
                Flag(tp, i) = Minus;
                return i;
            } else if (piplib_int_pos(Index(tp,i, bigparm)))
                Flag(tp, i) = Plus;
        }

    for(i = 0; i<tp->height; i++) {
        ff = Flag(tp,i);
        if(ff == 0) break;
        if(ff == Unknown) {
            ff = Zero;
            p = &(tp->row[i].objet.val[nvar+1]);
            for(j = nvar+1; j<ncol; j++) {
                if (piplib_int_neg(*p)) fff = Minus;
                else if (piplib_int_pos(*p)) fff = Plus;
                else fff = Zero;
                p++;
                if(fff != Zero && fff != ff) {
                    if(ff == Zero) {
                        ff = fff;
                    } else {
                        ff = Unknown;
                        break;
                    }
                }
            }
            /* bug de'tecte' par [paf], 16/2/93 !
               Si tous les coefficients des parame`tres sont ne'gatifs
               et si le terme constant est nul, le signe est inconnu!!
               On traite donc spe'cialement le terme constant. */
            if(piplib_int_neg(Index(tp, i, nvar))) fff = Minus;
            else if(piplib_int_pos(Index(tp, i, nvar))) fff = Plus;
            else fff = Zero;
            /* ici on a le signe du terme constant */
            switch(ff) {
            /* le signe est inconnu si les coefficients sont positifs et
               le terme constant ne'gatif */
            case Plus:
                if(fff == Minus) ff = Unknown;
                break;
            /* si les coefficients sont tous nuls, le signe est celui
               du terme constant */
            case Zero:
                ff = fff;
                break;
            /* le signe est inconnu si les coefficients sont ne'gatifs,
               sauf si le terme constant est egalement negatif. */
            case Minus:
                if(fff != Minus) ff = Unknown;
                break;
                /* enfin, il n'y a rien a` dire si le signe des coefficients est inconnu */
            }
            Flag(tp, i) = ff;
            if(ff == Minus) return(i);
        }
    }
    return(i);
}

#define compa_test_xx PIPLIB_NAME(compa_test)
void compa_test_xx(Tableau_xx *tp,
                   Tableau_xx *context,
                   int ni, int nvar, int nparm, int nc) {
    int i, j;
    int ff;
    int cPlus, cMinus, isCritic;
    Tableau_xx *tPlus, *tMinus;
    int p;
    struct high_water_mark_xx q;

    if(nparm == 0) return;
    if(nparm >= MAXPARM) {
        fprintf(stderr, "Too much parameters : %d\n", nparm);
        exit(1);
    }
    q = tab_hwm_xx();

    for(i = 0; i<ni + nvar; i++) {
        ff = Flag(tp,i);
        if(ff & (Critic | Unknown)) {
            isCritic = Pip_True;
            for(j = 0; j<nvar; j++) {
                if(piplib_int_pos(Index(tp, i, j))) {
                    isCritic = Pip_False;
                    break;
                }
            }
            /*compa_count++;*/
            tPlus = expanser_xx(context, nparm, nc, nparm+1, nparm, 1, 0);
            Flag(tPlus, nparm+nc) = Unknown;
            for (j = 0; j < nparm; j++)
                piplib_int_assign(Index(tPlus, nparm+nc, j), Index(tp, i, j+nvar+1));
            piplib_int_assign(Index(tPlus, nparm+nc, nparm), Index(tp, i, nvar));
            if (!isCritic)
                piplib_int_decrement(Index(tPlus, nparm+nc, nparm),
                                     Index(tPlus, nparm+nc, nparm));
            piplib_int_set_si(Denom(tPlus, nparm+nc), 1);

            p = sol_hwm_xx();
            traiter_xx(tPlus, NULL, nparm, 0, nc+1, 0, -1, TRAITER_INT);
            cPlus = is_not_Nil_xx(p);
            if(verbose_xx>0) {
                fprintf(dump_xx, "\nThe positive case has been found ");
                fprintf(dump_xx, cPlus? "possible\n": "impossible\n");
                fflush(dump_xx);
            }

            sol_reset_xx(p);
            tMinus = expanser_xx(context, nparm, nc, nparm+1, nparm, 1, 0);
            Flag(tMinus, nparm+nc) = Unknown;
            for (j = 0; j < nparm; j++)
                piplib_int_oppose(Index(tMinus, nparm+nc, j), Index(tp, i, j+nvar+1));
            piplib_int_oppose(Index(tMinus, nparm+nc, nparm), Index(tp, i, nvar));
            piplib_int_decrement(Index(tMinus, nparm+nc, nparm),
                                 Index(tMinus, nparm+nc, nparm));
            piplib_int_set_si(Denom(tMinus, nparm+nc), 1);
            traiter_xx(tMinus, NULL, nparm, 0, nc+1, 0, -1, TRAITER_INT);
            cMinus = is_not_Nil_xx(p);
            if(verbose_xx>0) {
                fprintf(dump_xx, "\nThe negative case has been found ");
                fprintf(dump_xx, cMinus? "possible\n": "impossible\n");
                fflush(dump_xx);
            }

            sol_reset_xx(p);
            if (cPlus && cMinus) {
                Flag(tp,i) = isCritic ? Critic : Unknown;
            } else if (cMinus) {
                Flag(tp,i) = Minus;
                break;
            } else {
                Flag(tp,i) = cPlus ? Plus : Zero;
            }
        }
    }
    tab_reset_xx(q);

    return;
}

#define valeur_xx PIPLIB_NAME(valeur)
piplib_int_t_xx *valeur_xx(
    Tableau_xx *tp, int i, int j, piplib_int_t_xx* zero) {
    if(Flag(tp, i) & Unit)
        return(tp->row[i].objet.unit == j ? &Denom(tp,i) : zero);
    else return(&Index(tp, i, j));
}

#define solution_xx PIPLIB_NAME(solution)
void solution_xx(Tableau_xx *tp, int nvar, int nparm) {
    int i, j;
    int ncol = nvar + nparm + 1;

    piplib_int_t_xx zero;
    piplib_int_init_set_si(zero, 0);

    sol_list_xx(nvar);
    for(i = 0; i<nvar; i++) {
        sol_forme_xx(nparm+1);
        for(j = nvar+1; j<ncol; j++)
            sol_val_xx(*valeur_xx(tp, i, j, &zero), Denom(tp,i));
        sol_val_xx(*valeur_xx(tp, i, nvar, &zero), Denom(tp,i));
    }

    piplib_int_clear(zero);
}

#define solution_dual_xx PIPLIB_NAME(solution_dual)
static void solution_dual_xx(
    Tableau_xx *tp, int nvar/*, int nparm*/, int *pos) {
    int i;

    piplib_int_t_xx zero;
    piplib_int_init_set_si(zero, 0);

    sol_list_xx(tp->height - nvar);
    for (i = 0; i < tp->height - nvar; ++i) {
        sol_forme_xx(1);
        if (Flag(tp, pos[i]) & Unit)
            sol_val_xx(
                *valeur_xx(tp, 0, tp->row[pos[i]].objet.unit, &zero),
                Denom(tp, 0));
        else
            sol_val_zero_one_xx();
    }

    piplib_int_clear(zero);
}

#define choisir_piv_xx PIPLIB_NAME(choisir_piv)
int choisir_piv_xx(
    Tableau_xx *tp, int pivi, int nvar, int nligne) {
    int j, k;
    piplib_int_t_xx pivot, foo, x, y;
    int pivj = -1;

    piplib_int_t_xx zero;
    piplib_int_init_set_si(zero, 0);

    piplib_int_init(pivot);
    piplib_int_init(foo);
    piplib_int_init(x);
    piplib_int_init(y);

    for(j = 0; j<nvar; j++) {
        piplib_int_assign(foo, Index(tp, pivi, j));
        if(piplib_int_pos(foo) == 0) continue;
        if(pivj < 0) {
            pivj = j;
            piplib_int_assign(pivot, foo);
            continue;
        }
        for(k = 0; k<nligne; k++) {
            piplib_int_mul(x, pivot, *valeur_xx(tp, k, j, &zero));
            piplib_int_mul(y, *valeur_xx(tp, k, pivj, &zero), foo);
            piplib_int_sub(x, x, y);
            /*cross_product++;*/
            if(piplib_int_zero(x) == 0) break;
        }
        if(piplib_int_neg(x)) {
            pivj = j;
            piplib_int_assign(pivot, foo);
        }
    }

    piplib_int_clear(pivot);
    piplib_int_clear(foo);
    piplib_int_clear(x);
    piplib_int_clear(y);

    piplib_int_clear(zero);

    return(pivj);
}


#define pivoter_xx PIPLIB_NAME(pivoter)
int pivoter_xx(
    Tableau_xx *tp, int pivi, int nvar, int nparm, int ni) {
    int pivj;
    int ncol = nvar + nparm + 1;
    int nligne = nvar + ni;
    int i, j, k;
#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_t_xx x;
#endif
    piplib_int_t_xx y, d, gcd, dpiv;
    int ff, fff;
    piplib_int_t_xx pivot, foo, z;
    piplib_int_t_xx ppivot, dppiv;
    piplib_int_t_xx *new, *p, *q;
    piplib_int_t_xx lpiv;

    new = (piplib_int_t_xx *)malloc(ncol * sizeof(piplib_int_t_xx));
    if(new == NULL) {
        fprintf(stdout, "Memory overflow");
        exit(1);
    }

    if(0 > pivi || pivi >= nligne || Flag(tp, pivi) == Unit) {
        fprintf(stdout, "Syserr : pivoter_xx : wrong pivot row\n");
        exit(1);
    }

    pivj = choisir_piv_xx(tp, pivi, nvar, nligne);
    if(pivj < 0) {
        free(new);
        return(-1);
    }
    if(pivj >= nvar) {
        fprintf(stdout, "Syserr : pivoter_xx : wrong pivot\n");
        exit(1);
    }

#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_init(x);
#endif
    piplib_int_init(y);
    piplib_int_init(d);
    piplib_int_init(gcd);
    piplib_int_init(dpiv);
    piplib_int_init(lpiv);
    piplib_int_init(pivot);
    piplib_int_init(foo);
    piplib_int_init(z);
    piplib_int_init(ppivot);
    piplib_int_init(dppiv);

    for(i=0; i<ncol; i++)
        piplib_int_init(new[i]);

    piplib_int_assign(pivot, Index(tp, pivi, pivj));
    piplib_int_assign(dpiv, Denom(tp, pivi));
    piplib_int_gcd(d, pivot, dpiv);
    piplib_int_div_exact(ppivot, pivot, d);
    piplib_int_div_exact(dppiv, dpiv, d);

    if(verbose_xx>1) {
        fprintf(dump_xx, "Pivot ");
        piplib_int_print(dump_xx, ppivot);
        putc('/', dump_xx);
        piplib_int_print(dump_xx, dppiv);
        putc('\n', dump_xx);
        fprintf(dump_xx, "%d x %d\n", pivi, pivj);
    }

#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_floor_div_q_r(x, y, tp->determinant, dppiv);
#else
    for(i=0; i< tp->l_determinant; i++) {
        piplib_int_gcd(d, tp->determinant[i], dppiv);
        tp->determinant[i] /= d;
        dppiv /= d;
    }
#endif

#if defined(PIPLIB_ONE_DETERMINANT)
    if (piplib_int_zero(y) == 0) {
#else
    if(dppiv != 1) {
#endif
        fprintf(stderr, "Integer overflow\n");
        if(verbose_xx>0) fflush(dump_xx);
        exit(1);
    }

#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_mul(tp->determinant, x, ppivot);
#else
    for(i=0; i<tp->l_determinant; i++)
        if(piplib_lllog2_xx(tp->determinant[i])
                + piplib_lllog2_xx(ppivot)
                < 8*sizeof(piplib_int_t_xx)) {
            tp->determinant[i] *= ppivot;
            break;
        }
    if(i >= tp->l_determinant) {
        tp->l_determinant++;
        if(tp->l_determinant >= MAX_DETERMINANT) {
            fprintf(stderr, "Integer overflow : %d\n", tp->l_determinant);
            exit(1);
        }
        tp->determinant[i] = ppivot;
    }
#endif

    if(verbose_xx>1) {
        fprintf(dump_xx, "determinant ");
#if defined(PIPLIB_ONE_DETERMINANT)
        piplib_int_print(dump_xx, tp->determinant);
#else
        for(i=0; i<tp->l_determinant; i++)
            fprintf(dump_xx, piplib_int_format, tp->determinant[i]);
#endif
        fprintf(dump_xx, "\n");
    }


    for(j = 0; j<ncol; j++)
        if(j==pivj)
            piplib_int_assign(new[j], dpiv);
        else
            piplib_int_oppose(new[j], Index(tp, pivi, j));

    for(k = 0; k<nligne; k++) {
        if(Flag(tp,k) & Unit)continue;
        if(k == pivi)continue;
        piplib_int_assign(foo, Index(tp, k, pivj));
        piplib_int_gcd(d, pivot, foo);
        piplib_int_div_exact(lpiv, pivot, d);
        piplib_int_div_exact(foo, foo, d);
        piplib_int_assign(d, Denom(tp,k));
        piplib_int_mul(gcd, lpiv, d);
        piplib_int_assign(Denom(tp, k), gcd);
        p = tp->row[k].objet.val;
        q = tp->row[pivi].objet.val;
        for(j = 0; j<ncol; j++) {
            if(j == pivj)
                piplib_int_mul(z, dpiv, foo);
            else {
                piplib_int_mul(z, *p, lpiv);
                piplib_int_mul(y, *q, foo);
                piplib_int_sub(z, z, y);
            }
            q++;
            /*cross_product++;*/
            piplib_int_assign(*p, z);
            p++;
            if (piplib_int_one(gcd) == 0)
                piplib_int_gcd(gcd, gcd, z);
        }
        if(piplib_int_one(gcd) == 0) {
            p = tp->row[k].objet.val;
            for(j = 0; j<ncol; j++) {
                piplib_int_div_exact(*p, *p, gcd);
                p++;
            }
            piplib_int_div_exact(Denom(tp,k), Denom(tp,k), gcd);
        }
    }
    p = tp->row[pivi].objet.val;
    for(k = 0; k<nligne; k++)
        if((Flag(tp, k) & Unit) && tp->row[k].objet.unit == pivj) break;
    Flag(tp, k) = Plus;
    tp->row[k].objet.val = p;
    for(j = 0; j<ncol; j++) {
        piplib_int_assign(*p, new[j]);
        p++;
    }

    piplib_int_assign(Denom(tp, k), pivot);
    Flag(tp, pivi) = Unit | Zero;
    piplib_int_set_si(Denom(tp, pivi), 1);
    tp->row[pivi].objet.unit = pivj;

    for(k = 0; k<nligne; k++) {
        ff = Flag(tp, k);
        if(ff & Unit) continue;
        if (piplib_int_neg(Index(tp, k, pivj))) fff = Minus;
        else if(piplib_int_zero(Index(tp, k, pivj))) fff = Zero;
        else fff = Plus;
        if (fff != Zero && fff != ff)  {
            if(ff == Zero) {
                ff = (fff == Minus ? Unknown : fff);
            } else {
                ff = Unknown;
            }
        }
        Flag(tp, k) = ff;
    }

    if(verbose_xx>2) {
        fprintf(dump_xx, "just pivoted\n");
        tab_display_xx(tp, dump_xx);
    }

#if defined(PIPLIB_ONE_DETERMINANT)
    piplib_int_clear(x);
#endif
    piplib_int_clear(y);
    piplib_int_clear(d);
    piplib_int_clear(gcd);
    piplib_int_clear(dpiv);
    piplib_int_clear(lpiv);
    piplib_int_clear(pivot);
    piplib_int_clear(foo);
    piplib_int_clear(z);
    piplib_int_clear(ppivot);
    piplib_int_clear(dppiv);

    for(i=0; i<ncol; i++)
        piplib_int_clear(new[i]);
    free(new);
    return(0);
}

/*
 * Sort the rows in increasing order of the largest coefficient
 * and (if TRAITER_DUAL is set) return the new position of the
 * original constraints.
 */
#define tab_sort_rows_xx PIPLIB_NAME(tab_sort_rows)
static int *tab_sort_rows_xx(
    Tableau_xx *tp, int nvar, int nligne, int flags) {
#define piplib_max(x,y) ((x) > (y)? (x) : (y))

    int i, j;
    int pivi;
    double s, t, d, smax = 0;
    struct L_xx temp;
    int *pos = NULL, *ineq = NULL;

    if (flags & TRAITER_DUAL) {
        ineq = malloc(tp->height * sizeof(int));
        pos = malloc((tp->height-nvar) * sizeof(int));
        if (!ineq || !pos) {
            fprintf(stderr, "Memory Overflow.\n") ;
            exit(1) ;
        }
    }

    for (i = nvar; i < nligne; i++) {
        if (Flag(tp,i) & Unit)
            continue;
        s = 0;
        d = piplib_int_get_d(Denom(tp, i));
        for (j = 0; j < nvar; j++) {
            t = piplib_int_get_d(Index(tp,i,j))/d;
            s = piplib_max(s, abs((int)t)); // NdCed: or fabs(t) ?
        }
        tp->row[i].size = s;
        smax = piplib_max(s, smax);
        if (flags & TRAITER_DUAL)
            ineq[i] = i-nvar;
    }

    for (i = nvar; i < nligne; i++) {
        if (Flag(tp,i) & Unit)
            continue;
        s = smax;
        pivi = i;
        for (j = i; j < nligne; j++) {
            if (Flag(tp,j) & Unit)
                continue;
            if (tp->row[j].size < s) {
                s = tp->row[j].size;
                pivi = j;
            }
        }
        if (pivi != i) {
            temp = tp->row[pivi];
            tp->row[pivi] = tp->row[i];
            tp->row[i] = temp;
            if (flags & TRAITER_DUAL) {
                j = ineq[i];
                ineq[i] = ineq[pivi];
                ineq[pivi] = j;
            }
        }
    }

    if (flags & TRAITER_DUAL) {
        for (i = nvar; i < nligne; i++)
            pos[ineq[i]] = i;
        free(ineq);
    }

    return pos;
}

/* dans cette version, "traiter" modifie ineq; par contre
   le contexte est immediatement recopie' */

void traiter_xx(
    Tableau_xx *tp, Tableau_xx *ctxt,
    int nvar, int nparm, int ni, int nc, int bigparm, int flags) {
    static int profondeur = 1;
    int j;
    int pivi, nligne, ncol;
    struct high_water_mark_xx x;
    Tableau_xx *context;
    int dch, dcw;
    int *pos;
#if !defined(PIPLIB_ONE_DETERMINANT)
    int i;
#endif

#if defined(PIPLIB_ONE_DETERMINANT)
    dcw = piplib_int_size_in_base_2(tp->determinant);
#else
    dcw = 0;
    for(i=0; i<tp->l_determinant; i++)
        dcw += piplib_lllog2_xx(tp->determinant[i]);
#endif
    dch = 2 * dcw + 1;
    x = tab_hwm_xx();
    nligne = nvar+ni;

    context = expanser_xx(ctxt, 0, nc, nparm+1, 0, dch, dcw);

    pos = tab_sort_rows_xx(tp, nvar, nligne, flags);

    for(;;) {
        if(verbose_xx>2) {
            fprintf(dump_xx, "debut for\n");
            tab_display_xx(tp, dump_xx);
            fflush(dump_xx);
        }
        nligne = nvar+ni;
        ncol = nvar+nparm+1;
        if(nligne > tp->height || ncol > tp->width) {
            fprintf(stdout, "Syserr : traiter_xx : tableau too small\n");
            exit(1);
        }
        pivi = chercher_xx(tp, Minus, nligne);
        if(pivi < nligne) goto pirouette;	       /* There is a negative row   */

        pivi = exam_coef_xx(tp, nvar, ncol, bigparm);

        if(verbose_xx>2) {
            fprintf(dump_xx, "coefs examined\n");
            tab_display_xx(tp, dump_xx);
            fflush(dump_xx);
        }

        if(pivi < nligne) goto pirouette;
        /* There is a row whose coefficients are negative */
        compa_test_xx(tp, context, ni, nvar, nparm, nc);
        if(verbose_xx>2) {
            fprintf(dump_xx, "compatibility tested\n");
            tab_display_xx(tp, dump_xx);
            fflush(dump_xx);
        }

        pivi = chercher_xx(tp, Minus, nligne);
        if(pivi < nligne) goto pirouette;
        /* The compatibility test has found a negative row */
        pivi = chercher_xx(tp, Critic, nligne);
        if(pivi >= nligne)pivi = chercher_xx(tp, Unknown, nligne);
        /* Here, the problem tree splits        */
        if(pivi < nligne) {
            Tableau_xx * ntp;
            piplib_int_t_xx com_dem;
            struct high_water_mark_xx q;
            if(nc >= context->height) {
#if defined(PIPLIB_ONE_DETERMINANT)
                dcw = piplib_int_size_in_base_2(context->determinant);
#else
                dcw = 0;
                for(i=0; i<tp->l_determinant; i++)
                    dcw += piplib_lllog2_xx(tp->determinant[i]);
#endif
                dch = 2 * dcw + 1;
                context = expanser_xx(context, 0, nc, nparm+1, 0, dch, dcw);
            }
            if(nparm >= MAXPARM) {
                fprintf(stdout, "Too much parameters : %d\n", nparm);
                exit(2);
            }
            q = tab_hwm_xx();
            if(verbose_xx>1)
                fprintf(stdout,"profondeur %d %p\n", profondeur, q.top);
            ntp = expanser_xx(tp, nvar, ni, ncol, 0, 0, 0);
            fflush(stdout);
            sol_if_xx();
            sol_forme_xx(nparm+1);
            piplib_int_init_set_si(com_dem, 0);
            for (j = 0; j < nparm; j++)
                piplib_int_gcd(com_dem, com_dem, Index(tp, pivi, j + nvar +1));
            if (!(flags & TRAITER_INT))
                piplib_int_gcd(com_dem, com_dem, Index(tp, pivi, nvar));
            for (j = 0; j < nparm; j++) {
                piplib_int_div_exact(Index(context, nc, j),
                                     Index(tp, pivi, j + nvar + 1), com_dem);
                sol_val_one_xx(Index(context, nc, j));
            }
            if (!(flags & TRAITER_INT))
                piplib_int_div_exact(Index(context, nc, nparm),
                                     Index(tp, pivi, nvar), com_dem);
            else
                piplib_int_floor_div_q(Index(context, nc, nparm),
                                       Index(tp, pivi, nvar), com_dem);
            sol_val_one_xx(Index(context, nc, nparm));
            piplib_int_clear(com_dem);
            Flag(context, nc) = Unknown;
            piplib_int_set_si(Denom(context, nc), 1);
            Flag(ntp, pivi) = Plus;
            profondeur++;
            fflush(stdout);
            if(verbose_xx > 0) fflush(dump_xx);
            traiter_xx(ntp, context, nvar, nparm, ni, nc+1, bigparm, flags);
            profondeur--;
            tab_reset_xx(q);
            if(verbose_xx>1)
                fprintf(stdout,
                        "descente %d %p\n", profondeur, tab_hwm_xx().top);
            for(j = 0; j<nparm; j++)
                piplib_int_oppose(Index(context, nc, j), Index(context, nc, j));
            piplib_int_increment(Index(context, nc, nparm), Index(context, nc, nparm));
            piplib_int_oppose(Index(context, nc, nparm), Index(context, nc, nparm));
            Flag(tp, pivi) = Minus;
            piplib_int_set_si(Denom(context, nc), 1);
            nc++;
            goto pirouette;
        }
        /* Here, all rows are positive. Do we need an integral solution?      */
        if (!(flags & TRAITER_INT)) {
            solution_xx(tp, nvar, nparm);
            if (flags & TRAITER_DUAL)
                solution_dual_xx(tp, nvar/*, nparm*/, pos);
            break;
        }
        /* Yes we do! */
        pivi = integrer_xx(
                   &tp, &context, &nvar, &nparm, &ni, &nc, bigparm);
        if(pivi > 0) goto pirouette;
        /* A cut has been inserted and is always negative */
        /* Here, either there is an integral solution, */
        if(pivi == 0) solution_xx(tp, nvar, nparm);
        /* or no solution exists */
        else sol_nil_xx();
        break;

        /* Here, a negative row has been found. The call to <<pivoter>> executes
              a pivoting step                                                 */

pirouette :
        if (pivoter_xx(tp, pivi, nvar, nparm, ni) < 0) {
            sol_nil_xx();
            break;
        }
    }
    /* Danger : a premature return would induce memory leaks   */
    tab_reset_xx(x);
    free(pos);
    return;
}
