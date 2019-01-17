/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                   sol.h                                    *
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
 ******************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "pip.h"

extern int verbose_xx;
extern FILE *dump_xx;

#define S_xx PIPLIB_NAME(S)
struct S_xx {
    int flags;
    piplib_int_t_xx param1, param2;
};

#define Free 0
#define Nil  1
#define If   2
#define List 3
#define Form 4
#define New  5
#define Div  6
#define Val  7
#define Error 8

#define sol_space_xx PIPLIB_NAME(sol_space)
#define sol_free_xx PIPLIB_NAME(sol_free)
struct S_xx * sol_space_xx;
static int sol_free_xx;

void sol_init_xx(void) {
    sol_free_xx = 0;
    sol_space_xx = (struct S_xx *)malloc(
                       SOL_SIZE*sizeof(struct S_xx)) ;
}

void sol_close_xx(void) {
    free(sol_space_xx) ;
}

int sol_hwm_xx() {
    return(sol_free_xx);
}

void sol_reset_xx(p)
int p;
{
    int i;
    if(p<0 || p>=SOL_SIZE) {
        fprintf(stderr,
                "Syserr : sol_reset_xx : Memory allocation error\n");
        exit(40);
    }
    for(i=p; i<sol_free_xx; i++) {
        piplib_int_clear(sol_space_xx[i].param1);
        piplib_int_clear(sol_space_xx[i].param2);
    }
    sol_free_xx = p;
}

#define sol_alloc_xx PIPLIB_NAME(sol_alloc)
struct S_xx *sol_alloc_xx(void) {
    struct S_xx *r;
    r = sol_space_xx + sol_free_xx;
    r->flags = Free;
    piplib_int_init_set_si(r->param1,0);
    piplib_int_init_set_si(r->param2,0);
    sol_free_xx++;
    if(sol_free_xx >= SOL_SIZE) {
        fprintf(stderr, "The solution is too complex! : sol\n");
        exit(26);
    }
    return(r);
}

void sol_nil_xx(void) {
    struct S_xx * r;
    r = sol_alloc_xx();
    r -> flags = Nil;
    if(verbose_xx > 0) {
        fprintf(dump_xx, "\nNil");
        fflush(dump_xx);
    }
}

void sol_error_xx(int c) {
    struct S_xx *r;
    r = sol_alloc_xx();
    r->flags = Nil;
    piplib_int_set_si(r->param1, c);
    if(verbose_xx > 0) {
        fprintf(dump_xx, "Erreur %d\n", c);
        fflush(dump_xx);
    }
}

int is_not_Nil_xx(p)
int p;
{
    return(sol_space_xx[p].flags != Nil);
}

void sol_if_xx(void) {
    struct S_xx *r;
    r = sol_alloc_xx();
    r -> flags = If;
    if(verbose_xx > 0) {
        fprintf(dump_xx, "\nIf ");
        fflush(dump_xx);
    }
}

void sol_list_xx(n)
int n;
{
    struct S_xx * r;
    r = sol_alloc_xx();
    r->flags = List;
    piplib_int_set_si(r->param1, n);
    if(verbose_xx > 0) {
        fprintf(dump_xx, "\nList %d ", n);
        fflush(dump_xx);
    }
}

void sol_forme_xx(l)
int l;
{
    struct S_xx *r;
    r = sol_alloc_xx();
    r -> flags = Form;
    piplib_int_set_si(r -> param1, l);
    if(verbose_xx > 0) {
        fprintf(dump_xx, "\nForme %d ", l);
        fflush(dump_xx);
    }
}

void sol_new_xx(k)
int k;
{
    struct S_xx *r;
    r = sol_alloc_xx();
    r -> flags = New;
    piplib_int_set_si(r -> param1, k);
    if(verbose_xx > 0) {
        fprintf(dump_xx, "New %d ", k);
        fflush(dump_xx);
    }
}

void sol_div_xx() {
    struct S_xx *r;
    r = sol_alloc_xx();
    r -> flags = Div;
    if(verbose_xx > 0) {
        fprintf(dump_xx, "Div ");
        fflush(dump_xx);
    }
}

void sol_val_xx(n, d)
piplib_int_t_xx n, d;
{
    struct S_xx *r;
    r = sol_alloc_xx();
    r -> flags = Val;
    piplib_int_assign(r->param1, n);
    piplib_int_assign(r->param2, d);
    if(verbose_xx > 0) {
        fprintf(dump_xx, "val(");
        piplib_int_print(dump_xx, n);
        fprintf(dump_xx, "/");
        piplib_int_print(dump_xx, d);
        fprintf(dump_xx, ") ");
        fflush(dump_xx);
    }
}

void sol_val_one_xx(piplib_int_t_xx n) {
    piplib_int_t_xx one;
    piplib_int_init_set_si(one, 1);
    sol_val_xx(n, one);
    piplib_int_clear(one);
}

void sol_val_zero_one_xx() {
    piplib_int_t_xx zero;
    piplib_int_t_xx one;
    piplib_int_init_set_si(zero, 0);
    piplib_int_init_set_si(one, 1);
    sol_val_xx(zero, one);
    piplib_int_clear(one);
    piplib_int_clear(zero);
}

#define skip_xx PIPLIB_NAME(skip)
int skip_xx(int);

/* a` partir d'un point de la solution, sauter un objet
bien forme' ainsi qu'un e'ventuel New et pointer sur l'objet
suivant */

#define skip_New_xx PIPLIB_NAME(skip_New)
int skip_New_xx (int i) {
    if(sol_space_xx[i].flags != New) return i;
    i = skip_xx(i+1);      /* sauter le Div */
    return i;
}
/* au lancement, i indexe une cellule qui est la te^te d'un objet.
   la valeur retourne'e est la te^te de l'objet qui suit. Les
   objets de type New sont e'limine's                                */

int skip_xx (int i) {
    int n, f;
    while((f = sol_space_xx[i].flags) == Free || f == Error) i++;
    switch (sol_space_xx[i].flags) {
    case Nil :
    case Val :
        i++;
        break;
    case New :
        i = skip_New_xx(i);
        break;
    case If :
        i = skip_xx(i+1);        /* sauter le pre'dicat */
        i = skip_xx(i);          /* sauter le vrai */
        i = skip_xx(i);
        break;   /* sauter le faux */
    case List :
    case Form :
        n = piplib_int_get_si(sol_space_xx[i].param1);
        i++;
        while(n--) i = skip_xx(i);
        break;
    case Div :
        i = skip_xx(i+1);       /* sauter la forme */
        i = skip_xx(i);         /* sauter le diviseur */
        break;
    default :
        fprintf(stderr,
                "Syserr : skip_xx : unknown %d\n",
                sol_space_xx[i].flags);
    }
    return skip_New_xx(i);
}
/* simplification de la solution : e'limination des constructions
   (if p () ()). N'est en service qu'en pre'sence de l'option -z */

void sol_simplify_xx(int i) {
    int j, k, l;
    if(sol_space_xx[i].flags == If) {
        j = skip_xx(i+1);        /* j : debut de la partie vraie */
        k = skip_xx(j);          /* k : debut de la partie fausse */
        sol_simplify_xx(k);
        sol_simplify_xx(j);
        if (sol_space_xx[j].flags == Nil &&
                sol_space_xx[k].flags == Nil) {
            sol_space_xx[i].flags = Nil;
            if (k >= sol_free_xx - 1)
                sol_reset_xx(i+1);
            else for(l = i+1; l<=k; l++) sol_space_xx[l].flags = Free;
        }
    }

}
/* e'dition de la solution */

int sol_edit_xx(FILE *foo, int i) {
    int j, n;
    struct S_xx *p;
    piplib_int_t_xx N, D, d;
    piplib_int_init(N);
    piplib_int_init(D);
    piplib_int_init(d);

    p = sol_space_xx + i;
    for(;;) {
        if(p->flags == Free) {
            p++;
            i++;
            continue;
        }
        if(p->flags == New) {
            n = piplib_int_get_si(p->param1);
            fprintf(foo, "(newparm %d ", n);
            if(verbose_xx>0)fprintf(dump_xx, "(newparm %d ", n);
            i = sol_edit_xx(foo, ++i);
            p = sol_space_xx +i;
            fprintf(foo, ")\n");
            if(verbose_xx>0)fprintf(dump_xx, ")\n");
            continue;
        }
        break;
    }
    switch(p->flags) {
    case Nil :
        fprintf(foo, "()\n");
        if(verbose_xx>0)fprintf(dump_xx, "()\n");
        i++;
        break;
    case Error :
        fprintf(foo, "Error %d\n", piplib_int_get_si(p->param1));
        if(verbose_xx>0)
            fprintf(dump_xx, "Error %d\n", piplib_int_get_si(p->param1));
        i++;
        break;
    case If  :
        fprintf(foo, "(if ");
        if(verbose_xx>0)fprintf(dump_xx, "(if ");
        i = sol_edit_xx(foo, ++i);
        i = sol_edit_xx(foo, i);
        i = sol_edit_xx(foo, i);
        fprintf(foo, ")\n");
        if(verbose_xx>0)fprintf(dump_xx, ")\n");
        break;
    case List:
        fprintf(foo, "(list ");
        if(verbose_xx>0)fprintf(dump_xx, "(list ");
        n = piplib_int_get_si(p->param1);
        i++;
        while(n--) i = sol_edit_xx(foo, i);
        fprintf(foo, ")\n");
        if(verbose_xx>0)fprintf(dump_xx, ")\n");
        break;
    case Form:
        fprintf(foo, "#[");
        if(verbose_xx>0)fprintf(dump_xx, "#[");
        n = piplib_int_get_si(p->param1);
        for(j = 0; j<n; j++) {
            i++;
            p++;
            piplib_int_assign(N, p->param1);
            piplib_int_assign(D, p->param2);
            piplib_int_gcd(d, N, D);
            if(piplib_int_eq(d, D)) {
                putc(' ', foo);
                piplib_int_div_exact(N, N, d);
                piplib_int_print(foo, N);
                if(verbose_xx>0) {
                    putc(' ', dump_xx);
                    piplib_int_print(dump_xx, N);
                }
            } else {
                piplib_int_div_exact(N, N, d);
                piplib_int_div_exact(D, D, d);
                putc(' ', foo);
                piplib_int_print(foo, N);
                putc('/', foo);
                piplib_int_print(foo, D);
                if(verbose_xx>0) {
                    putc(' ', dump_xx);
                    piplib_int_print(dump_xx, N);
                    putc('/', dump_xx);
                    piplib_int_print(dump_xx, D);
                }
            }
        }
        fprintf(foo, "]\n");
        if(verbose_xx>0)fprintf(dump_xx, "]\n");
        i++;
        break;
    case Div :
        fprintf(foo, "(div ");
        if(verbose_xx>0)fprintf(dump_xx, "(div ");
        i = sol_edit_xx(foo, ++i);
        i = sol_edit_xx(foo, i);
        fprintf(foo, ")\n");
        if(verbose_xx>0)fprintf(dump_xx, ")\n");
        break;
    case Val :
        piplib_int_assign(N, p->param1);
        piplib_int_assign(D, p->param2);
        piplib_int_gcd(d, N, D);
        if (piplib_int_eq(d, D)) {
            piplib_int_div_exact(N, N, d);
            putc(' ', foo);
            piplib_int_print(foo, N);
            if(verbose_xx>0) {
                putc(' ', dump_xx);
                piplib_int_print(dump_xx, N);
            }
        } else {
            piplib_int_div_exact(N, N, d);
            piplib_int_div_exact(D, D, d);
            putc(' ', foo);
            piplib_int_print(foo, N);
            fprintf(foo, "/");
            piplib_int_print(foo, D);
            if(verbose_xx>0) {
                putc(' ', dump_xx);
                piplib_int_print(dump_xx, N);
                fprintf(dump_xx, "/");
                piplib_int_print(dump_xx, D);
            }
        }
        i++;
        break;
    default  :
        fprintf(foo, "Inconnu : sol\n");
        if(verbose_xx>0)fprintf(dump_xx, "Inconnu : sol\n");
    }
    piplib_int_clear(d);
    piplib_int_clear(D);
    piplib_int_clear(N);
    return(i);
}


/* Fonction sol_vector_edit :
 * Cette fonction a pour but de placer les informations correspondant
 * a un Vector dans la grammaire dans une structure de type PipVector. Elle
 * prend en parametre un pointeur vers une case memoire contenant le
 * numero de cellule du tableau sol_space a partir de laquelle on doit
 * commencer la lecture des informations. Elle retourne un pointeur vers
 * une structure de type PipVector contenant les informations de ce Vector.
 * Premiere version : Ced. 20 juillet 2001.
 */
#define sol_vector_edit_xx PIPLIB_NAME(sol_vector_edit)
PipVector_xx * sol_vector_edit_xx(int *i, int Bg,
                                  int Urs_p, int flags) {
    int j, k, n, unbounded  = 0, first_urs;
    struct S_xx *p ;
    piplib_int_t_xx N, D, d ;
    PipVector_xx * vector ;

    piplib_int_init(N);
    piplib_int_init(D);
    piplib_int_init(d);

    vector = (PipVector_xx *)malloc(sizeof(PipVector_xx)) ;
    if (vector == NULL) {
        fprintf(stderr, "Memory Overflow.\n") ;
        exit(1) ;
    }
    p = sol_space_xx + (*i) ;
    n = piplib_int_get_si(p->param1);
    if (flags & SOL_REMOVE)
        --n;
    n -= Urs_p;
    first_urs = Urs_p + (Bg >= 0);
    vector->nb_elements = n ;
    vector->the_vector = (piplib_int_t_xx *)malloc(
                             sizeof(piplib_int_t_xx)*n) ;
    if (vector->the_vector == NULL) {
        fprintf(stderr, "Memory Overflow.\n") ;
        exit(1) ;
    }
    vector->the_deno = (piplib_int_t_xx *)malloc(
                           sizeof(piplib_int_t_xx)*n) ;
    if (vector->the_deno == NULL) {
        fprintf(stderr, "Memory Overflow.\n") ;
        exit(1) ;
    }

    for (j=0, k=0; k < n; j++) {
        (*i)++ ;
        p++ ;

        piplib_int_assign(N, p->param1);
        piplib_int_assign(D, p->param2);
        piplib_int_gcd(d, N, D);

        if ((flags & SOL_SHIFT) && j == Bg) {
            piplib_int_sub(N, N, D);   /* subtract 1 */
            if (piplib_int_zero(N) == 0)
                unbounded = 1;
        }

        if ((flags & SOL_REMOVE) && j == Bg)
            continue;

        if (first_urs <= j && j < first_urs+Urs_p)
            continue;

        piplib_int_init(vector->the_vector[k]);
        piplib_int_div_exact(vector->the_vector[k], N, d);
        if (flags & SOL_NEGATE)
            piplib_int_oppose(vector->the_vector[k], vector->the_vector[k]);
        piplib_int_init(vector->the_deno[k]);
        if (piplib_int_eq(d, D))
            piplib_int_set_si(vector->the_deno[k], 1);
        else
            piplib_int_div_exact(vector->the_deno[k], D, d);
        ++k;
    }
    if (unbounded)
        for (k=0; k < n; k++)
            piplib_int_set_si(vector->the_deno[k], 0);
    (*i)++ ;

    piplib_int_clear(d);
    piplib_int_clear(D);
    piplib_int_clear(N);

    return(vector) ;
}


/* Fonction sol_newparm_edit :
 * Cette fonction a pour but de placer les informations correspondant
 * a un Newparm dans la grammaire dans une structure de type PipNewparm. Elle
 * prend en parametre un pointeur vers une case memoire contenant le
 * numero de cellule du tableau sol_space a partir de laquelle on doit
 * commencer la lecture des informations. Elle retourne un pointeur vers
 * une structure de type PipNewparm contenant les informations de ce Newparm.
 * Premiere version : Ced. 18 octobre 2001.
 */
#define sol_newparm_edit_xx PIPLIB_NAME(sol_newparm_edit)
PipNewparm_xx * sol_newparm_edit_xx(int *i, int Bg,
                                    int Urs_p, int flags) {
    struct S_xx * p ;
    PipNewparm_xx * newparm,
                  * newparm_first = NULL,
                    * newparm_now = NULL;

    /* On place p au lieu de lecture. */
    p = sol_space_xx + (*i) ;

    do {
        /* On passe le New et le Div pour aller a Form et lire le VECTOR. */
        (*i) += 2 ;

        newparm = (PipNewparm_xx *)malloc(
                      sizeof(PipNewparm_xx));
        if (newparm == NULL) {
            fprintf(stderr, "Memory Overflow.\n") ;
            exit(1) ;
        }
        newparm->vector = sol_vector_edit_xx(i, Bg, Urs_p, flags);
        newparm->rank = piplib_int_get_si(p->param1);
        /* On met p a jour pour lire le denominateur (un Val de param2 1). */
        p = sol_space_xx + (*i) ;
        piplib_int_init(newparm->deno);
        piplib_int_assign(newparm->deno, p->param1);
        if (flags & SOL_REMOVE)
            newparm->rank--;
        newparm->rank -= Urs_p;
        newparm->next = NULL ;

        if (newparm_now)
            newparm_now->next = newparm;
        else
            newparm_first = newparm;
        newparm_now = newparm ;
        if (verbose_xx > 0) {
            fprintf(dump_xx,"\n(newparm ") ;
            fprintf(dump_xx, "%i", newparm->rank) ;
            fprintf(dump_xx," (div ") ;
            pip_vector_print_xx(dump_xx,newparm->vector) ;
            fprintf(dump_xx," ") ;
            piplib_int_print(dump_xx, newparm->deno);
            fprintf(dump_xx,"))") ;
        }

        /* On passe aux elements suivants. */
        (*i) ++ ;
        p = sol_space_xx + (*i) ;
    } while (p->flags == New);

    return newparm_first;
}


/* Fonction sol_list_edit :
 * Cette fonction a pour but de placer les informations correspondant
 * a une List dans la grammaire dans une structure de type PipList. Elle
 * prend en parametre un pointeur vers une case memoire contenant le
 * numero de cellule du tableau sol_space a partir de laquelle on doit
 * commencer la lecture des informations. Elle retourne un pointeur vers
 * une structure de type PipList contenant les informations de cette List.
 * Premiere version : Ced. 18 octobre 2001.
 * 16 novembre 2005 : Ced. Prise en compte du cas 0 éléments, avant impossible.
 */
#define sol_list_edit_xx PIPLIB_NAME(sol_list_edit)
PipList_xx * sol_list_edit_xx(int *i, int nb_elements,
                              int Bg, int Urs_p, int flags) {
    PipList_xx * list, * list_new, * list_now ;

    /* Pour le premier element. */
    list = (PipList_xx *)malloc(sizeof(PipList_xx)) ;
    if (list == NULL) {
        fprintf(stderr, "Memory Overflow.\n") ;
        exit(1) ;
    }
    list->next = NULL ;

    if (nb_elements == 0) {
        list->vector = NULL ;
        return(list) ;
    }

    list->vector = sol_vector_edit_xx(i, Bg, Urs_p, flags);

    list_now = list ;
    if (verbose_xx > 0) {
        fprintf(dump_xx,"\n(list ") ;
        pip_vector_print_xx(dump_xx,list->vector) ;
    }
    nb_elements-- ;

    /* Pour les elements suivants. */
    while (nb_elements--) {
        list_new = (PipList_xx *)malloc(sizeof(PipList_xx)) ;
        if (list_new == NULL) {
            fprintf(stderr, "Memory Overflow.\n") ;
            exit(1) ;
        }
        list_new->vector = sol_vector_edit_xx(i, Bg, Urs_p, flags);
        list_new->next = NULL ;

        if (verbose_xx > 0) {
            fprintf(dump_xx,"\n") ;
            pip_vector_print_xx(dump_xx,list_new->vector) ;
        }
        list_now->next = list_new ;
        list_now = list_now->next ;
    }
    if (verbose_xx > 0)
        fprintf(dump_xx,"\n)") ;

    return(list) ;
}


/* Fonction sol_quast_edit :
 * Cette fonction a pour but de placer les informations de la solution
 * (qui sont contenues dans le tableau sol_space) dans une structure de
 * type PipQuast en vue d'une utilisation directe de la solution par une
 * application exterieure. Elle prend en parametre un pointeur vers une
 * case memoire contenant le numero de cellule du tableau sol_space
 * a partir de laquelle on doit commencer la lecture des informations. Elle
 * recoit aussi l'adresse du PipQuast qui l'a appelle (pour le champ father).
 * Elle retourne un pointeur vers une structure de type PipQuast qui
 * contient toutes les informations sur la solution (sous forme d'arbre).
 * Remarques : cette fonction lit les informations comme elles doivent
 * se presenter a la fin du traitement. Elle respecte scrupuleusement
 * la grammaire attendue et n'accepte de passer des cellules a Free
 * qu'entre une des trois grandes formes (if, list ou suite de newparm).
 * 20  juillet 2001 : Premiere version, Ced.
 * 31  juillet 2001 : Ajout du traitement de l'option verbose = code*2 :0(
 * 18  octobre 2001 : Grands changements dus a l'eclatement de la structure
 *                    PipVector en PipVector, PipNewparm et PipList, et
 *                    eclatement de la fonction avec sol_newparm_edit et
 *                    sol_list_edit.
 * 16 novembre 2005 : (debug) Même si une liste est vide il faut la créer pour
 *                    afficher plus tard le (list), repéré par Sven Verdoolaege.
 */
PipQuast_xx *sol_quast_edit_xx(
    int *i, PipQuast_xx *father, int Bg, int Urs_p, int flags) {
    int nb_elements ;
    struct S_xx * p ;
    PipQuast_xx * solution ;

    /* On place p au lieu de lecture. */
    p = sol_space_xx + (*i) ;
    /* En cas d'utilisation de l'option de simplification, une plage de
     * structures S peut avoir les flags a Free. On doit alors les passer.
     */
    while (p->flags == Free) {
        p ++ ;
        (*i) ++ ;
    }

    solution = (PipQuast_xx *)malloc(sizeof(PipQuast_xx)) ;
    if (solution == NULL) {
        fprintf(stderr, "Memory Overflow.\n") ;
        exit(1) ;
    }
    solution->newparm = NULL ;
    solution->list = NULL ;
    solution->condition = NULL ;
    solution->next_then = NULL ;
    solution->next_else = NULL ;
    solution->father = father ;

    /* On peut commencer par une chaine de nouveaux parametres... */
    if (p->flags == New) {
        solution->newparm = sol_newparm_edit_xx(i, Bg, Urs_p,
                                                flags & SOL_REMOVE);
        p = sol_space_xx + (*i) ;
    }

    /* ...ensuite soit par une liste (vide ou non) soit par un if. */
    (*i)++ ; /* Factorise de List, Nil et If. */
    switch (p->flags) {
    case List :
        nb_elements = piplib_int_get_si(p->param1) ;
        solution->list = sol_list_edit_xx(i, nb_elements,
                                          Bg, Urs_p, flags);
        if (flags & SOL_DUAL)
            solution->next_then = sol_quast_edit_xx(i, solution,
                                                    Bg, Urs_p, 0);
        break ;
    case Nil  :
        if (verbose_xx > 0)
            fprintf(dump_xx,"\n()") ;
        break ;
    case If   :
        solution->condition =
            sol_vector_edit_xx(i, Bg, Urs_p, flags & SOL_REMOVE);
        if (verbose_xx > 0) {
            fprintf(dump_xx,"\n(if ") ;
            pip_vector_print_xx(
                dump_xx,solution->condition) ;
        }
        solution->next_then = sol_quast_edit_xx(i, solution,
                                                Bg, Urs_p, flags);
        solution->next_else = sol_quast_edit_xx(i, solution,
                                                Bg, Urs_p, flags);
        if (verbose_xx > 0)
            fprintf(dump_xx,"\n)") ;
        break ;
    default   :
        fprintf(stderr,"\nAie !!! Flag %d inattendu.\n",p->flags) ;
        if (verbose_xx > 0)
            fprintf(dump_xx,"\nAie !!! Flag %d inattendu.\n",p->flags) ;
        exit(1) ;
    }

    return(solution) ;
}
