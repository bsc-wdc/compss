#ifndef CLOOG_UNION_DOMAIN_H
#define CLOOG_UNION_DOMAIN_H

#if defined(__cplusplus)
extern "C" {
#endif 

struct osl_scop;

/**
 * CloogNamedDomainList structure:
 * this structure reprensents a node of a linked list of CloogDomain structures.
 */
struct cloognameddomainlist {
  CloogDomain *domain;              /**< An element of the list. */
  CloogScattering *scattering;      /**< Scattering function for domain. */
  char *name;                       /**< Name of the domain. */
  void *usr;                        /**< A pointer for library user's convenience. */
  struct cloognameddomainlist *next;/**< Pointer to the next element of the list.*/
};
typedef struct cloognameddomainlist CloogNamedDomainList;

/**
 * A structure representing the input domains and scattering functions.
 */
struct clooguniondomain {
	int n_name[3];
	char **name[3];
	CloogNamedDomainList *domain;
	CloogNamedDomainList **next_domain;
};
typedef struct clooguniondomain CloogUnionDomain;

enum cloog_dim_type { CLOOG_PARAM, CLOOG_ITER, CLOOG_SCAT };

CloogUnionDomain *cloog_union_domain_read(FILE *file, int nb_par,
	CloogOptions *options);
CloogUnionDomain *cloog_union_domain_alloc(int nb_par);
CloogUnionDomain *cloog_union_domain_add_domain(CloogUnionDomain *ud,
	const char *name, CloogDomain *domain, CloogScattering *scattering,
	void *usr);
CloogUnionDomain *cloog_union_domain_set_name(CloogUnionDomain *ud,
	enum cloog_dim_type type, int index, const char *name);
void cloog_union_domain_free(CloogUnionDomain *ud);
CloogUnionDomain *cloog_union_domain_from_osl_scop(CloogState *,
                                                   struct osl_scop *);

#if defined(__cplusplus)
}
#endif 

#endif
