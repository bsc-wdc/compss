#include <stdlib.h>
#include "../include/cloog/cloog.h"

#define ALLOC(type) (type*)malloc(sizeof(type))
#define ALLOCN(type,n) (type*)malloc((n)*sizeof(type))

#if defined(CLOOG_INT_INT) || \
    defined(CLOOG_INT_LONG) || \
    defined(CLOOG_INT_LONG_LONG)

cloog_int_t cloog_gcd(cloog_int_t a, cloog_int_t b) {
    while (a) {
        cloog_int_t t = b % a;
        b = a;
        a = t;
    }
    if (b < 0)
        b = -b;
    return b;
}

#endif

struct cloog_vec *cloog_vec_alloc(unsigned size) {
    int i;
    struct cloog_vec *vec;

    vec = ALLOC(struct cloog_vec);
    if (!vec)
        return NULL;

    vec->p = ALLOCN(cloog_int_t, size);
    if (!vec->p)
        goto error;
    vec->size = size;

    for (i = 0; i < size; ++i)
        cloog_int_init(vec->p[i]);

    return vec;
error:
    free(vec);
    return NULL;
}

void cloog_vec_free(struct cloog_vec *vec) {
    int i;

    if (!vec)
        return;

    for (i = 0; i < vec->size; ++i)
        cloog_int_clear(vec->p[i]);
    free(vec->p);
    free(vec);
}

void cloog_vec_dump(struct cloog_vec *vec) {
    int i;

    for (i = 0; i < vec->size; ++i) {
        cloog_int_print(stderr, vec->p[i]);
        fprintf(stderr, " ");
    }
    fprintf(stderr, "\n");
}

int cloog_seq_first_non_zero(cloog_int_t *p, unsigned len) {
    int i;

    for (i = 0; i < len; ++i)
        if (!cloog_int_is_zero(p[i]))
            return i;
    return -1;
}

void cloog_seq_neg(cloog_int_t *dst, cloog_int_t *src, unsigned len) {
    int i;
    for (i = 0; i < len; ++i)
        cloog_int_neg(dst[i], src[i]);
}

void cloog_seq_cpy(cloog_int_t *dst, cloog_int_t *src, unsigned len) {
    int i;
    for (i = 0; i < len; ++i)
        cloog_int_set(dst[i], src[i]);
}

static void cloog_seq_scale_down(cloog_int_t *dst, cloog_int_t *src, cloog_int_t m, unsigned len) {
    int i;
    for (i = 0; i < len; ++i)
        cloog_int_divexact(dst[i], src[i], m);
}

void cloog_seq_combine(cloog_int_t *dst, cloog_int_t m1, cloog_int_t *src1,
                       cloog_int_t m2, cloog_int_t *src2, unsigned len) {
    int i;
    cloog_int_t tmp;

    cloog_int_init(tmp);
    for (i = 0; i < len; ++i) {
        cloog_int_mul(tmp, m1, src1[i]);
        cloog_int_addmul(tmp, m2, src2[i]);
        cloog_int_set(dst[i], tmp);
    }
    cloog_int_clear(tmp);
}

static int cloog_seq_abs_min_non_zero(cloog_int_t *p, unsigned len) {
    int i, min = cloog_seq_first_non_zero(p, len);
    if (min < 0)
        return -1;
    for (i = min + 1; i < len; ++i) {
        if (cloog_int_is_zero(p[i]))
            continue;
        if (cloog_int_abs_lt(p[i], p[min]))
            min = i;
    }
    return min;
}

void cloog_seq_gcd(cloog_int_t *p, unsigned len, cloog_int_t *gcd) {
    int i, min = cloog_seq_abs_min_non_zero(p, len);

    if (min < 0) {
        cloog_int_set_si(*gcd, 0);
        return;
    }
    cloog_int_abs(*gcd, p[min]);
    for (i = 0; cloog_int_cmp_si(*gcd, 1) > 0 && i < len; ++i) {
        if (i == min)
            continue;
        if (cloog_int_is_zero(p[i]))
            continue;
        cloog_int_gcd(*gcd, *gcd, p[i]);
    }
}

int cloog_seq_is_neg(cloog_int_t *p1, cloog_int_t *p2, unsigned len) {
    int i;

    for (i = 0; i < len; ++i) {
        if (cloog_int_abs_ne(p1[i], p2[i]))
            return 0;
        if (cloog_int_is_zero(p1[i]))
            continue;
        if (cloog_int_eq(p1[i], p2[i]))
            return 0;
    }
    return 1;
}

void cloog_seq_normalize(cloog_int_t *p, unsigned len) {
    cloog_int_t gcd;

    if (len == 0)
        return;

    cloog_int_init(gcd);
    cloog_seq_gcd(p, len, &gcd);
    if (!cloog_int_is_zero(gcd) && !cloog_int_is_one(gcd))
        cloog_seq_scale_down(p, p, gcd, len);
    cloog_int_clear(gcd);
}
