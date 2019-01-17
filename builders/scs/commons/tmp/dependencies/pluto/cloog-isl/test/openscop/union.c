/* Generated from ./openscop/union.scop by CLooG 0.18.3 gmp bits in 0.01s. */
/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#ifdef TIME
#define IF_TIME(foo) foo;
#else
#define IF_TIME(foo)
#endif

/* Scattering iterators. */
int c2, c4;
/* Original iterators. */
int i, j;

if (N >= 1) {
    for (c2=0; c2<=N-1; c2++) {
        for (c4=0; c4<=min(10,N-1); c4++) {
            C[c2+c4]+=A[c2]*B[c4];
        }
        for (c4=20; c4<=N-1; c4++) {
            C[c2+c4]+=A[c2]*B[c4];
        }
    }
}
