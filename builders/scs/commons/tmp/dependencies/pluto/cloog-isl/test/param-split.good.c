/* Generated from ../../../git/cloog/test/param-split.cloog by CLooG 0.14.0-277-gce2ba57 gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i) { hash(1); hash(i); }
#define S2(i) { hash(2); hash(i); }

void test(int M) {
    /* Original iterators. */
    int i;
    if (M >= 0) {
        S1(0);
        S2(0);
    }
    for (i=1; i<=M; i++) {
        S1(i);
    }
    if (M <= -1) {
        S2(0);
    }
}
