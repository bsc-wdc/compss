/* Generated from ../../../git/cloog/test/isl/unroll2.cloog by CLooG 0.16.3-13-g27516e4 gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i) { hash(1); hash(i); }

void test(int M) {
    /* Original iterators. */
    int i;
    if ((M >= -1) && (M <= 9)) {
        for (i=max(0,M); i<=M+1; i++) {
            S1(i);
        }
    }
}
