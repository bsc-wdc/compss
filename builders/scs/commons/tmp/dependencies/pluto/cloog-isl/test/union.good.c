/* Generated from ../../../git/cloog/test/union.cloog by CLooG 0.14.0-277-g62f7d82 gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i) { hash(1); hash(i); }

void test(int M) {
    /* Scattering iterators. */
    int c1;
    /* Original iterators. */
    int i;
    if (M <= -1) {
        for (c1=0; c1<=100; c1++) {
            S1(c1);
        }
    }
    if (M >= 1) {
        if (M >= 11) {
            for (c1=-100; c1<=0; c1++) {
                S1(-c1);
            }
        }
        if (M <= 10) {
            for (c1=0; c1<=100; c1++) {
                S1(c1);
            }
        }
    }
}
