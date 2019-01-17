/* Generated from ../../../git/cloog/test/backtrack.cloog by CLooG 0.14.0-367-gf043665 gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i) { hash(1); hash(i); }

void test() {
    /* Scattering iterators. */
    int c1;
    /* Original iterators. */
    int i;
    S1(0);
}
