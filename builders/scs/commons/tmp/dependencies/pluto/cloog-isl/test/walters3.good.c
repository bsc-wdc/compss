/* Generated from ../../../git/cloog/test/walters3.cloog by CLooG 0.14.0-338-g99c7504 gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(j,a,b) { hash(1); hash(j); hash(a); hash(b); }
#define S2(j,a,b) { hash(2); hash(j); hash(a); hash(b); }

void test() {
    /* Original iterators. */
    int j, a, b;
    for (j=2; j<=8; j++) {
        if (j%2 == 0) {
            S1(j,j/2,j/2);
            S2(j,j/2,j/2);
        }
    }
    S2(10,5,5);
}
