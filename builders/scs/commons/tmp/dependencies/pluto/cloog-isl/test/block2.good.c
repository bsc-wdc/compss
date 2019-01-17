/* Generated from /home/skimo/git/cloog/test/block2.cloog by CLooG 0.14.0-302-g309b32c gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i,j) { hash(1); hash(i); hash(j); }
#define S2(i,j) { hash(2); hash(i); hash(j); }
#define S3(i,j) { hash(3); hash(i); hash(j); }

void test() {
    /* Scattering iterators. */
    int c0, c1;
    /* Original iterators. */
    int i, j;
    for (c0=0; c0<=9; c0++) {
        S1(c0,1);
        S3(c0,1);
        S2(c0,1);
    }
}
