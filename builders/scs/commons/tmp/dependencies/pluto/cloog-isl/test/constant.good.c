/* Generated from ../../../git/cloog/test/constant.cloog by CLooG 0.14.0-333-g4442dac gmp bits in 0.01s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i) { hash(1); hash(i); }
#define S2(i) { hash(2); hash(i); }
#define S3(i) { hash(3); hash(i); }
#define S4(i) { hash(4); hash(i); }
#define S5(i) { hash(5); hash(i); }
#define S6(i) { hash(6); hash(i); }

void test(int M) {
    /* Scattering iterators. */
    int c1, c2;
    /* Original iterators. */
    int i;
    for (c2=0; c2<=min(1023,M+1024); c2++) {
        S1(c2);
        S3(c2);
    }
    for (c2=max(0,M+1025); c2<=1023; c2++) {
        S2(c2);
        S3(c2);
    }
    for (c1=0; c1<=min(1023,M+1024); c1++) {
        S4(c1);
        S6(c1);
    }
    for (c1=max(0,M+1025); c1<=1023; c1++) {
        S5(c1);
        S6(c1);
    }
}
