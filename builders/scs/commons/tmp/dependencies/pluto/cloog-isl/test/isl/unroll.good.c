/* Generated from ./isl/unroll.cloog by CLooG 0.16.3-14-g80e4ef0 gmp bits in 0.00s. */
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
    S1(0);
    S1(1);
    S1(2);
    S1(3);
    S1(4);
    S1(5);
    S1(6);
    S1(7);
    S1(8);
    S1(9);
    S1(10);
}
