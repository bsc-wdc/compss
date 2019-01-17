/* Generated from /home/skimo/git/cloog/test/walters2.cloog by CLooG 0.14.0-227-g08f253a gmp bits in 0.00s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(j,i) { hash(1); hash(j); hash(i); }
#define S2(j,i) { hash(2); hash(j); hash(i); }

void test() {
    /* Original iterators. */
    int j, i;
    for (i=0; i<=51; i++) {
        S2(0,i);
    }
    for (j=1; j<=24; j++) {
        S2(j,0);
        for (i=1; i<=50; i++) {
            S1(j,i);
        }
        S2(j,51);
    }
    for (i=0; i<=51; i++) {
        if (i >= 0) {
            S2(25,i);
        }
    }
}
