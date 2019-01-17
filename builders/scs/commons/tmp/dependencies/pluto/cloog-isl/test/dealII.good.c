/* Generated from ../../../git/cloog/test/dealII.cloog by CLooG 0.14.0-270-g7ee1261 gmp bits in 0.01s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(scat_0) { hash(1); hash(scat_0); }
#define S2(scat_0) { hash(2); hash(scat_0); }

void test(int T_2, int T_67, int T_66) {
    /* Original iterators. */
    int scat_0;
    for (scat_0=0; scat_0<=min(T_66,T_2-1); scat_0++) {
        S1(scat_0);
        S2(scat_0);
    }
    if ((T_2 == 0) && (T_67 == 0)) {
        S1(0);
    }
    for (scat_0=max(0,T_66+1); scat_0<=T_2-1; scat_0++) {
        S1(scat_0);
    }
    for (scat_0=T_2; scat_0<=min(T_66,T_67-1); scat_0++) {
        S2(scat_0);
    }
}
