/* Generated from ../../../git/cloog/test/isl/jacobi-shared.cloog by CLooG 0.16.2-19-gfcd8fdc gmp bits in 1.65s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i,j) { hash(1); hash(i); hash(j); }

void test(int T, int N, int h0, int b0, int b1, int g0, int g1, int g2, int g3, int g4, int t0, int t1) {
    /* Scattering iterators. */
    int c0, c1, c2, c3;
    /* Original iterators. */
    int i, j;
    if ((h0+1)%2 == 0) {
        if ((16*floord(g1+t0-3,16) >= -N+g1+t0+1) && (16*floord(N+15*g1+15*t0+15,16) >= 16*g1+15*t0+17) && (floord(t1-1,32) <= floord(g2+t1-3,32)) && (32*floord(t1-1,32) >= -N+g2+t1+1)) {
            for (c0=max(-16*floord(t0-1,16)+t0,-16*floord(g1+t0-3,16)+t0); c0<=min(32,N-g1-1); c0+=16) {
                c1 = -32*floord(t1-1,32)+t1;
                if (c1 <= 32) {
                    S1(c0+g1-1,c1+g2-1);
                }
            }
        }
    }
}
