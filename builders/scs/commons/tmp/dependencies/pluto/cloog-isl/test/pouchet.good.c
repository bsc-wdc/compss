/* Generated from ../../../git/cloog/test/pouchet.cloog by CLooG 0.16.2-3-gc1aebd7 gmp bits in 0.04s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i,j,k,l,m) { hash(1); hash(i); hash(j); hash(k); hash(l); hash(m); }
#define S2(i,j,k,l,m) { hash(2); hash(i); hash(j); hash(k); hash(l); hash(m); }

void test(int Ny) {
    /* Scattering iterators. */
    int c0, c1, c2, c3, c4, c5;
    /* Original iterators. */
    int i, j, k, l, m;
    if (Ny >= 2) {
        for (c0=1; c0<=floord(Ny+4,2); c0++) {
            for (c1=max(ceild(c0+1,2),c0-1); c1<=min(floord(2*c0+Ny,4),c0); c1++) {
                if (c0 >= ceild(4*c1-Ny+1,2)) {
                    for (c2=1; c2<=2; c2++) {
                        S1(c0-c1,c1,2*c0-2*c1,-2*c0+4*c1,c2);
                        S2(c0-c1,c1,2*c0-2*c1,-2*c0+4*c1-1,c2);
                    }
                }
                if (2*c0 == 4*c1-Ny) {
                    for (c2=1; c2<=2; c2++) {
                        if (Ny%2 == 0) {
                            if ((2*c0+3*Ny)%4 == 0) {
                                S2((2*c0-Ny)/4,(2*c0+Ny)/4,(2*c0-Ny)/2,Ny-1,c2);
                            }
                        }
                    }
                }
            }
        }
    }
}
