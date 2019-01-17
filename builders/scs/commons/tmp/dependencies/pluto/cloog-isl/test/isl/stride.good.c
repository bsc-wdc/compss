/* Generated from ../../../src/cloog/test/isl/stride.cloog by CLooG 0.17.0 gmp bits in 0.29s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i,j,k,l) { hash(1); hash(i); hash(j); hash(k); hash(l); }

void test(int M) {
    /* Scattering iterators. */
    int c1, c2, c3, c4;
    /* Original iterators. */
    int i, j, k, l;
    if (M >= 3) {
        for (c1=-1; c1<=min(2,floord(M+2,4)); c1++) {
            for (c2=max(ceild(2*c1-M+1,4),ceild(4*c1-M-2,4)); c2<=min(0,floord(c1,2)); c2++) {
                for (c3=max(max(-4*c2-2,4*c2+3),4*c1-4*c2+1); c3<=min(min(min(M+3,-4*c2+9),4*c2+2*M),4*c1-4*c2+4); c3++) {
                    for (c4=max(3*c3-4*floord(c3+M+1,2)+6,4*c2-c3-4*floord(-c3+1,4)+2); c4<=min(min(4*c2+4,-c3+10),c3-2); c4+=4) {
                        if ((c2 <= floord(c4-1,4)) && (c2 >= ceild(c4-4,4))) {
                            S1(c1-c2,c2,(c3+c4-2)/4,(c3-c4)/2);
                        }
                    }
                }
            }
        }
    }
}
