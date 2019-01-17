/* Generated from /home/skimo/git/cloog/test/sor1d.cloog by CLooG 0.14.0-226-g3fc65ac gmp bits in 0.04s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(tileT1,tileP1,other1,other2) { hash(1); hash(tileT1); hash(tileP1); hash(other1); hash(other2); }
#define S2(tileT1,tileP1,other1,other2,other3) { hash(2); hash(tileT1); hash(tileP1); hash(other1); hash(other2); hash(other3); }
#define S3(tileT1,tileP1,other1,other2,other3) { hash(3); hash(tileT1); hash(tileP1); hash(other1); hash(other2); hash(other3); }
#define S4(tileT1) { hash(4); hash(tileT1); }

void test(int M, int N) {
    /* Scattering iterators. */
    int glT1, rp1, vT1, vP1, otherP1, arrAcc1;
    /* Original iterators. */
    int tileT1, tileP1, other1, other2, other3;
    if ((M >= 1) && (N >= 3)) {
        for (glT1=-1; glT1<=floord(3*M+N-5,100); glT1++) {
            for (rp1=max(max(0,ceild(100*glT1-2*M-N+5,100)),ceild(100*glT1-N-193,300)); rp1<=min(min(min(min(floord(glT1+1,2),floord(M,100)),floord(100*glT1+99,100)),floord(50*glT1+51,150)),floord(100*glT1+N+98,300)); rp1++) {
                for (vT1=max(max(max(max(0,100*glT1-100*rp1),100*rp1-1),200*rp1-3),200*rp1-N+1); vT1<=min(min(2*M+N-5,100*glT1-100*rp1+99),200*rp1+N+193); vT1++) {
                    if (rp1 >= max(1,ceild(vT1-N+7,200))) {
                        S3(glT1-rp1,rp1-1,rp1,100*rp1-1,-200*rp1+vT1+6);
                    }
                    for (vP1=max(max(1,ceild(vT1-N+5,2)),100*rp1); vP1<=min(min(floord(vT1+2,2),M),100*rp1+99); vP1++) {
                        S1(glT1-rp1,rp1,vP1,vT1-2*vP1+4);
                        if ((rp1 <= min(floord(M-100,100),floord(vT1-197,200))) && (100*rp1 == vP1-99)) {
                            S2(glT1-rp1,rp1,rp1+1,100*rp1+99,-200*rp1+vT1-194);
                        }
                    }
                }
            }
            S4(glT1);
        }
    }
}
