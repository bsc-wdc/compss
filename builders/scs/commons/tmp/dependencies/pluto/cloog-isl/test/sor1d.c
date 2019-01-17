/* Generated from ./sor1d.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.13s. */
if ((M >= 1) && (N >= 3)) {
    for (glT1=-1; glT1<=floord(3*M+N-5,100); glT1++) {
        for (rp1=max(max(0,ceild(100*glT1-2*M-N+5,100)),ceild(100*glT1-N-193,300)); rp1<=min(min(floord(glT1+1,3),floord(M,100)),glT1); rp1++) {
            for (vT1=max(max(100*glT1-100*rp1,200*rp1-3),200*rp1-N+1); vT1<=min(min(min(2*M+N-5,100*glT1-100*rp1+99),200*rp1+N+193),100*glT1-100*rp1+N+95); vT1++) {
                if (rp1 >= max(1,ceild(vT1-N+7,200))) {
                    S3((glT1-rp1),(rp1-1),rp1,(100*rp1-1),(-200*rp1+vT1+6));
                }
                for (vP1=max(max(1,ceild(vT1-N+5,2)),100*rp1); vP1<=min(min(floord(vT1+2,2),M),100*rp1+99); vP1++) {
                    S1((glT1-rp1),rp1,vP1,(vT1-2*vP1+4));
                }
                if (rp1 <= min(floord(M-100,100),floord(vT1-197,200))) {
                    S2((glT1-rp1),rp1,(rp1+1),(100*rp1+99),(-200*rp1+vT1-194));
                }
            }
        }
        S4(glT1);
    }
}
