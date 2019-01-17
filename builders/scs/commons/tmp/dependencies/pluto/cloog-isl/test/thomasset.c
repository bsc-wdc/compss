/* Generated from ./thomasset.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.02s. */
if (n >= 1) {
    for (c1=0; c1<=floord(n-4,3); c1++) {
        for (i=3*c1+1; i<=3*c1+3; i++) {
            S1(i,c1);
        }
    }
    c1 = floord(n-1,3);
    if (c1 >= ceild(n-2,3)) {
        if (c1 == 0) {
            S1(1,0);
            for (j=1; j<=n; j++) {
                S2(1,j,0,0,0);
            }
        }
        if (c1 >= 1) {
            for (j=1; j<=2; j++) {
                S2(1,j,0,c1,0);
            }
        }
        for (i=max(2,3*c1+1); i<=n; i++) {
            S1(i,c1);
        }
    }
    if (3*c1 == n-3) {
        for (i=n-2; i<=n; i++) {
            if (n%3 == 0) {
                S1(i,((n-3)/3));
            }
        }
    }
    if (c1 >= ceild(n-2,3)) {
        for (c2=1; c2<=n-1; c2++) {
            for (j=1; j<=2; j++) {
                S2((c2+1),j,0,c1,0);
            }
        }
    }
    for (c1=ceild(n,3); c1<=floord(2*n,3); c1++) {
        for (c2=0; c2<=n-1; c2++) {
            for (j=max(1,3*c1-n); j<=min(n,3*c1-n+4); j++) {
                p = max(ceild(3*c1-j,3),ceild(n-2,3));
                if (p <= min(floord(n,3),floord(3*c1-j+2,3))) {
                    S2((c2+1),j,0,p,(c1-p));
                }
            }
        }
    }
}
