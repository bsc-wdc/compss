/* Generated from ./pouchet.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.02s. */
if (Ny >= 2) {
    for (c0=1; c0<=floord(Ny+4,2); c0++) {
        for (c1=max(ceild(c0+1,2),c0-1); c1<=min(floord(2*c0+Ny,4),c0); c1++) {
            if (c0 >= ceild(4*c1-Ny+1,2)) {
                for (c2=1; c2<=2; c2++) {
                    S1((c0-c1),c1,(2*c0-2*c1),(-2*c0+4*c1),c2);
                    S2((c0-c1),c1,(2*c0-2*c1),(-2*c0+4*c1-1),c2);
                }
            }
            if (2*c0 == 4*c1-Ny) {
                for (c2=1; c2<=2; c2++) {
                    if (Ny%2 == 0) {
                        if ((2*c0+3*Ny)%4 == 0) {
                            S2(((2*c0-Ny)/4),((2*c0+Ny)/4),((2*c0-Ny)/2),(Ny-1),c2);
                        }
                    }
                }
            }
        }
    }
}
