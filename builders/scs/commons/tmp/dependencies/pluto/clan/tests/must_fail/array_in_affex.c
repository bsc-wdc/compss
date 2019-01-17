/*
 * JJ has to be considered as an array and is illegal in S2.
 * Reported by David Wonnacott and Uday Bondhugula.
 */
#pragma scop
for (N_MINUS_J=N-(N-1); N_MINUS_J<=N-1; N_MINUS_J++) {
    JJ=N - N_MINUS_J;
    for (I=1; I<N-1; I++) {
        RX[JJ][I] = (RX[JJ][I]-AA[JJ][I]*RX[JJ+1][I])*D[JJ][I];
    }
}
#pragma endscop

