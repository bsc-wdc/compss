void main() {
    float a[10][10];

    int K, n, j, k;

#pragma scop
    for(K=1; K<=n; K++) {
        for(j=K; j<=n; j++)
            for(k=1; k<=K-1; k++)
                a[K][j] -= a[K][k] * a[k][j];
        for(j=K+1; j<=n; j++) {
            for(k=1; k<=K-1; k++)
                a[j][K] -= a[j][k] * a[k][K];
            a[j][K] /= a[K][K];
        }
    }
#pragma endscop
}
