/* matmul.c  128*128 matrix multiply */
#pragma scop
for(i=0; i<N; i++)
    for(j=0; j<N; j++) {
        c[i][j] = 0.0;
        for(k=0; k<N; k++)
            c[i][j] = c[i][j] + a[i][k]*b[k][j];
    }
#pragma endscop
