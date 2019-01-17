#pragma scop
for(t = 0 ; t < T ; t++) {
    for(i = 2 ; i <= N+2 ; i++)
        b[i] = 0.33333 * (a[i-1] + a[i] + a[i+1]);
    for(j = 2 ; j <= N+2 ; j++)
        a[j] = b[j];
}
#pragma endscop
