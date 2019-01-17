void main() {
    int i, j, n;
    float a[100][100], b[100], c[100] ;
#pragma scop
    /* ax-do kernel */
    for (i=1; i<=n; i++)         /* Loop label: i=1. */
        c[i] = 0 ;                 /* Array label: c=1. */

    for (i=1; i<=n; i++)         /* Loop label: i=2. */
        for (j=1; j<=n; j++)       /* Loop label: j=3. */
            c[i] += a[i][j] * b[j] ; /* Array labels: c=1, a=2, b=3. */
#pragma endscop
}

