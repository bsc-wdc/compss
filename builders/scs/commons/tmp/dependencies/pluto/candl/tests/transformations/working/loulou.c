void main() {
    int i, j, n, m;
    float a[100], b[100] ;

#pragma scop
    /* Clay
      split(S0, 1);
      interchange([1,0], 1, 2);
    */

    /* Esced kernel */
    for (i=1; i<=n; i++) {   /* Loop label: i=1. */
        a[i] = i ;             /* Array labels: a=1, i=2. */
        for (j=1; j<=m; j++)   /* Loop label: j=2. */
            b[j] = b[j] + a[i] ; /* Array labels: b=3, a=1. */
    }
#pragma endscop
}
