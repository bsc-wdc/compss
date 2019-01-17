/* Generated from ../../../git/cloog/test/classen2.cloog by CLooG 0.14.0-271-gaa1e292 gmp bits in 0.13s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(compIter1,compIter2,compIter3,compIter4,compIter5,compIter6) { hash(1); hash(compIter1); hash(compIter2); hash(compIter3); hash(compIter4); hash(compIter5); hash(compIter6); }

void test(int outerTimeTileScatter, int outerProcTileScatter1, int outerProcTileScatter2, int M, int N) {
    /* Scattering iterators. */
    int compScatter1, compScatter2, compScatter3;
    /* Original iterators. */
    int compIter1, compIter2, compIter3, compIter4, compIter5, compIter6;
    if ((M >= 2) && (N >= 3) && (outerProcTileScatter1 >= outerProcTileScatter2) && (5*outerProcTileScatter1 <= M+2*N-4) && (5*outerProcTileScatter1 <= 5*outerProcTileScatter2+N+2) && (outerProcTileScatter2 >= 0) && (5*outerProcTileScatter2 <= M+N-2) && (outerTimeTileScatter >= outerProcTileScatter1) && (outerTimeTileScatter <= 2*outerProcTileScatter1) && (outerTimeTileScatter <= outerProcTileScatter1+outerProcTileScatter2+1) && (5*outerTimeTileScatter <= 2*M+2*N-6) && (5*outerTimeTileScatter <= 5*outerProcTileScatter1+M+2) && (5*outerTimeTileScatter >= 10*outerProcTileScatter1-2*N-2) && (5*outerTimeTileScatter <= 5*outerProcTileScatter2+M+N) && (5*outerTimeTileScatter >= 10*outerProcTileScatter2-N-3) && (5*outerTimeTileScatter <= 10*outerProcTileScatter2+N+3) && (5*outerTimeTileScatter >= 5*outerProcTileScatter1+5*outerProcTileScatter2-N-4)) {
        for (compScatter1=max(max(max(max(max(4,5*outerTimeTileScatter),5*outerProcTileScatter2+1),5*outerProcTileScatter1+5*outerProcTileScatter2-N),10*outerProcTileScatter1-2*N+2),10*outerProcTileScatter2-N+1); compScatter1<=min(min(min(min(min(5*outerTimeTileScatter+4,2*M+2*N-6),5*outerProcTileScatter1+M+2),5*outerProcTileScatter1+5*outerProcTileScatter2+5),5*outerProcTileScatter2+M+N),10*outerProcTileScatter2+N+3); compScatter1++) {
            for (compScatter2=max(max(max(max(ceild(compScatter1+4,2),5*outerProcTileScatter1),5*outerProcTileScatter2+1),compScatter1-M+2),compScatter1-5*outerProcTileScatter2-1); compScatter2<=min(min(min(min(floord(compScatter1+2*N-2,2),compScatter1),5*outerProcTileScatter1+4),compScatter1-5*outerProcTileScatter2+N),5*outerProcTileScatter2+N+2); compScatter2++) {
                for (compScatter3=max(max(5*outerProcTileScatter2,compScatter1-compScatter2+3),compScatter2-N+2); compScatter3<=min(min(compScatter2-1,5*outerProcTileScatter2+4),compScatter1-compScatter2+N); compScatter3++) {
                    S1(compScatter1-compScatter2+1,-compScatter1+compScatter2+compScatter3-2,compScatter2-compScatter3,compScatter1,compScatter2,compScatter3);
                }
            }
        }
    }
}
