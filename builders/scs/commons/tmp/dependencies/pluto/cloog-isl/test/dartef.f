! Generated from ../test/dartef.cloog by CLooG 0.18.4-dac762f gmp bits in 0.06s.
IF (n >= 1) THEN
  DO t3=n+3, 3*n+1
    IF (MOD(t3+n+1, 2) == 0) THEN
      S1(1,n,((t3-n-1)/2))
    END IF
  END DO
  DO t1=-n+2, n-1
    IF (t1 >= 0) THEN
      DO t3=t1+4, t1+2*n+2
        IF (MOD(t1+t3, 2) == 0) THEN
          S1((t1+1),1,((-t1+t3-2)/2))
        END IF
      END DO
    END IF
    DO t2=MAX(-t1+2,t1+3), -t1+4
      DO t3=t2+2, t2+2*n
        IF (MOD(t1+t2, 2) == 0) THEN
          IF (MOD(t2+t3, 2) == 0) THEN
            S1(((t1+t2)/2),((-t1+t2)/2),((-t2+t3)/2))
          END IF
        END IF
      END DO
    END DO
    DO t2=MAX(-t1+5,t1+3), MIN(-t1+2*n,t1+2*n)
      DO t3=1, MIN(n,t2+1)
        IF (MOD(t1+t2+1, 2) == 0) THEN
          S2(((t1+t2-3)/2),((-t1+t2-1)/2),t3)
        END IF
      END DO
      DO t3=t2+2, n
        IF (MOD(t1+t2+1, 2) == 0) THEN
          S2(((t1+t2-3)/2),((-t1+t2-1)/2),t3)
        END IF
        IF (MOD(t1+t2, 2) == 0) THEN
          IF (MOD(t2+t3, 2) == 0) THEN
            S1(((t1+t2)/2),((-t1+t2)/2),((-t2+t3)/2))
          END IF
        END IF
      END DO
      DO t3=MAX(n+1,t2+2), t2+2*n
        IF (MOD(t1+t2, 2) == 0) THEN
          IF (MOD(t2+t3, 2) == 0) THEN
            S1(((t1+t2)/2),((-t1+t2)/2),((-t2+t3)/2))
          END IF
        END IF
      END DO
    END DO
    IF (t1 <= -1) THEN
      DO t3=1, n
        S2((t1+n-1),n,t3)
      END DO
    END IF
    DO t2=-t1+2*n+1, MIN(-t1+2*n+3,t1+2*n+1)
      DO t3=1, n
        IF (MOD(t1+t2+1, 2) == 0) THEN
          S2(((t1+t2-3)/2),((-t1+t2-1)/2),t3)
        END IF
      END DO
    END DO
  END DO
  DO t3=1, n
    S2(n,1,t3)
  END DO
END IF
