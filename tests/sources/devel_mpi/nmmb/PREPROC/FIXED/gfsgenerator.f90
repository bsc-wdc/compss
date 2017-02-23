!--------1---------2---------3---------4---------5---------6---------7--
program gfsgenerator
!-----------------------------------------------------------------------
implicit none
!--nmm variables--------------------------------------------------------
include 'modelgrid.inc'
!
integer(kind=4):: &
 kflip,l,lpt2

real(kind=4) &
 pdtop,pt2

real(kind=4),dimension(1:10):: &
 xold,dold,y2s,pps,qqs

real(kind=4),dimension(1:lm):: &
 dsg,dsg1,dsg2,sgml1,sgml2,xnew

real(kind=4),dimension(1:lm+1):: &
 a,b,sg1,sg2,sgm
!--gfs variables--------------------------------------------------------
integer levs,lupp,k
real pbot,psig,ppre,pupp,ptop,dpbot,dpsig,dppre,dpupp,dptop,pmin
real,allocatable:: ak(:),bk(:)
!-----------------------------------------------------------------------
!write(0,*) 'Enter levs,lupp,pbot,psig,ppre,pupp,ptop,dpbot,dpsig,dppre,dpupp,dptop'
!read *,levs,lupp,pbot,psig,ppre,pupp,ptop,dpbot,dpsig,dppre,dpupp,dptop
!-----------------------------------------------------------------------
      levs=lm ! # of layers
!-----------------------------------------------------------------------
      allocate(ak(0:levs),bk(0:levs))
!-----------------------------------------------------------------------
      lupp=levs
      pbot=100000.-pt ! surface pressure-pt
      psig=99400.-pt ! lowest interface above surface-pt
      ppre=ptsgm-pt ! pressure-hybrid transition point-pt
      pupp=0.
      ptop=0.
      dpbot=500.
      dpsig=1200.
      dppre=18000.
      dpupp=60.
      dptop=60.
!-----------------------------------------------------------------------
      call akbkgen &
      (levs,lupp,pbot,psig,ppre &
      ,pupp,ptop,dpbot,dpsig,dppre,dpupp,dptop &
      ,pmin,ak,bk)
!-----------------------------------------------------------------------
      write(0,*) 'pmin=',pmin
      print '(2i6)',2,levs
      print '(f12.3,f12.8)',(ak(k),bk(k),k=0,levs)
!--flip gfs's ak, bk into nmm's a, b------------------------------------
      do l=1,lm+1
        kflip=lm+1-l
        a(l)=ak(kflip)+pt ! to account for reduced pbot
        b(l)=bk(kflip)
      enddo
!--retrieve required nmm parameters-------------------------------------
      do l=1,lm+1
        if(b(l).gt.1.e-7) then
          exit
        else
          b(l)=0.
          lpt2=l
        endif
      enddo
!
      pt2=a(lpt2)
      pdtop=pt2-pt
      print*,'*** Mixed system starts at ',pt2,' Pa, from level ',lpt2
!
      do l=1,lm+1
        sgm(l)=(a(l)-pt+b(l)*(pbot-pt))/(pbot-pt) 
        print*, '   sgm(',l,')=',sgm(l)
      enddo
!--define NMM's sg1 and sg2---------------------------------------------
      do l=1,lm+1
        sg1(l)=(a(l)-pt)/pdtop
        sg2(l)=b(l)
      enddo
!--define NMM's dsg1, dsg2, sgml1, sgml2--------------------------------
      do l=1,lm
        dsg1(l)=sg1(l+1)-sg1(l)
        dsg2(l)=sg2(l+1)-sg2(l)
        sgml1(l)=(sg1(l)+sg1(l+1))*0.5
        sgml2(l)=(sg2(l)+sg2(l+1))*0.5
      enddo
!--print resulting vertical discretization------------------------------
      do l=1,lm+1
        print*, '   sg1(',l,')=',  sg1(l),'   sg2(',l,')=',  sg2(l)
      enddo
      do l=1,lm
        print*, '  dsg1(',l,')=', dsg1(l),'  dsg2(',l,')=', dsg2(l)
      enddo
      do l=1,lm
        print*, ' sgml1(',l,')=',sgml1(l),' sgml2(',l,')=',sgml2(l)
      enddo
!--test surface pressures-----------------------------------------------
      do l=1,lm+1
        print*,a(l),b(l),a(l)+b(l)*(100000.-pt)
      enddo
      print*,'midlayer pressures, 100000 pa'
      do l=1,lm
        print*,(a(l+1)+b(l+1)*(100000.-pt)+a(l)+b(l)*(100000.-pt))*0.5
      enddo
      print*,'layer thhicknesses, 100000 pa'
      do l=1,lm
        print*,a(l+1)+b(l+1)*(100000.-pt)-a(l)-b(l)*(100000.-pt)
      enddo
      print*,'midlayer pressures, 75000 pa'
      do l=1,lm
        print*,(a(l+1)+b(l+1)*(75000.-pt)+a(l)+b(l)*(75000.-pt))*0.5
      enddo
      print*,'layer thhicknesses, 75000 pa'
      do l=1,lm
        print*,a(l+1)+b(l+1)*(75000.-pt)-a(l)-b(l)*(75000.-pt)
      enddo
      print*,'midlayer pressures, 50000 pa'
      do l=1,lm
        print*,(a(l+1)+b(l+1)*(50000.-pt)+a(l)+b(l)*(50000.-pt))*0.5
      enddo
      print*,'layer thhicknesses, 50000 pa'
      do l=1,lm
        print*,a(l+1)+b(l+1)*(50000.-pt)-a(l)-b(l)*(50000.-pt)
      enddo
!-----------------------------------------------------------------------
      open(unit=1,file='../output/dsg' &
          ,status='unknown',form='unformatted')
      write(1) pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
      close(1)
!-----------------------------------------------------------------------
end program
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!$$$  Subprogram documentation block
!
! Subprogram:    akbkgen     Generates hybrid coordinate interface profiles
!   Prgmmr: Iredell    Org: W/NP23      Date: 2008-08-01
!
! Abstract: This subprogram generates hybrid coordinate interface profiles
!   from a few given parameters. The hybrid coordinate is intended to start
!   out at the bottom in pure sigma and end up at the top in pure pressure,
!   with a smooth transition in between. The pressure thickness is close to
!   quadratic in pressure, with maximum thicknesses in the middle of the domain.
!   The coordinate pressure will have continuous second derivatives in level.
!
!   The hybrid coordinate is returned in terms of vectors AK and BK, where
!   the interface pressure is defined as A+B*ps where ps is surface pressure
!   Thus A=0 in regions of pure sigma and B=0 in regions of pure sigma.
!   At the bottom, A(0)=0 and B(0)=1 so that surface pressure is the bottom
!   boundary condition, while at the top, A(levs)=ptop and B(levs)=0 so that
!   the constant top pressure (which can be zero) is the top boundary condition.
!
!   The procedure for the calculation is described in the remarks section below.
!
! Program history log:
!   2008-08-01  Mark Iredell
!
! Usage:    call akbkgen(levs,lupp,pbot,psig,ppre,pupp,ptop,&
!                        dpbot,dpsig,dppre,dpupp,dptop,pmin,ak,bk)
!   Input argument list:
!     levs     integer number of levels
!     lupp     integer number of levels below pupp
!     pbot     real nominal surface pressure (Pa)
!     psig     real nominal pressure where coordinate changes
!              from pure sigma (Pa)
!     ppre     real nominal pressure where coordinate changes
!              to pure pressure (Pa)
!     pupp     real nominal pressure where coordinate changes
!              to upper atmospheric profile (Pa)
!     ptop     real pressure at top (Pa)
!     dpbot    real coordinate thickness at bottom (Pa)
!     dpsig    real thickness of zone within which coordinate changes
!              to pure sigma (Pa)
!     dppre    real thickness of zone within which coordinate changes
!              to pure pressure (Pa)
!     dpupp    real coordinate thickness at pupp (Pa)
!     dptop    real coordinate thickness at top (Pa)
!   Output argument list:
!     pmin     real minimum surface pressure (Pa)
!     ak       real(0:levs) a coordinate values, bottom to top (Pa)
!     bk       real(0:levs) b coordinate values, bottom to top ()
!
! Subprograms called:
!     ludcmp   lower and upper triangular decomposition
!     lubksb   lower and upper triangular back substitution
!
! Attributes:
!   Language: Fortran 90
!
! Remarks:
!   Example: Create the operational GFS 64-level hybrid coordinate.
!      call akbkgen(64,100000.,500.,60.,7000.,18000.,99400.,1200.,pmin,ak,bk)
!
!   Graphical description of parameters and zones:
!     ptop  ---  -----  ----------------------
!           ...  dptop
!           ---         zone U (upper atmos)
!           ...
!     pupp  ---  -----  ----------------------
!           ...  dpupp
!           ---  -----
!           ...         zone P (pure pressure)
!           ---
!           ...
!     ppre  ---  -----  ----------------------
!           ...
!           ---  dppre  zone T1 (transition 1)
!           ...
!           ---  -----  ----------------------
!           ...
!           ---
!           ...         zone T2 (transition 2)
!           ---
!           ...
!           ---  -----  ----------------------
!           ...
!           ---  dpsig  zone T3 (transition 3)
!           ...
!     psig  ---  -----  ----------------------
!           ...
!           ---  -----  zone S (pure sigma)
!           ...  dpbot
!     pbot  ---  -----  ----------------------
!
!   Detailed procedure description:
!     STEP 1.
!   The pressure profile is computed with respect to the given reference
!   surface pressure pbot. For this surface pressure, the 'sigma' thicknesses
!   dsig are assumed to be proportional to a quadratic polynomial in sigma sig
!   with zero intercepts sig1 and sig2 somewhere below and above the model
!   domain, respectively. That is,
!     dsig ~ (sig2-sig)*(sig-sig1)*dk
!   Integrating this differential equation gives
!     sig = (sig1*exp(c1*k+c2)+sig2)/(exp(c1*k+c2)+1)
!   The required boundary conditions sig(0)=1 and sig(levs)=0
!   fix the proportionality and integration constants c1 and c2.
!   The two crossing parameters (sig1 and sig2) are determined
!   by two input sigma thickness conditions dsig/dk at the bottom and top
!   which are respectively given as dpbot/(pbot-ptop) and dptop/(pbot-ptop).
!   The crossing parameters are computed using Newton-Raphson iteration.
!   This procedure fixes the pressure profile for surface pressure pbot.
!     STEP 2.
!   The pressure profile is computed with respect to a minimum surface pressure.
!   This minimum surface pressure pmin is yet to be determined.
!   Divide the profile into zones:
!     zone P (pure pressure) from ptop to ppre
!     zone T1 (transition 1) from ppre to ppre+dppre
!     zone T2 (transition 2) from ppre+dppre to psig-dpsig
!     zone T3 (transition 3) from psig-dpsig to psig
!     zone S (pure "sigma") from psig to pmin
!     (here sigma=p/ps so that d(ln(p))/dk is horizontally uniform)
!   The pressure profile in the pure pressure zone P is set from step 1.
!   The pressure thicknesses in zone T1 is set to be quadratic in level k.
!   The pressure thicknesses in zone T2 is set to be linear in level k.
!   The pressure thicknesses in zone T3 is set to be quadratic in level k.
!   The pressure profile in the pure sigma zone S is also set from step 1.
!   Thus there are nine unknowns:
!     the 3 polynomial coefficients in zone T1
!     the 2 polynomial coefficients in zone T2
!     the 3 polynomial coefficients in zone T3
!     and the 1 minimum surface pressure.
!   The nine conditions to determine these unknowns are:
!     the thickness and its derivative match at zone P and T1 boundary
!     the thickness and its derivative match at zone T1 and T2 boundary
!     the thickness and its derivative match at zone T2 and T3 boundary
!     the thickness and its derivative match at zone T3 and S boundary
!     the sum of the thicknesses of zones T1, T2, T3, and S is pmin-ppre
!   The unknowns are computed using standard linear decomposition.
!   This procedure fixes the pressure profile for surface pressure pmin.
!     STEP 3.
!   For an arbitrary surface pressure, the pressure profile is an linear
!   combination of the pressure profiles for surface pressures pbot and pmin
!     p(psfc)=p(pbot)*(psfc-pmin)/(pbot-pmin)+p(pmin)*(pbot-psfc)/(pbot-pmin)
!   from which the hybrid coordinate profiles ak and bk are found such that
!     p(psfc)=ak+bk*psfc
!
!$$$
subroutine akbkgen(levs,lupp,pbot,psig,ppre,pupp,ptop,&
                   dpbot,dpsig,dppre,dpupp,dptop,pmin,ak,bk)
  implicit none
  integer,intent(in):: levs,lupp
  real,intent(in):: pbot,psig,ppre,pupp,ptop
  real,intent(in):: dpbot,dpsig,dppre,dpupp,dptop
  real,intent(out):: pmin,ak(0:levs),bk(0:levs)
  integer,parameter:: lo=100,li=10  ! outer and inner N-R iterations
  real pdif        ! thickness from pbot to ptop
  real delb        ! delta sig at bot
  real delt        ! delta sig at top
  real sig1        ! crossing parameter 1
  real sig2        ! crossing parameter 2
  real c1          ! proportionality constant
  real c2          ! integration constant
  real sig         ! sig variable
  real dsig        ! delta sig variable
  real delbio0     ! initial guess at delta sig at bot
  real deltio0     ! initial guess at delta sig at top
  real delbio      ! guess at delta sig at bot
  real deltio      ! guess at delta sig at top
  real c1sig1      ! d(c1)/d(sig1)
  real c1sig2      ! d(c1)/d(sig2)
  real p(2)        ! rhs in N-R iteration
  real fjac(2,2)   ! lhs in N-R iteration
  integer indx(2)  ! permutations in N-R iteration
  real ppred       ! pressure at T1-T2 boundary
  real spre        ! sig at P-T1 boundary
  real spred       ! sig at T1-T2 boundary
  real rkpre       ! level at P-T1 boundary
  real rkpred      ! level at T1-T2 boundary
  real pkpre       ! dp/dk at P-T1 boundary
  real pkkpre      ! d2p/dk2 at P-T1 boundary
  real psigd       ! pressure at T2-T3 boundary
  real ssig        ! sig at T3-S boundary
  real ssigd       ! sig at T2-T3 boundary
  real rksig       ! level at T3-S boundary
  real rksigd      ! level at T2-T3 boundary
  real pksig       ! dp/dk at T3-S boundary
  real pkksig      ! d2p/dk2 at T3-S boundary
  real p2sig       ! pressure at T3-S boundary for pmin surface pressure
  real p2sigd      ! pressure at T2-T3 boundary for pmin surface pressure
  real p2pred      ! pressure at T1-T2 boundary for pmin surface pressure
  real x2(9)       ! rhs in linear solver
  real a2(9,9)     ! lhs in linear solver
  integer indx2(9) ! permutations in linear solver
  real pkupp       ! dp/dk at U-P boundary
  real pkkupp      ! d2p/dk2 at U-P boundary
  real x3(3)       ! rhs in linear solver
  real a3(3,3)     ! lhs in linear solver
  integer indx3(3) ! permutations in linear solver
  real p1          ! pressure variable for pbot surface pressure
  real p2          ! pressure variable for pmin surface pressure
  real d           ! determinant permutation
  integer io,ii,k
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
!  STEP 1.
  pdif=pbot-pupp
  delb=dpbot/pdif
  delt=dpupp/pdif
   write(9,*) 'pdif,delb,delt=',pdif,delb,delt
  sig1=1+delb
  sig2=-delt
  c1=log(-sig2*(1-sig1)/sig1/(sig2-1))/lupp
  c2=log((sig2-1)/(1-sig1))
  sig=1
  dsig=(sig2-sig)*(sig-sig1)*c1/(sig1-sig2)
  delbio0=-dsig
  sig=0
  dsig=(sig2-sig)*(sig-sig1)*c1/(sig1-sig2)
  deltio0=-dsig
!  Newton-Raphson iterations
  do io=1,lo
    delbio=delbio0+(delb-delbio0)*io/lo
    deltio=deltio0+(delt-deltio0)*io/lo
    do ii=1,li
      c1sig1=-1/(sig1*(1-sig1)*lupp)
      c1sig2=-1/(sig2*(sig2-1)*lupp)
      sig=1
      dsig=(sig2-sig)*(sig-sig1)*c1/(sig1-sig2)
      p(1)=-delbio-dsig
      fjac(1,1)=((-c1*(sig+sig2)+(sig-sig1)*c1sig1*(sig1+sig2)) &
                *(sig2-sig)/(sig1+sig2)**2)
      fjac(1,2)=((c1*(sig+sig1)+(sig2-sig)*c1sig2*(sig1+sig2)) &
                *(sig-sig1)/(sig1+sig2)**2)
      sig=0
      dsig=(sig2-sig)*(sig-sig1)*c1/(sig1-sig2)
      p(2)=-deltio-dsig
      fjac(2,1)=((-c1*(sig+sig2)+(sig-sig1)*c1sig1*(sig1+sig2)) &
                *(sig2-sig)/(sig1+sig2)**2)
      fjac(2,2)=((c1*(sig+sig1)+(sig2-sig)*c1sig2*(sig1+sig2)) &
                *(sig-sig1)/(sig1+sig2)**2)
      call ludcmp(fjac,2,2,indx,d)
      call lubksb(fjac,2,2,indx,p)
      sig1=sig1+p(1)
      sig2=sig2+p(2)
      c1=log(-sig2*(1-sig1)/sig1/(sig2-1))/lupp
      c2=log((sig2-1)/(1-sig1))
   !write(9,*) 'io,ii,sig1,sig2,c1,c2=',io,ii,sig1,sig2,c1,c2
    enddo
  enddo
   write(9,*) 'sig1,sig2,c1,c2=',sig1,sig2,c1,c2
   write(9,*) 'pktop=',pdif*c1*(sig2-0)*(0-sig1)/(sig1-sig2)
   k=0
   sig=(sig1*exp(c1*k+c2)+sig2)/(exp(c1*k+c2)+1)
   write(9,*) 'pk0=',pdif*c1*(sig2-sig)*(sig-sig1)/(sig1-sig2)
   k=59
   sig=(sig1*exp(c1*k+c2)+sig2)/(exp(c1*k+c2)+1)
   write(9,*) 'pk59=',pdif*c1*(sig2-sig)*(sig-sig1)/(sig1-sig2)
   !stop 1
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
!  STEP 2.
!  Compute minimum surface pressure
  ppred=ppre+dppre
  spre=(ppre-pupp)/pdif
  spred=(ppred-pupp)/pdif
  rkpre=(log((spre-sig2)/(sig1-spre))-c2)/c1
  rkpred=(log((spred-sig2)/(sig1-spred))-c2)/c1
  pkpre=pdif*c1*(sig2-spre)*(spre-sig1)/(sig1-sig2)
  pkkpre=pkpre*c1*(sig2+sig1-2*spre)/(sig1-sig2)
  psigd=psig-dpsig
  ssig=(psig-pupp)/pdif
  ssigd=(psigd-pupp)/pdif
  rksig=(log((ssig-sig2)/(sig1-ssig))-c2)/c1
  rksigd=(log((ssigd-sig2)/(sig1-ssigd))-c2)/c1
  pksig=pdif*c1*(sig2-ssig)*(ssig-sig1)/(sig1-sig2)
  pkksig=pksig*c1*(sig2+sig1-2*ssig)/(sig1-sig2)
   write(9,*) 'psig,ssig,rksig=',psig,ssig,rksig
  x2=0
  a2=0
  x2(1)=pkpre
  a2(1,1)=1
  a2(1,2)=rkpre
  a2(1,3)=rkpre**2
  x2(2)=pkkpre
  a2(2,2)=1
  a2(2,3)=2*rkpre
  a2(3,1)=1
  a2(3,2)=rkpred
  a2(3,3)=rkpred**2
  a2(3,4)=-1
  a2(3,5)=-rkpred
  a2(4,2)=1
  a2(4,3)=2*rkpred
  a2(4,5)=-1
  a2(5,4)=-1
  a2(5,5)=-rksigd
  a2(5,6)=1
  a2(5,7)=rksigd
  a2(5,8)=rksigd**2
  a2(6,5)=-1
  a2(6,7)=1
  a2(6,8)=2*rksigd
  a2(7,6)=1
  a2(7,7)=rksig
  a2(7,8)=rksig**2
  a2(7,9)=-pksig/pbot
  a2(8,7)=1
  a2(8,8)=2*rksig
  a2(8,9)=-pkksig/pbot
  x2(9)=ppre
  a2(9,1)=(rkpre-rkpred)
  a2(9,2)=(rkpre**2-rkpred**2)/2
  a2(9,3)=(rkpre**3-rkpred**3)/3
  a2(9,4)=(rkpred-rksigd)
  a2(9,5)=(rkpred**2-rksigd**2)/2
  a2(9,6)=(rksigd-rksig)
  a2(9,7)=(rksigd**2-rksig**2)/2
  a2(9,8)=(rksigd**3-rksig**3)/3
  a2(9,9)=psig/pbot
  call ludcmp(a2,9,9,indx2,d)
  call lubksb(a2,9,9,indx2,x2)
  pmin=x2(9)
   write(9,*) 'pmin=',pmin
  p2sig=psig/pbot*pmin
  p2sigd=p2sig &
        +x2(6)*(rksigd-rksig) &
        +x2(7)*(rksigd**2-rksig**2)/2 &
        +x2(8)*(rksigd**3-rksig**3)/3
  p2pred=p2sigd &
        +x2(4)*(rkpred-rksigd) &
        +x2(5)*(rkpred**2-rksigd**2)/2
   !stop 2
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
!  STEP 3.
  if(lupp.lt.levs) then
    pkupp=pdif*c1*(sig2-0)*(0-sig1)/(sig1-sig2)
    pkkupp=pkupp*c1*(sig2+sig1-2*0)/(sig1-sig2)
    x3=0
    a3=0
    x3(1)=pkupp
    a3(1,1)=pupp
    x3(2)=pkkupp*pupp-pkupp**2
    a3(2,2)=pupp**2
    x3(3)=log((ptop+dptop)/pupp)
    a3(3,1)=levs-1-lupp
    a3(3,2)=(levs-1-lupp)**2/2
    a3(3,3)=(levs-1-lupp)**3/3
     write(9,*) 'pre-x3=',x3
     !stop 31
    call ludcmp(a3,3,3,indx3,d)
     write(9,*) 'at stop 32. a3=',a3
     write(9,*) 'at stop 32. x3=',x3
     !stop 32
    call lubksb(a3,3,3,indx3,x3)
     write(9,*) 'post-x3=',x3
     !stop 3
  endif
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
!  STEP 4.
!  Compute hybrid interface values
  ak(0)=0
  bk(0)=1
  do k=1,levs-1
    if(k.ge.lupp) then
      p1=pupp*exp(x3(1)*(k-lupp)+x3(2)*(k-lupp)**2/2+x3(3)*(k-lupp)**3/3)
    else
      p1=(sig1*exp(c1*k+c2)+sig2)/(exp(c1*k+c2)+1)*pdif+pupp
    endif
    if(k.ge.rkpre) then
      p2=p1
    elseif(k.ge.rkpred) then
      p2=p2pred+x2(1)*(k-rkpred) &
               +x2(2)*(k**2-rkpred**2)/2 &
               +x2(3)*(k**3-rkpred**3)/3
    elseif(k.ge.rksigd) then
      p2=p2sigd+x2(4)*(k-rksigd) &
               +x2(5)*(k**2-rksigd**2)/2
    elseif(k.ge.rksig) then
      p2=p2sig+x2(6)*(k-rksig) &
              +x2(7)*(k**2-rksig**2)/2 &
              +x2(8)*(k**3-rksig**3)/3
    else
      p2=p1/pbot*pmin
    endif
    ak(k)=(p2*pbot-p1*pmin)/(pbot-pmin)
    bk(k)=(p1-p2)/(pbot-pmin)
  enddo
  ak(levs)=ptop
  bk(levs)=0
end subroutine
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!$$$  Subprogram documentation block
!
! Subprogram:    ludcmp      lower and upper triangular decomposition
!   Prgmmr: Iredell    Org: W/NP23      Date: 2008-08-01
!
! Abstract: This subprogram decomposes a matrix into a product of
!   lower and upper triangular matrices.
!
! Program history log:
!   2008-08-01  Mark Iredell
!
! Usage:    call ludcmp(a,n,np,indx,d)
!   Input argument list:
!     a        real(np,np) matrix (will be overwritten)
!     n        integer order of matrix
!     np       integer dimension of matrix
!
!   Output argument list:
!     a        real(np,np) LU-decomposed matrix
!              (U is upper part of A, including diagonal;
!               L is lower part of A, with 1 as diagonal;
!               L*U equals original A after permuting)
!     indx     integer(n) pivot indices
!              (original A rows are permuted in order i with indx(i))
!     d        real determinant permutation (1 or -1, or 0 if singular)
!              (determinant is output diagonal product times d)
!
! Attributes:
!   Language: Fortran 90
!
!$$$
subroutine ludcmp(a,n,np,indx,d)
  implicit none
  integer,intent(in):: n,np
  real,intent(inout):: a(np,np)
  integer,intent(out):: indx(n)
  real,intent(out):: d
  integer i,j,k,imax
  real aamax,sum,dum
  real vv(n)
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  d=1
  do i=1,n
    aamax=0
    do j=1,n
      if(abs(a(i,j))>aamax) aamax=abs(a(i,j))
    enddo
    if(aamax==0) then
      d=0
      return
    endif
    vv(i)=1/aamax
  enddo
  do j=1,n
    do i=1,j-1
      sum=a(i,j)
      do k=1,i-1
        sum=sum-a(i,k)*a(k,j)
      enddo
      a(i,j)=sum
    enddo
    aamax=0.
    do i=j,n
      sum=a(i,j)
      do k=1,j-1
        sum=sum-a(i,k)*a(k,j)
      enddo
      a(i,j)=sum
      dum=vv(i)*abs(sum)
      if(dum>=aamax) then
        imax=i
        aamax=dum
      endif
    enddo
    if (j/=imax)then
      do k=1,n
        dum=a(imax,k)
        a(imax,k)=a(j,k)
        a(j,k)=dum
      enddo
      d=-d
      vv(imax)=vv(j)
    endif
    indx(j)=imax
    if(a(j,j)==0) then
      d=0
      return
    endif
    if(j/=n)then
      dum=1/a(j,j)
      do i=j+1,n
        a(i,j)=a(i,j)*dum
      enddo
    endif
  enddo
end subroutine
!-------------------------------------------------------------------------------
!$$$  Subprogram documentation block
!
! Subprogram:    lubksb      lower and upper triangular back substitution
!   Prgmmr: Iredell    Org: W/NP23      Date: 2008-08-01
!
! Abstract: This subprogram back substitutes to solve decomposed
!   lower and upper triangular matrices as outputted by ludcmp.
!
! Program history log:
!   2008-08-01  Mark Iredell
!
! Usage:    call lubksb(a,n,np,indx,b)
!   Input argument list:
!     a        real(np,np) LU-decomposed matrix
!              (from ludcmp)
!     n        integer order of matrix
!     np       integer dimension of matrix
!     indx     integer(n) pivot indices
!              (from ludcmp)
!     b        real(n) rhs vector of linear problem (will be overwritten)
!
!   Output argument list:
!     b        real(n) solution of linear problem
!              (original A times output B equals original B)
!
! Attributes:
!   Language: Fortran 90
!
!$$$
subroutine lubksb(a,n,np,indx,b)
  implicit none
  integer,intent(in):: n,np
  real,intent(in):: a(np,np)
  integer,intent(in):: indx(n)
  real,intent(inout):: b(n)
  integer i,j,ii,ll
  real sum
! - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  ii=0
  do i=1,n
    ll=indx(i)
    sum=b(ll)
    b(ll)=b(i)
    if (ii/=0)then
      do j=ii,i-1
        sum=sum-a(i,j)*b(j)
      enddo
    elseif(sum/=0) then
      ii=i
    endif
    b(i)=sum
  enddo
  do i=n,1,-1
    sum=b(i)
    do j=i+1,n
      sum=sum-a(i,j)*b(j)
    enddo
    b(i)=sum/a(i,i)
  enddo
end subroutine

