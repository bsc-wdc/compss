program vcgenerator
!     *************************************************************
!     *                                                           *
!     *  hybrid vertical coordinate generator                     *
!     *  programer z.janjic, ncep 2008                            *
!     *                                                           *
!     *************************************************************
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
real(kind=4),parameter:: &
 g=9.8,r=287.04,gor=g/r

integer(kind=4):: &
 kold,l,lpt2,lsal

real(kind=4) &
 bsg,es,pdtop,pl,pbot,pt2,rp,rs,rsg,rsum,sum

real(kind=4),dimension(1:10):: &
 xold,dold,y2s,pps,qqs

real(kind=4),dimension(1:lm):: &
 dsg,dsg1,dsg2,sgml1,sgml2,xnew

real(kind=4),dimension(1:lm+1):: &
 a,b,sg1,sg2,sgm

integer num, length, stat
character*256 output_dsg

if (command_argument_count().gt.0) then
  call get_command_argument(1,output_dsg,length,stat)
end if

!-----------------------------------------------------------------------
      print*,'*** Hi, this is your vertical coordinate generator.'
!--set rel. thicknesses of ref. sigma layers at significant points------
      xold(1)=.00
      xold(2)=.30 !.25
      xold(3)=.40 !.35
      xold(4)=.55 !.50
      xold(5)=.80
      xold(6)=1.
!
      dold(1)=.002
      dold(2)=.008
      dold(3)=.008
      dold(4)=.016
      dold(5)=.008
      dold(6)=.0025
!
!      dold(1)=.002
!      dold(2)=.008
!      dold(3)=.01
!      dold(4)=.016
!      dold(5)=.008
!      dold(6)=.002
!
      kold=6
!
      do l=1,lm
        xnew(l)=float(l-1)/float(lm-1)
      enddo
!
      do l=1,10
        y2s(l)=0.
      enddo
!
      call spline(kold,xold,dold,y2s,lm,xnew,dsg,pps,qqs)
!
      sum=0.
      do l=1,lm
        sum=sum+dsg(l)
      enddo
!
      rsum=1./sum
      sgm(1)=0.
      do l=1,lm-1
        dsg(l)=dsg(l)*rsum
        sgm(l+1)=sgm(l)+dsg(l)
      enddo
      sgm(lm+1)=1.
      dsg(lm)=sgm(lm+1)-sgm(lm)
!--print reference sigma and thicknesses of the layers------------------
      print*,' reference sigma, l and sigma increase top down'
      do l=1,lm+1
        print*,'l=',l,' sgm(l)=',sgm(l)
      enddo
      do l=1,lm
        print*,'l=',l,' dsg(l)=',dsg(l)
      enddo
!-----------------------------------------------------------------------
      pbot=101300. ! reference mslp
      lpt2=0. ! interface below pressure range, # of pressure layers + 1
!
      do l=1,lm+1
        pl=sgm(l)*(pbot-pt)+pt
        if(pl.lt.ptsgm) lpt2=l
      enddo
!
      if(lpt2.gt.0) then
        pt2=sgm(lpt2)*(101300.-pt)+pt ! transition point
      else
        pt2=pt                        ! there are no pressure layers
      endif
      print*,'*** Mixed system starts at ',pt2,' Pa, from level ',lpt2
      pdtop=pt2-pt 
!--pressure range-------------------------------------------------------
      do l=1,lpt2
        a(l)=sgm(l)/sgm(lpt2)*pdtop+pt
        b(l)=0.
      enddo
!--mixed range----------------------------------------------------------
      lsal=0 !1 ! # of sangster-arakawa-lamb layers at the botom
      rp=2.2 !2. !2.2
      rs=1.2 !1.1 !1.2 !1. !1.35
      es=9. !10. !5. !10. !5.
      do l=lpt2+1,lm+1-lsal
        bsg=(sgm(l)-sgm(lpt2))/(1.-sgm(lpt2))
        rsg=rp+(rs-rp)/atan(es)*atan(es*bsg)
        b(l)=bsg**rsg
        a(l)=(sgm(l)-b(l))*(pbot-pt)+pt
      enddo
      do l=lm+1-lsal+1,lm+1
        b(l)=(sgm(l)-sgm(lpt2))/(1.-sgm(lpt2))
        a(l)=(sgm(l)-b(l))*(pbot-pt)+pt
      enddo
!--redefined equivalent sigma-------------------------------------------
      do l=1,lm+1
        sgm(l)=(a(l)-pt+b(l)*(pbot-pt))/(pbot-pt)
        print*, 'equivalent sgm(',l,')=',sgm(l)
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
      open(unit=1,file=output_dsg &
          ,status='unknown',form='unformatted')
      write(1) pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
      close(1)
!-----------------------------------------------------------------------
endprogram vcgenerator
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
subroutine spline(nold,xold,yold,y2,nnew,xnew,ynew,p,q)           
!     ******************************************************************
!     *                                                                *
!     *  this is a one-dimensional cubic spline fitting routine        *
!     *  programed for a small scalar machine.                         *
!     *                                                                *
!     *  programer: z. janjic, yugoslav fed. hydromet. inst., beograd  *
!     *                                                                *
!     *                                                                *
!     *                                                                *
!     *  nold - number of given values of the function.  must be ge 3. *
!     *  xold - locations of the points at which the values of the     *
!     *         function are given.  must be in ascending order.       *
!     *  yold - the given values of the function at the points xold.   *
!     *  y2   - the second derivatives at the points xold.  if natural *
!     *         spline is fitted y2(1)=0. and y2(nold)=0. must be      *
!     *         specified.                                             *
!     *  nnew - number of values of the function to be calculated.     *
!     *  xnew - locations of the points at which the values of the     *
!     *         function are calculated.  xnew(k) must be ge xold(1)   *
!     *         and le xold(nold).                                     *
!     *  ynew - the values of the function to be calculated.           *
!     *  p, q - auxiliary vectors of the length nold-2.                *
!     *                                                                *
!     ******************************************************************                                                                       
!-----------------------------------------------------------------------
!      include 'double.inc'
!-----------------------------------------------------------------------
real(kind=4),dimension(1:nold):: &
 xold,yold,y2,p,q

real(kind=4),dimension(1:nnew):: &
 xnew,ynew
!-----------------------------------------------------------------------
print*,'1'
      noldm1=nold-1                                                     
!                                                                       
      dxl=xold(2)-xold(1)                                               
      dxr=xold(3)-xold(2)                                               
      dydxl=(yold(2)-yold(1))/dxl                                       
      dydxr=(yold(3)-yold(2))/dxr                                       
      rtdxc=.5/(dxl+dxr)                                                
!                                                                       
      p(1)= rtdxc*(6.*(dydxr-dydxl)-dxl*y2(1))                          
      q(1)=-rtdxc*dxr                                                   
!                                                                       
      if(nold.eq.3) go to 700                                           
!-----------------------------------------------------------------------
      k=3                                                               
!                                                                       
 100  dxl=dxr                                                           
      dydxl=dydxr                                                       
      dxr=xold(k+1)-xold(k)                                             
      dydxr=(yold(k+1)-yold(k))/dxr                                     
      dxc=dxl+dxr                                                       
      den=1./(dxl*q(k-2)+dxc+dxc)                                       
!                                                                       
      p(k-1)= den*(6.*(dydxr-dydxl)-dxl*p(k-2))                         
      q(k-1)=-den*dxr                                                   
!                                                                       
      k=k+1                                                             
      if(k.lt.nold) go to 100                                           
!-----------------------------------------------------------------------
 700  k=noldm1                                                          
!                                                                       
 200  y2(k)=p(k-1)+q(k-1)*y2(k+1)                                       
!                                                                       
      k=k-1                                                             
      if(k.gt.1) go to 200                                              
!-----------------------------------------------------------------------
      k1=1                                                              
!                                                                       
 300  xk=xnew(k1)                                                       
!                                                                       
      do 400 k2=2,nold                                                  
      if(xold(k2).le.xk) go to 400                                      
      kold=k2-1                                                         
      go to 450                                                         
 400  continue                                                          
      ynew(k1)=yold(nold)                                               
      go to 600                                                         
!                                                                       
 450  if(k1.eq.1)   go to 500                                           
      if(k.eq.kold) go to 550                                           
!                                                                       
 500  k=kold                                                            
!                                                                       
      y2k=y2(k)                                                         
      y2kp1=y2(k+1)                                                     
      dx=xold(k+1)-xold(k)                                              
      rdx=1./dx                                                         
!                                                                       
      ak=.1666667*rdx*(y2kp1-y2k)                                       
      bk=.5*y2k                                                         
      ck=rdx*(yold(k+1)-yold(k))-.1666667*dx*(y2kp1+y2k+y2k)            
!                                                                       
 550  x=xk-xold(k)                                                      
      xsq=x*x                                                           
!                                                                       
      ynew(k1)=ak*xsq*x+bk*xsq+ck*x+yold(k)                             
!                                                                       
 600  k1=k1+1                                                           
      if(k1.le.nnew) go to 300
!-----------------------------------------------------------------------
      return 
!-----------------------------------------------------------------------
      endsubroutine spline                                                               


