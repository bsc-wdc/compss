program albedo4season
!-----------------------------------------------------------------------
! Original program albedo from z. janjic, oct. 1999, aug. 2007
!
! New albedo4season by Carlos Perez  13th Math 2009
! Barcelona Supercomputing Center
!
! Reads seasonal climatologies from geodata/albedo_rrtm1deg
! Interpolates in time to the initial forecast day
! Interpolates in space to the model grid

! Produces 4 albedo components: vis and ir with strong and weak cosz
! dependency
! Produces 2 fractional coverages (strong and weak cosz dependency)
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include 'modelgrid.inc'
include 'llgrid_rrtm1deg.inc'
!-----------------------------------------------------------------------
character(2):: &
 sfx

character(128):: &
 infile &
,outfile

logical(kind=4):: &
 run

integer(kind=4):: &
 i,iday,ihrst,imon &
,j,jday &
,mon

real(kind=4):: &
 ctph,ctph0,data00,data10,data01,data11,deltax,dlm,dph,pp,qq &
,rdx,rdy,sb,stph0,sum,tlm0,tph0,txgrid,tygrid,wb,wght,xgrid,ygrid

integer(kind=4),dimension(1:3):: &
 idat

integer(kind=4),dimension(1:12):: &
 month

integer(kind=4),dimension(1:13):: &
 jmonth

real(kind=4),dimension(1:imi,1:jmi):: &
 seamask

real(kind=4),dimension(1:imi,1:jmi):: &
 alvsf &
,alnsf &
,alvwf &
,alnwf &
,facsf &
,facwf

real(kind=4),dimension(1:imll,1:jmll):: &
 alvsf_sea1 &
,alnsf_sea1 & 
,alvwf_sea1 &
,alnwf_sea1 &
,alvsf_sea2 &
,alnsf_sea2 &
,alvwf_sea2 &
,alnwf_sea2 &
,galvsf &
,galnsf &
,galvwf &
,galnwf &
,gfacsf &
,gfacwf  

integer(kind=4) :: &
 monend &
,is &
,mm &
,mmm &
,mmp &
,sea1 &
,sea2 &
,frac1 &
,frac2 &
,ierr

real(kind=4), dimension(1:13) :: &
dayhf

real(kind=4) :: &
 wei1s &
,wei2s

real(kind=4), dimension (3,1:imi,1:jmi) :: &
 coh

real(kind=4), dimension (4,1:imi,1:jmi) :: &
 inh &
,jnh

real(kind=4), parameter :: &
 dtr=3.1415926535897932384626433832795/180.

!--------------------------------------------------------------------
      data dayhf/ 15.5, 45.0, 74.5,105.0,135.5,166.0,196.5,227.5  &
                  ,258.0,288.5,319.0,349.5,380.5/     
      data month/31,28,31,30,31,30,31,31,30,31,30,31/
!-------------read in albedo data for mnth1 and mnth2----------------
 1002 format(100i2)
 1100 format(100f5.1)
!-----------------------------------------------------------------------
      sfx='  '
      infile = &
      '                                                                '
      outfile= &
      '                                                                '
!-----------------------------------------------------------------------
      tlm0=tlm0d*dtr
      tph0=tph0d*dtr
      stph0=sin(tph0)
      ctph0=cos(tph0)
!
      wb=wbd*dtr
      sb=sbd*dtr
      dlm=dlmd*dtr
      dph=dphd*dtr
!
      do j=1,jmi
        do i=1,imi
       alvsf(i,j)=-9999. 
       alnsf(i,j)=-9999. 
       alvwf(i,j)=-9999. 
       alnwf(i,j)=-9999. 
       facsf(i,j)=-9999. 
       facwf(i,j)=-9999.
        enddo
      enddo
!----------------------------------------------------------------------
      infile='../output/seamask'
      open(unit=1,file=infile,status='old',form='unformatted')
      read(1) seamask
      close(1)
!----------------------------------------------------------------------
      infile='../output/llspl.000'
      open(unit=1,file=infile,status='old',form='unformatted')
      read(1) run,idat,ihrst
      close(1)
!
      print*,'Cycle data read from ',infile
      print*,'Cycle ',idat,ihrst,' UTC'
!-------------julian day for 15th of each month-------------------------
!      jmonth(1)=15
!      do imon=2,13
!        jmonth(imon)=0
!        do mon=1,imon-1
!          jmonth(imon)=jmonth(imon)+month(mon)
!        enddo
!        jmonth(imon)=jmonth(imon)+15
!      enddo
!--------------julian day for current date------------------------------
!      iday=idat(1)
!      imon=idat(2)
!      jday=iday
!      if(imon.gt.1) then
!        do mon=2,imon
!          jday=jday+month(mon-1)
!        enddo
!      endif
!!
!      if(jday.lt.15) jday=365+jday
!!-----------------------------------------------------------------------
!      do imon=1,12
!        if(jday.ge.jmonth(imon).and.jday.lt.jmonth(imon+1)) then
!          mnth1=imon
!          mnth2=imon+1
!          wght=float(jday-jmonth(imon)) &
!              /float(jmonth(imon+1)-jmonth(imon))
!          go to 100
!        endif
!      enddo
! 100  continue
!      if(mnth2.gt.12) mnth2=1

!     FOR SEASONAL MEAN CLIMATOLOGY
!
      iday=idat(1)
      imon=idat(2)
      jday=iday
      if(imon.gt.1) then
        do mon=2,imon
          jday=jday+month(mon-1)
        enddo
      endif
!
      if(jday.lt.15) jday=365+jday

      monend = 4
      is     = imon/3 + 1
!
      if (is.eq.5) is = 1
      do mm=1,monend
        mmm=mm*3 - 2
        mmp=(mm+1)*3 - 2
        if(jday.ge.dayhf(mmm).and.jday.lt.dayhf(mmp)) then
          sea1 = mmm
          sea2 = mmp
          go to 30
        endif
      enddo
      print *,'wrong jday',jday
      call abort
 30   continue
      wei1s = (dayhf(sea2)-jday)/(dayhf(sea2)-dayhf(sea1))
      wei2s = (jday-dayhf(sea1))/(dayhf(sea2)-dayhf(sea1))
!
!!! KARSTEN
      sea1=(mmm+2)/3
      sea2=(mmp+2)/3
!!! KARSTEN
!
      if(sea2.eq.5) sea2=1

      print*,'wei1s=',wei1s,' sea1=',sea1
      print*,'wei2s=',wei2s,' sea2=',sea2
!-------------read in albedo data for season1 and season2 ----------------------------

      write(sfx,'(i2.2)') sea1
!
      print*,'sfx=',sfx
      infile='../geodata/albedo_rrtm1deg/alvsf'//sfx
      print*,'infile=',infile
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alvsf_sea1
      close(2)
!
      infile='../geodata/albedo_rrtm1deg/alnsf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alnsf_sea1
      close(2)
!
      infile='../geodata/albedo_rrtm1deg/alvwf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alvwf_sea1
      close(2)
!
      infile='../geodata/albedo_rrtm1deg/alnwf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alnwf_sea1
      close(2)
!--------------------------------------------------------------------------------------

      write(sfx,'(i2.2)') sea2
!
      infile='../geodata/albedo_rrtm1deg/alvsf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alvsf_sea2
      close(2)
!

      do j=jmll,1,-10
        write(*,1100) (alvsf_sea2(i,j),i=1,imll,20)
      enddo


      infile='../geodata/albedo_rrtm1deg/alnsf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alnsf_sea2
      close(2)
!
      infile='../geodata/albedo_rrtm1deg/alvwf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alvwf_sea2
      close(2)
!
      infile='../geodata/albedo_rrtm1deg/alnwf'//sfx
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) alnwf_sea2
      close(2)
!--------------------------------------------------------------------------------------
      infile='../geodata/albedo_rrtm1deg/facsf'
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) gfacsf
      close(2)
!--------------------------------------------------------------------------------------
      infile='../geodata/albedo_rrtm1deg/facwf'
      open(unit=2,file=infile,status='unknown',form='unformatted')
      read(2) gfacwf
      close(2)
      print*,'----------------------------------------------------'
      do j=jmll,1,-10
        write(*,1100) (gfacwf(i,j),i=1,imll,20)
      enddo



!--------------do the time interpolation-----------------------------------------------
      do j=1,jmll
        do i=1,imll
           galvsf(i,j)=alvsf_sea1(i,j)*wei1s + alvsf_sea2(i,j)*wei2s
           galnsf(i,j)=alnsf_sea1(i,j)*wei1s + alnsf_sea2(i,j)*wei2s
           galvwf(i,j)=alvwf_sea1(i,j)*wei1s + alvwf_sea2(i,j)*wei2s
           galnwf(i,j)=alnwf_sea1(i,j)*wei1s + alnwf_sea2(i,j)*wei2s
        enddo
      enddo
!-------------spacial interpolation loop-----------------------------------------------
!
      call gtllhnewalb(coh,inh,jnh)

      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,galvsf,alvsf)
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,galnsf,alnsf)
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,galvwf,alvwf)
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,galnwf,alnwf)
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gfacsf,facsf)
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gfacwf,facwf)

!-------------SET ALBEDO OVER OCEAN TO 0.06------------------------------------

      do i=1,imi
        do j=1,jmi
          if(seamask(i,j).gt.0.5) then
              alvsf(i,j)=6.0
              alnsf(i,j)=6.0
              alvwf(i,j)=6.0
              alnwf(i,j)=6.0
              facsf(i,j)=6.0
              facwf(i,j)=6.0
          endif
         enddo
       enddo
!-----------------------------------------------------------------------

      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (alvsf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (alnsf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (alvwf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (alnwf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (facsf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'
      do j=jmi,1,-10
        write(*,1100) (facwf(i,j),i=1,imi,30)
      enddo
      print*,'----------------------------------------------------------'

!-----------------------------------------------------------------------
      outfile='../output/albedorrtm'
      open(unit=2,file=outfile,status='unknown',form='unformatted')
      write(2) alvsf*0.01
      write(2) alnsf*0.01
      write(2) alvwf*0.01
      write(2) alnwf*0.01
      write(2) facsf*0.01
      write(2) facwf*0.01
      close(2)
!
      print*,'New albedos written to ../output/albedorrtm'
      print*,'Enjoy your new albedos!'
!-----------------------------------------------------------------------
endprogram albedo4season
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine gtllhnewalb(coh,inh,jnh)
!-----------------------------------------------------------------------
      include 'modelgrid.inc'
!-----------------------------------------------------------------------
      parameter(dtr=3.1415926535897932384626433832795/180.)
!------ llgrid05.inc for 0.5 deg sst data-------------------------------
      include 'llgrid_rrtm1deg.inc'
!-----------------------------------------------------------------------
      dimension coh(3,imi,jmi),inh(4,imi,jmi),jnh(4,imi,jmi)
!
      print*,'*** model dimensions imi=',imi,' jmi=',jmi
      print*,'*** data dimensions imll=',imll,' jmll=',jmll
!--------------- umo domain geometry-----------------------------------
      wb=wbd*dtr
      sb=sbd*dtr
      tph0=tph0d*dtr
      ctph0=cos(tph0)
      stph0=sin(tph0)
      dlm=dlmd*dtr
      dph=dphd*dtr
!-------------- entry to the umo i,j loop -----------------------------
!               neighbour avn index identification
!               avn data defined in ll system
!-----------------------------------------------------------------------
!               umo height pts
!-----------------------------------------------------------------------
      tph=sb-dph
              do j=1,jmi
          tph=tph+dph
          tlm=wb-dlm
!
          do i=1,imi
      tlm=tlm+dlm
!------------- tll to ll conversion ------------------------------------
      call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
!-------------conversion from -180,180 range to 0,360 range-------------
      if(almd.lt.0.) almd=360.+almd
!--------------check if umo pt is out of avn domain--------------------
      x=almd-bowest
      y=aphd-bosout
!
      indll=x/delon+1
      if(x.lt.0.) indll=imll
      jndll=y/delat+1
!
      if(indll.eq.0)      indll=imll-1
      if(indll.gt.imll)   indll=1
      if(jndll.eq.0)      jndll=1
      if(jndll.ge.jmll-1) jndll=jmll-1
!
          if(x.ge.0.) then
      x=x-(indll-1)*delon
          else
      x=x+delon
          endif
      y=y-(jndll-1)*delat
!
          if((indll.lt.1..or.indll.gt.imll).or.   &
     &       (jndll.lt.1..or.jndll.gt.jmll))    then
      print *,'*** At h point i,j=',i,j,' nearest AVN point is indll=', &
     &        indll,' jndll=',jndll
      stop
          endif

!-----------------------------------------------------------------------
      coh(1,i,j)=x/delon
      coh(2,i,j)=y/delat
      coh(3,i,j)=coh(1,i,j)*coh(2,i,j)
!-----------------------------------------------------------------------
      inh(1,i,j)=indll
      inh(3,i,j)=indll
          if(indll.lt.imll) then
      inh(2,i,j)=indll+1
      inh(4,i,j)=indll+1
          else
      inh(2,i,j)=1
      inh(4,i,j)=1
          endif
!
      jnh(1,i,j)=jndll
      jnh(2,i,j)=jndll
      jnh(3,i,j)=jndll+1
      jnh(4,i,j)=jndll+1
          enddo
              enddo
!-----------------------------------------------------------------------
      return
      end subroutine gtllhnewalb
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
!     ****************************************************************
!     *                                                              *
!     *  programer: z. janjic, shmz, feb. 1981                       *
!     *  ammended:  z. janjic, ncep, jan. 1996                       *
!     *                                                              *
!     *  transformation from rotated lat-lon to lat-lon coordinates  *
!     ****************************************************************
!     ****************************************************************
!     *  tlm   - transformed longitude, rad.                         *
!     *  tph   - transformed latitude, rad.                          *
!     *  tlm0d - the angle of rotation of the transformed lat-lon    *
!     *          system in the longitudinal direction, degs          *
!     *  ctph0 - cos(tph0), tph0 is the angle of rotation of the     *
!     *          transformed lat-lon systemn in the latitudinal      *
!     *          direction, precomputed                              *
!     *  stph0 - sin(tph0), tph0 is the angle of rotation of the     *
!     *          transformed lat-lon systemn in the latitudinal      *
!     *          direction, precomputed                              *
!     *  almd  - geographical longitude, degs, range -180.,180       *
!     *  aphd  - geographical latitude,  degs, range - 90., 90.,     *
!     *          poles are singular                                  *
!     ****************************************************************
!
      parameter(pi=3.1415926535897932384626433832795,dtr=pi/180.)
!
      stlm=sin(tlm)
      ctlm=cos(tlm)
      stph=sin(tph)
      ctph=cos(tph)
!
      sph=ctph0*stph+stph0*ctph*ctlm
      sph=min(sph,1.)
      sph=max(sph,-1.)
      aph=asin(sph)
      aphd=aph/dtr
      anum=ctph*stlm
      denom=(ctlm*ctph-stph0*sph)/ctph0
      relm=atan2(anum,denom)-pi
      almd=relm/dtr+tlm0d
!
      if(almd.gt. 180.)    almd=almd-360.
      if(almd.lt.-180.)    almd=almd+360.
!
      endsubroutine rtll
!----------------------------------------------------------------------

!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine bilinb(cob,inb,jnb,imll,jmll,imi,jmi,ww,wfb)
!-----------------------------------------------------------------------
      dimension cob(3,imi,jmi)
      integer inb(4,imi,jmi),jnb(4,imi,jmi)
      dimension ww(imll,jmll),wfb(imi,jmi)
!-----------------------------------------------------------------------
              do j=1,jmi
          do i=1,imi
!
      i00=inb(1,i,j)
      i10=inb(2,i,j)
      i01=inb(3,i,j)
      i11=inb(4,i,j)
!
      j00=jnb(1,i,j)
      j10=jnb(2,i,j)
      j01=jnb(3,i,j)
      j11=jnb(4,i,j)
!
      p=cob(1,i,j)
      q=cob(2,i,j)
      pq=cob(3,i,j)
!
      z=ww(i00,j00)  &
     & +p*(ww(i10,j10)-ww(i00,j00)) &
     & +q*(ww(i01,j01)-ww(i00,j00)) &
     & +pq*(ww(i00,j00)-ww(i10,j10)-ww(i01,j01)+ww(i11,j11))
!
      wfb(i,j)=z
!
          enddo
              enddo
!-----------------------------------------------------------------------
      return
      end subroutine bilinb



