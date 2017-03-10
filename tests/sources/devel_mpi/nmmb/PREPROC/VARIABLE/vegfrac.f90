program vegfracprog
!-----------------------------------------------------------------------
!     z. janjic, oct. 1999, aug. 2007
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include 'include/modelgrid.inc'
!-----------------------------------------------------------------------
!    note:  this subroutine and interpolation algorithm assume
!    a 0.144-deg global field in the following format:
!
!     i=1 at 180-0.144/2w, i=2 at 180-0.144/2-0.144w, ... ,
!     i=2500 at 180-0.144e
!     j=1 at 89.928s, j=2 at 88.928-0.144s, ..., j=1250 at 89.928n
!-----------------------------------------------------------------------
integer(kind=4),parameter:: &
 idatamax=2500 &
,jdatamax=1250

real(kind=4),parameter:: &
 dtr=3.1415926535897932384626433832795/180 &
,x0=-180.+0.144/2. &
,y0=-89.928 &
,boeast=180.-0.144/2. &
,bonort=89.928 &
,dx=0.144 &
,dy=0.144  

character(2):: &
 sfx

character(256):: &
 infile &
,outfile

logical(kind=4):: &
 run

integer(kind=4):: &
 i,idata,idatabase,idatap1,iday,ihrst,imon &
,j,jdata,jdatap1,jday &
,ksum &
,mnth1,mnth2,mon

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
 seamask &
,vegfrac

integer(kind=2),dimension(1:idatamax,1:jdatamax):: &
 ifrc &
,ifrcp 
!-----------------------------------------------------------------------
      data month/31,28,31,30,31,30,31,31,30,31,30,31/

      character*256 param1,param2,param3,vegfracmnth
      call getarg(1,param1)
      call getarg(2,param2)
      call getarg(3,param3)
      call getarg(4,vegfracmnth)

!-------------read in vegfrac data for mnth1 and mnth2----------------
 1002 format(100i2)
 1100 format(100f4.2)
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
      rdx=1./dx
      rdy=1./dy
!
      do j=1,jmi
        do i=1,imi
          vegfrac(i,j)=-9999.
        enddo
      enddo
!----------------------------------------------------------------------
      infile=param1
      open(unit=1,file=infile,status='old',form='unformatted')
      read(1) run,idat,ihrst
      close(1)
!
      print*,'Cycle data read from ',infile
      print*,'Cycle ',idat,ihrst,' UTC'
!-------------julian day for 15th of each month-------------------------
      jmonth(1)=15
      do imon=2,13
        jmonth(imon)=0
        do mon=1,imon-1
          jmonth(imon)=jmonth(imon)+month(mon)
        enddo
        jmonth(imon)=jmonth(imon)+15
      enddo
!--------------julian day for current date------------------------------
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
!-----------------------------------------------------------------------
      do imon=1,12
        if(jday.ge.jmonth(imon).and.jday.lt.jmonth(imon+1)) then
          mnth1=imon
          mnth2=imon+1
          wght=float(jday-jmonth(imon)) &
              /float(jmonth(imon+1)-jmonth(imon))
          go to 100
        endif
      enddo
 100  continue
      if(mnth2.gt.12) mnth2=1
!-------------read in vegfrac data for mnth1 and mnth2-------------------
      write(sfx,'(i2.2)') mnth1
      infile = trim(vegfracmnth) // sfx // '.ascii'
      open(unit=2,file=infile,status='unknown',form='formatted')
      read(2,1002) ifrc
      close(2)
      print*,'vegfrac read data for month',mnth1
!
      write(sfx,'(i2.2)') mnth2
      infile= trim(vegfracmnth) // sfx // '.ascii'
      open(unit=2,file=infile,status='unknown',form='formatted')
      read(2,1002) ifrcp
      close(2)
      print*,'vegfracprog read data for month',mnth2
!--------------do the time interpolation--------------------------------
      do jdata=1,jdatamax
        do idata=1,idatamax
          ifrc(idata,jdata)=(ifrcp(idata,jdata)-ifrc(idata,jdata))*wght &
                            +ifrc (idata,jdata)
        enddo
      enddo
!--interpolation loop---------------------------------------------------
      do j=1,jmi
        tygrid=(j-1)*dph+sb
        do i=1,imi
          txgrid=(i-1)*dlm+wb
!
          call rtll(txgrid,tygrid,tlm0d,ctph0,stph0,xgrid,ygrid)
!
          jdata=(ygrid-y0)*rdy+1
          if(jdata.lt.1         ) jdata=1
          if(jdata.gt.jdatamax-1) jdata=jdatamax-1
!
          qq=(ygrid-((jdata-1)*dy+y0))*rdy
          jdatap1=jdata+1
!
          deltax=xgrid-x0
          if(deltax.ge.0.) then
            idata=deltax*rdx+1.
            pp=(xgrid-((idata-1)*dx+x0))*rdx
          else
            idata=idatamax*2
            pp=dx+deltax
          endif
!
          if(idata.ne.idatamax*2)  then
            idatap1=idata+1
          else
            idatap1=1
          endif
!
          sum=0.
          ksum=0
!
          if(   ifrc(idata  ,jdata  ).gt.1.) then
            sum=ifrc(idata  ,jdata  )+sum
            ksum=ksum+1
          endif
          if(   ifrc(idatap1,jdata  ).gt.1.) then
            sum=ifrc(idatap1,jdata  )+sum
            ksum=ksum+1
          endif
          if(   ifrc(idata  ,jdatap1).gt.1.) then
            sum=ifrc(idata  ,jdatap1)+sum
            ksum=ksum+1
          endif
          if(   ifrc(idatap1,jdatap1).gt.1.) then
            sum=ifrc(idatap1,jdatap1)+sum
            ksum=ksum+1
          endif
!
          data00=ifrc(idata  ,jdata  )
          data10=ifrc(idatap1,jdata  )
          data01=ifrc(idata  ,jdatap1)
          data11=ifrc(idatap1,jdatap1)
!
          if(ksum.lt.4.and.ksum.gt.0) then
            sum=sum/ksum
            if(data00.lt.1.) &
               data00=sum
            if(data10.lt.1.) &
               data10=sum
            if(data01.lt.1.) &
               data01=sum
            if(data11.lt.1.) &
               data11=sum
!            print*,sum
          endif
!
          vegfrac(i,j)= &
              (data00+(data10-data00)*pp &
                     +(data01-data00)*qq &
                     +(data00-data10-data01+data11)*pp*qq)*0.01

!print*,'i,j',i,j,'idata,idatap1,jdata,jdatap1',idata,idatap1,jdata,jdatap1
!print*,'x,y,data',xgrid,ygrid,data00,data10,data01,data11
!print*,'pp,qq,snoalb',pp,qq,vegfrac(i,j)
!stop
        enddo
!stop
      enddo
!--read in the sea-mask-------------------------------------------------
      infile = param2
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) seamask
      close(1)
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          if(seamask(i,j).gt.0.5) vegfrac(i,j)=0.
        enddo
      enddo
!-----------------------------------------------------------------------
      do j=jmi,1,-10
        write(*,1100) (vegfrac(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile = param3
      open(unit=2,file=outfile,status='unknown',form='unformatted')
      write(2) vegfrac
      close(2)
      print*,'vegfrac written to ../output/vegfrac'
      print*,'Enjoy your vegetation fraction!'
!-----------------------------------------------------------------------
endprogram vegfracprog
!-----------------------------------------------------------------------
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
      parameter(pi=3.1415927,dtr=pi/180.)
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
