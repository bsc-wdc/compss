!-----------------------------------------------------------------------
!
      program conv
!
!-----------------------------------------------------------------------
!
      character*64 fname,infile,outfil,outtmp,outmst
     &            ,outsst,outsno,outcic
      dimension idat(3),month(12),intgrs(32)
      data fname/'                                                      
     &          '/
      data infile/'                                                     
     &           '/
      data outfil/'                                                      
     &           '/
      data outtmp/'
     &           '/
      data outmst/'
     &           '/
      data outsst/'
     &           '/
      data outsno/'
     &           '/
      data outcic/'
     &           '/
      data intgrs/32*0/
      data month/31,28,31,30,31,30,31,31,30,31,30,31/

      character*256 param1,param2,param3,param4,param5,param6,param7
      call getarg(1,param1)
      call getarg(2,param2)
      call getarg(3,param3)
      call getarg(4,param4)
      call getarg(5,param5)
      call getarg(6,param6)
      call getarg(7,param7)

!
!-----------------------------------------------------------------------
!      open(1,file='flist',status='unknown')
!      rewind 1
!-----------------------------------------------------------------------
!
 1000 format(a)
 2000 format(' ***  year=',i4,' month=',i2,' day=',i2,' ihrst=',i2,
     &       ' ihr=',i3,'  ***')
 2100 format(a)
!
!-----------------------------------------------------------------------
!
!              do kread=1,20
!
!-----------------------------------------------------------------------
!***  read the file name
!-----------------------------------------------------------------------
!
!      read(1,2100,end=200) fname
!      write(*,1000) fname
!
      fname = '../output/131140000.gfs'
      do k=1,9
        intgrs(k)=iachar(fname(k+10:k+10))-48
      enddo
!
!-----------------------------------------------------------------------
!***  convert the date to hibu standard
!-----------------------------------------------------------------------
!
      iyer=intgrs(1)*10+intgrs(2)
          if(iyer.gt.20)    then
      iyer=iyer+1900
          else
      iyer=iyer+2000
          endif
!
      jday=intgrs(3)*100+intgrs(4)*10+intgrs(5)
      iday=jday
!
              if(iday.le.31)    then
      mnth=1
              else
      if(mod(iyer,4).eq.0)   month(2)=29
          do months=2,12
      iday=iday-month(months-1)
          if(iday.le.month(months))    then
      mnth=months
      go to 100
          endif
          enddo
              endif
!
!-----------------------------------------------------------------------
 100  continue
!-----------------------------------------------------------------------
!
      idat(1)=iday
      idat(2)=mnth
      idat(3)=iyer
!
!-----------------------------------------------------------------------
!
      ihrst=intgrs(6)*10+intgrs(7)
      ihr=intgrs(8)*10+intgrs(9)
      write(*,2000) iyer,mnth,iday,ihrst,ihr
!
!-----------------------------------------------------------------------
!

      infile=param1
      outfil=param2
      outtmp=param3
      outmst=param4
      outsst=param5
      outsno=param6
      outcic=param7

!
!-----------------------------------------------------------------------
      call io(idat,ihrst,ihr,infile,outfil
     &       ,outtmp,outmst,outsst,outsno,outcic)
!-----------------------------------------------------------------------
!
!      enddo
 200  close(1)
!
!-----------------------------------------------------------------------
!
      stop
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine io(idat,ihrst,ihr,infile,outfil
     &             ,outtmp,outmst,outsst,outsno,outcic)

!-----------------------------------------------------------------------
      include 'include/deco.inc'
!-----------------------------------------------------------------------
!
      logical run
      character*64 infile,outfil,outtmp,outmst,outsst,outsno,outcic
      dimension idat(3)
!-----------------------------------------------------------------------
      dimension gsst(ime,jme)           !  sst
      dimension gsno(ime,jme)           !  sno
      dimension gcic(ime,jme)           !  sea-ice
      dimension pmsl(ime,jme)
      dimension gstmp(4,ime,jme)        !  soil temperature
      dimension gsmst(4,ime,jme)        !  soil moisture
      dimension tp(ime,jme,lme)         !  temperature
      dimension ht(ime,jme,lme)         !  isobaric heights (m)
      dimension uw(ime,jme,lme)         !  isobaric u-wind (lon) (m/s)
      dimension vw(ime,jme,lme)         !  isobaric v-wind (lat) (m/s)
      dimension qh(ime,jme,lme)         !  isobaric spec. humidity (kg/kg)
      dimension ww(ime,jme,lme)         !  condensate (missing)
      dimension sfc(ime,jme,nsfcfld)
                                        !sfc(i,j,1)=PMSL
                                        !sfc(i,j,2,4,6 & 8) are STC
                                        !sfc(i,j,3,5,7 & 9) are SMC
                                        !sfc(i,j,10)=WEASD;slp(i,j,11)=SEAICE
                                        !sfc(i,j,12)=SST/TS

      save gstmp,gsmst,ht,uw,vw,qh,ww,sfc
!
!-----------------------------------------------------------------------
 7777 format(' ',24f5.0)
 8888 format(' ',24f7.0)
 7778 format(' ',24f5.2)
!-----------------------------------------------------------------------
!
      run=.true.
!
!-----------------------------------------------------------------------
!
      open(unit=2,file=infile,status='old',form='unformatted')
!
      read(2) dummy
      read(2) dummy
!
      read(2) (((uw(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((vw(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((tp(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((ht(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((qh(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((ww(i,j,k),i=1,ime),j=1,jme),k=1,lme)
      read(2) (((sfc(i,j,k),i=1,ime),j=1,jme),k=1,nsfcfld)
!
      close(2)
!
      print*,'*** Data read in io from ',infile
      print*,' '
!
!-----------------------------------------------------------------------
      if(ihr.eq.0) then
!-----------------------------------------------------------------------
!
!-----------------------------------------------------------------------
        do l=1,4
!-----------------------------------------------------------------------
!
          tmx=-9999.
          tmn= 9999.
          do  j=1,jme
            do  i=1,ime
              tmx=max(sfc(i,j,2*l),tmx)
              tmn=min(sfc(i,j,2*l),tmn)
              gstmp(l,i,j)=sfc(i,j,2*l)
!
              if(gstmp(l,i,j).eq.0.) then
                write(3,*) l,i,j
              endif
!
            enddo
          enddo
!
          do j=jme,1,-30
            write(*,7777) (gstmp(l,i,j),i=1,ime,50)
          enddo
          print*,'l=',l,' tmin=',tmn,' tmax=',tmx
          print*,' '
!
          wmx=-9999.
          wmn= 9999.
          do  j=1,jme
            do  i=1,ime
              wmx=max(sfc(i,j,2*l+1),wmx)
              wmn=min(sfc(i,j,2*l+1),wmn)
              gsmst(l,i,j)=sfc(i,j,2*l+1)
            enddo
          enddo
!
          do j=jme,1,-30
            write(*,7778) (gsmst(l,i,j),i=1,ime,50)
          enddo
          print*,'l=',l,' wmin=',wmn,' wmax=',wmx
          print*,' '
!
!-----------------------------------------------------------------------
        enddo
!-----------------------------------------------------------------------
!
        do j=1,jme
          do i=1,ime
            gsno(i,j)=sfc(i,j,10)
            gcic(i,j)=sfc(i,j,11)
            gsst(i,j)=sfc(i,j,12)
          enddo
        enddo
!
        do j=jme,1,-30
          write(*,7777) (gsno(i,j),i=1,ime,50)
        enddo
        print*,'gsno'
!
        do j=jme,1,-30
          write(*,8888) (gcic(i,j),i=1,ime,50)
        enddo
        print*,'gcic'
!
        do j=jme,1,-30
          write(*,7777) (gsst(i,j),i=1,ime,50)
        enddo
        print*,'gsst'
!-----------------------------------------------------------------------
        open(unit=3,file=outtmp,status='unknown',form='unformatted')
        write(3) gstmp
        close(3)
        print*,'*** Data written in io to ',outtmp
!-----------------------------------------------------------------------
        open(unit=3,file=outmst,status='unknown',form='unformatted')
        write(3) gsmst
        close(3)
        print*,'*** Data written in io to ',outmst
!-----------------------------------------------------------------------
        open(unit=3,file=outsno,status='unknown',form='unformatted')
        write(3) gsno
        close(3)
        print*,'*** Data written in io to ',outsno
!-----------------------------------------------------------------------
        open(unit=3,file=outcic,status='unknown',form='unformatted')
        write(3) gcic
        close(3)
        print*,'*** Data written in io to ',outcic
!-----------------------------------------------------------------------
        open(unit=3,file=outsst,status='unknown',form='unformatted')
        write(3) gsst
        close(3)
        print*,'*** Data written in io to ',outsst
!
!-----------------------------------------------------------------------
      endif
!-----------------------------------------------------------------------
!
        do j=1,jme
          do i=1,ime
            pmsl(i,j)=sfc(i,j,1)
          enddo
        enddo
!
!-----------------------------------------------------------------------
!*** write final outfile
!-----------------------------------------------------------------------
!
      open(unit=3,file=outfil,status='unknown',form='unformatted')
      write(3) run,idat,ihrst,ihr
!
      write(3) ((pmsl(i,j),i=1,ime),j=1,jme)
      write(3) (((ht(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)
      write(3) (((tp(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)
      write(3) (((uw(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)
      write(3) (((vw(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)
      write(3) (((qh(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)
      write(3) (((ww(i,j,k),i=1,ime),j=1,jme),k=lme,1,-1)

      close(3)
!
      print*,'*** Data written in io to ',outfil
      print*,' '
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
