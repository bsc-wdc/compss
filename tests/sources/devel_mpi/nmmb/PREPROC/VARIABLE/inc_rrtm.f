!-----------------------------------------------------------------------
!
      program inc_hires
!
!-----------------------------------------------------------------------
!!! KARSTEN
      include 'include/llgrid.inc'
!!! KARSTEN
!-----------------------------------------------------------------------
      character*64 fname,infile
      data fname/'                                                    '/
      data infile/'                                                   '/
!-----------------------------------------------------------------------
      character*2 gproj
      real*4 alat1,alat2,alon0
!-----------------------------------------------------------------------
!

      character*256 param1,param2
      call getarg(1,param1)
      call getarg(2,param2)

 1000 format(a)
 2000 format(' ***  year=',i4,' month=',i2,' day=',i2,' ihrst=',i2,
     &       ' ihr=',i3,'  ***')
 2100 format(a)
 2200 format('      parameter',/,
     &       '     &(bowest=',f3.1,',bosout=',f10.5,/,
     &       '     &,boeast=',f9.4,',bonort=',f9.5,/,
     &       '     &,delon=',f7.4,',delat=',f7.4,/,
     &       '     &,ime=',i4,',jme=',i3,',lme=',i3,/,
     &       '     &,nsfcfld=',i3,',tboco=',f4.1,')')
! 
!-----------------------------------------------------------------------
!
!      open(1,file='flist',status='unknown')
!
!-----------------------------------------------------------------------
!***  read the first file name
!-----------------------------------------------------------------------
!
!      read(1,2100,end=200) fname
!      write(*,1000) fname
!      infile=fname(1:38)

      open(unit=2,file=param1,status='old',form='unformatted')
!
!!! KARSTEN
      read(2) nx2,ny2,ldm,nx2,ny2,ip,jp,nsfcfld,gproj
      print*,nx2,ny2,ldm,nx2,ny2,ip,jp,nsfcfld,gproj
      read(2) nx2,ny2,ldm,alat1,alat2,alon0!,sw,ne
      print*,nx2,ny2,ldm,alat1,alat2,alon0!,sw,ne
!!! KARSTEN
!
      close(2)
!
!-----------------------------------------------------------------------
!
!!! KARSTEN
!      bowest=0.
!      boeast=359.5
!      bosout=-90.00000
!      bonort=90.00000
!      delon=0.5
!      delat=0.5
!      ime=(boeast-bowest)/delon+1.5
!      jme=(bonort-bosout)/delat+1.5
      if(nz.eq.26) then
        ime=(boeast-bowest)/delon+1
        jme=(bonort-bosout)/delat+1
      elseif(nz.eq.47) then
        ime=(boeast-bowest)/delon+1.5
        jme=(bonort-bosout)/delat+1.5
      endif
!      tboco=3.
!
          if(nx2.ne.ime.or.ny2.ne.jme)    then
      print*,'*** Something is wrong with geometry.'
      print*,nx2,ime,ny2,jme
          else
      print*,'*** Geometry OK ',ime,jme
          endif
!!! KARSTEN
!
!----------------------------------------------------------------------
!
      open(unit=3,file=param2
     &     ,status='unknown',form='formatted')
      write(3,2200) bowest,bosout
     &             ,boeast,bonort
     &             ,delon,delat
     &             ,ime,jme,ldm
     &             ,nsfcfld,tboco
      close(3)
      print*,'*** Include file created from data read from ',infile
!
!----------------------------------------------------------------------
      stop
!----------------------------------------------------------------------
!
 200  print*,'*** No data.'
!
!----------------------------------------------------------------------
      stop
!----------------------------------------------------------------------
!
      end      
!
!----------------------------------------------------------------------
