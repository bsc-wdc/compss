!
!-----------------------------------------------------------------------
!
      program degrib2model_driver
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
!      implicit none
!
!-----------------------------------------------------------------------
!
      character *2 chr2
      integer      ihr2
!      integer*4 nx,ny,nz,nsi,nsfc       !Degrib grid dimensions
!      real*4 bowest,bosout,boeast,bonort,delon,delat,tboco
!      real*4 imll,jmll,lmll
!
!!! Oriol
      real*4 esat,es
      common /estab/esat(15000:45000),es(15000:45000)
!!! Oriol
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      include 'include/llgrid.inc'
!
!-----------------------------------------------------------------------
!
      ihr2 = 00

!
!!! Oriol
      print*,'Initialize tables'
!
!-----------------------------------------------------------------------
      call es_ini
!-----------------------------------------------------------------------
!!! Oriol
!
!      nx=720               !<-- llgrid.inc
!      ny=361               !<-- llgrid.inc
!      nz=26                !<-- llgrid.inc
!      ns=12                !<-- llgrid.inc
!
       print*,'call degrib2model ',nx,ny,nz,nsfc,ns
!
!-----------------------------------------------------------------------
!     call degrib2model(nx,ny,nz,ns,ihr2,chr2)
      call degrib2model(nx,ny,nz,nsfc,ns,ihr2,chr2
     &                 ,bowest,bosout,boeast,bonort,delon,delat)
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
!      subroutine degrib2model(nx,ny,nz,ns,ihr2,chr2)
      subroutine degrib2model(nx,ny,nz,nsfc,ns,ihr2,chr2
     &                       ,bowest,bosout,boeast,bonort,delon,delat)
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      implicit none
!
!-----------------------------------------------------------------------
!
      real*4 dlat,dlon
      real*4 bowest,bosout,boeast,bonort,delon,delat
!
      integer*4 nx,ny,nz,nsfc,ns,i,j,k,l,ip,jp
      integer*4 idate,ihr,ihr2,idy,imo,iyr,ijulday,julday_laps
      character *9 filename
      character *2 chr2
      character *2 gproj
!
      real*4 ht(nx,ny,nz)              !Isobaric heights (m)
     &      ,tp(nx,ny,nz)              !Isobaric temps (K)
     &      ,uw(nx,ny,nz)              !Isobaric u-wind (m/s)
     &      ,vw(nx,ny,nz)              !Isobaric v-wind (m/s)
     &      ,sh(nx,ny,nz)              !Isobaric sh     (kg/kg)
     &      ,cldw(nx,ny,nz)            !Isobaric Cloud water
     &      ,lat1,lon0,sw(2),ne(2)
     &      ,slp(nx,ny,nsfc)        !slp(i,j,1)=PMSL
     &      ,temp(nx,ny)
!OJORBA3
     &      ,temp2(nx,ny,nz)
!OJORBA3
                                  !slp(i,j,2,4,6 & 8) are STC
                                  !slp(i,j,3,5,7 & 9) are SMC
                                  !slp(i,j,10)=WEASD;slp(i,j,11)=SEAICE
                                  !slp(i,j,12)=SST/TS
!
!-----------------------------------------------------------------------
!
!!! Oriol
      real*4 xe,mrsat,esat,es,cp,kappa
     &      ,ex(nx,ny,nz)              !Isobaric Exner
     &      ,pr(nz),pri(nz)            !Isobaric pressures (mb)
     &      ,th(nx,ny,nz)              !Isobaric theta (K)
!
!!! KARSTEN
     &      ,AVNLEVS(26),AVNLEVS2(47)
!!! KARSTEN
!
      parameter(cp=1004.,kappa=287./1004.)
      integer*4 it
!
      data AVNLEVS/1000,975,950,925,900,850,800,750,700,650,
     &              600,550,500,450,400,350,300,250,200,150,
     &              100, 70, 50, 30, 20, 10/
!
      data AVNLEVS2/1000,975,950,925,900,875,850,825,800,775,
     &               750,725,700,675,650,625,600,575,550,525,
     &               500,475,450,425,400,375,350,325,300,275,
     &               250,225,200,175,150,125,100, 70, 50, 30,
     &                20, 10,  7,  5,  3,  2,  1/
!
      common /estab/esat(15000:45000),es(15000:45000)

      character*256 param19
      call getarg(19,param19)

!!! Oriol
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
!      include 'llgrid.inc'
!
!-----------------------------------------------------------------------
!
      print*,'call read_degrib ',nx,ny,nz,nsfc,ns
!
!-----------------------------------------------------------------------
      call read_degrib(nx,ny,nz,nsfc,ns,ht,tp,sh,uw,vw,cldw,slp,chr2)
!-----------------------------------------------------------------------
!
!OJORBA3
! Change vertical coordinates if required ...
! Program expects tp(1)=1000hpa tp(nz)=1hpa
      print *,ht(nx/2,ny/2,1),ht(nx/2,ny/2,nz)
     & ,(ht(nx/2,ny/2,1).gt.ht(nx/2,ny/2,nz))
      if(ht(nx/2,ny/2,1).gt.ht(nx/2,ny/2,nz))then
!do the vertical flip
      do k=1,nz
      temp2(:,:,nz-k+1)=tp(:,:,k)
      enddo
      tp(:,:,:)=temp2(:,:,:)
      do k=1,nz
      temp2(:,:,nz-k+1)=uw(:,:,k)
      enddo
      uw(:,:,:)=temp2(:,:,:)
      do k=1,nz
      temp2(:,:,nz-k+1)=vw(:,:,k)
      enddo
      vw(:,:,:)=temp2(:,:,:)
      do k=1,nz
      temp2(:,:,nz-k+1)=ht(:,:,k)
      enddo
      ht(:,:,:)=temp2(:,:,:)
!sh and cldw 1 to ns = values
      temp2=0.
      do k=1,ns
      temp2(:,:,ns-k+1)=sh(:,:,k)
      enddo
      sh(:,:,:)=temp2(:,:,:)
      temp2=0.
      do k=1,ns
      temp2(:,:,ns-k+1)=cldw(:,:,k)
      enddo
      cldw(:,:,:)=temp2(:,:,:)
      endif
!OJORBA3
!!! Oriol
!-----------------------------------------------------------------------
!***  introduction of rh to specific humidity
!-----------------------------------------------------------------------
!
      if(nz.eq.26) then
        do k=1,nz
!Cmp         pr(k)=1025.-float(k*25)
          pr(k)=AVNLEVS(k)
          write(6,*) 'pressure value of ', pr(k)
        enddo
      elseif(nz.eq.47) then
        do k=1,nz
!Cmp         pr(k)=1025.-float(k*25)
          pr(k)=AVNLEVS2(k)
          write(6,*) 'pressure value of ', pr(k)
        enddo
      endif
!
!-----------------------------------------------------------------------
!***  Convert 3d temp to theta.
!***  Compute Exner function.
!***  Convert 3d rh to mr.
!-----------------------------------------------------------------------
!
      do k=1,nz
         pri(k)=1./pr(k)
      enddo
!
      do k=1,nz
      do j=1,ny
      do i=1,nx
         th(i,j,k)=tp(i,j,k)*(1000.*pri(k))**kappa
         ex(i,j,k)=cp*tp(i,j,k)/th(i,j,k)
         it=tp(i,j,k)*100
         it=min(45000,max(15000,it))
         xe=esat(it)
         mrsat=0.00622*xe/pr(k)                                             !<-- specific humidity
!!! Oriol         rh(i,j,k)=rh(i,j,k)*mrsat
         sh(i,j,k)=sh(i,j,k)*mrsat
      enddo
      enddo
      enddo
!!! Oriol
!
      print*,'flipping arrays '
!
!-----------------------------------------------------------------------
      do k=1,nz
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=tp(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        tp(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=uw(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        uw(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=vw(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        vw(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=ht(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        ht(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=sh(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        sh(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=cldw(i,j,k)
      enddo
      enddo
!
      do J=1,ny
      do i=1,nx
        cldw(i,j,k)=temp(i,ny-J+1)
      enddo
      enddo
!
!-----------------------------------------------------------------------
      enddo                                                                 !<-- k loop
!-----------------------------------------------------------------------
!
      do k=1,nsfc
!
      do j=1,ny
      do i=1,nx
        temp(i,j)=slp(i,j,k)
      enddo
      enddo

      do j=1,ny
      do i=1,nx
        slp(i,j,k)=temp(i,ny-j+1)
      enddo
      enddo
!
      enddo
!
!-----------------------------------------------------------------------
!***  create filename
!-----------------------------------------------------------------------
!
!      open(22,file='date.txt',form='formatted')
!      read(22,*)idate
!      close(22)
!
!      iyr=idate/1000000
!      imo=(idate-iyr*1000000)/10000
!      idy=(idate-iyr*1000000-imo*10000)/100
!      ihr=(idate-iyr*1000000-imo*10000-idy*100)/1
!      ijulday=julday_laps(idy,imo,iyr)
!
!      write(filename,'(i2.2,i3.3,i2.2,i2.2)')
!     &   iyr,ijulday,ihr,ihr2
!
!-----------------------------------------------------------------------
!
      print*,'WRITE new file '//filename//'.gfs'
!
!-----------------------------------------------------------------------
!***  Open temporary output file
!-----------------------------------------------------------------------
!
      open(1,file = param19
     & ,status='unknown',form='unformatted',CONVERT='BIG_ENDIAN')
!
!-----------------------------------------------------------------------
!***  Set up input grid parameters.
!***  Will be printed in deco.inc later on.
!-----------------------------------------------------------------------
!
      ip=1
      jp=1
      gproj='LL'
!
      write(1) nx,ny,nz,nx,ny,ip,jp,nsfc,gproj
!
      lat1=bosout                 !<-- llgrid.inc
      lon0=bowest                 !<-- llgrid.inc
      sw(1)=bosout                !<-- llgrid.inc
      sw(2)=bowest                !<-- llgrid.inc
      ne(1)=bonort                !<-- llgrid.inc
      ne(2)=boeast                !<-- llgrid.inc
      dlat=delat                  !<-- llgrid.inc
      dlon=delon                  !<-- llgrid.inc
!
!      lat1=-90.                                                             !<-- -89.75
!      lon0=0.
!      sw(1)=-90.                                                            !<-- -89.75
!      sw(2)=0.                                                              !<--   0.25
!      ne(1)=90.                                                             !<--  89.75
!      ne(2)=359.5                                                           !<-- 359.75
!      dlat=0.5
!      dlon=0.5
!
      write(1) nx,ny,nz,lat1,lon0,dlat,dlon
!
!-----------------------------------------------------------------------
!***  Write isobaric upper air data.
!-----------------------------------------------------------------------
!
      write(1) uw                                                           !<-- field U (nz)
      write(1) vw                                                           !<-- field V (nz)
      write(1) tp                                                           !<-- field TEMP (nz)
      write(1) ht                                                           !<-- field H (nz)
      write(1) sh                                                           !<-- field SH (nz)
      write(1) cldw                                                         !<-- field CLW (nz)
      write(1) slp                                                          !<-- field SLP (ns) ... which contains:
!                                                                           !<-- SST/TS, SOILT1...4, SOILW1...4, SNOW, SEAICE, PRMSL
!-----------------------------------------------------------------------
!
      close(1)
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
      subroutine read_degrib(nx,ny,nz,nsfc,ns
     &                      ,ht,tp,sh,uw,vw,cldw,slp,chr2)
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      implicit none
!
!-----------------------------------------------------------------------
!
      integer*4 nx,ny,nz,i,j,k,nsfc,ns
      character *2 chr2
!
      real*4 ht(nx,ny,nz)                                                   !<-- Isobaric heights (m)
     &      ,tp(nx,ny,nz)                                                   !<-- Isobaric temps (K)
     &      ,uw(nx,ny,nz)                                                   !<-- Isobaric u-wind (m/s)
     &      ,vw(nx,ny,nz)                                                   !<-- Isobaric v-wind (m/s)
     &      ,sh(nx,ny,nz)                                                   !<-- Isobaric sh (kg/kg)
     &      ,cldw(nx,ny,nz)                                                 !<-- Isobaric Cloud water
     &      ,slp(nx,ny,nsfc)
     &      ,temp(nx,ny)

      character*256 param1,param2,param3,param4,param5,param6,param7
      character*256 param8,param9,param10,param11,param12,param13
      character*256 param14,param15,param16,param17,param18

!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
!-----------------------------------------------------------------------
!***  Fill a missing value flag into first space of each variable.
!-----------------------------------------------------------------------
!
      print*,'in read_degrib ',nx,ny,nz,nsfc,ns
!
!-----------------------------------------------------------------------
!

      call getarg(1,param1)
      call getarg(2,param2)
      call getarg(3,param3)
      call getarg(4,param4)
      call getarg(5,param5)
      call getarg(6,param6)
      call getarg(7,param7)
      call getarg(8,param8)
      call getarg(9,param9)
      call getarg(10,param10)
      call getarg(11,param11)
      call getarg(12,param12)
      call getarg(13,param13)
      call getarg(14,param14)
      call getarg(15,param15)
      call getarg(16,param16)
      call getarg(17,param17)
      call getarg(18,param18)
      
!
      do k=1,nz
      do j=1,ny
      do i=1,nx
        ht(i,j,k)=-99999.
        tp(i,j,k)=-99999.
        sh(i,j,k)=-99999.
        uw(i,j,k)=-99999.
        vw(i,j,k)=-99999.
        cldw(i,j,k)=-99999.
      enddo
      enddo
      enddo
!
      do k=1,nsfc
      do j=1,ny
      do i=1,nx
        slp(i,j,k)=-99999.
      enddo
      enddo
      enddo
!
!-----------------------------------------------------------------------
!***  Read surface data.
!-----------------------------------------------------------------------
!
      print*,'READ surface '
!
!-----------------------------------------------------------------------
!
      open(11,file=param11,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,1)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param12,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,2)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param14,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,3)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param4,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,4)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param6,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,5)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param13,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,6)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param15,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,7)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param5,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,8)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param7,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,9)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param18,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,10)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param2,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,11)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
      open(11,file=param16,status='old'
     &       ,form='unformatted')
      read(11)temp
        do j=1,ny
        do i=1,nx
         slp(i,j,12)=temp(i,j)
        enddo
        enddo
      close(11)
!
!-----------------------------------------------------------------------
!
! 1206 format(' ',24f7.1)
!      write(*,*)' '
!      write(*,*)'-------------- PRMSL -------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,1),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILT1 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,2),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILW1 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,3),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILT2 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,4),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILW2 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,5),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILT3 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,6),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILW3 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,7),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILT4 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,8),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SOILW4 ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,9),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- WEASD -------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,10),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- ICEC --------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,11),i=1,nx,20)
!      enddo
!      write(*,*)'-------------- SST_TS ------------------'
!      do j=ny,1,-10
!        write(*,1206) (slp(i,j,12),i=1,nx,20)
!      enddo
!      write(*,*)' '
!
!-----------------------------------------------------------------------
!***  Reading upper field
!-----------------------------------------------------------------------
!
      print*,'READ upper fields '
!
!-----------------------------------------------------------------------
!
      write(*,*)'------------- TEMP -----------------'
!
      open(11,file=param8,status='old'
     &       ,form='unformatted')
        do k=1,nz
      read(11)temp
        do j=1,ny
        do i=1,nx
         tp(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7777) (tp(i,j,1),i=1,nx,20)
      enddo
 7777 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'-------------- UW ------------------'
!
      open(11,file=param17,status='old'
     &       ,form='unformatted')
        do k=1,nz
      read(11)temp
        do j=1,ny
        do i=1,nx
         uw(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7778) (uw(i,j,1),i=1,nx,20)
      enddo
 7778 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'-------------- VW ------------------'
!
      open(11,file=param9,status='old'
     &       ,form='unformatted')
        do k=1,nz
      read(11)temp
        do j=1,ny
        do i=1,nx
         vw(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7780) (vw(i,j,1),i=1,nx,20)
      enddo
 7780 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'-------------- HT ------------------'
!
      open(11,file=param10,status='old'
     &       ,form='unformatted')
        do k=1,nz
      read(11)temp
        do j=1,ny
        do i=1,nx
         ht(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7781) (ht(i,j,1),i=1,nx,20)
      enddo
 7781 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'-------------- SH ------------------'
!
      open(11,file=param3,status='old'
     &       ,form='unformatted')
!!! Oriol        do k=1,nz-10
!!! KARSTEN      do k=1,nz-5
        do k=1,ns
      read(11)temp
        do j=1,ny
        do i=1,nx
         sh(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7782) (sh(i,j,1),i=1,nx,20)
      enddo
 7782 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'------------- CLDW -----------------'
!
      open(11,file=param1,status='old'
     &       ,form='unformatted')
!!! Oriol        do k=1,nz-10
!!! KARSTEN      do k=1,nz-5
        do k=1,ns
      read(11)temp
        do j=1,ny
        do i=1,nx
         cldw(i,j,k)=temp(i,j)
        enddo
        enddo
        enddo
      close(11)
!
      do j=ny,1,-10
        write(*,7783) (cldw(i,j,1),i=1,nx,20)
      enddo
 7783 format(' ',40f5.1)
!
!-----------------------------------------------------------------------
!
      write(*,*)'------------------------------------'
!
!-----------------------------------------------------------------------
!
!!! Oriol        do k=nz-9,nz
!!! KARSTEN      do k=nz-4,nz
        do k=ns+1,nz
        do j=1,ny
        do i=1,nx
         cldw(i,j,k)=0.
         sh(i,j,k)=0.
        enddo
        enddo
        enddo
!
!-----------------------------------------------------------------------
!
       print*,'end READ '
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
      function julday_laps(day,month,year)
!
!-----------------------------------------------------------------------
!
      implicit  none
!
      integer*4 julday_laps
     &         ,day,month,year
     &         ,ndays(12),i
!
      data ndays/31,28,31,30,31,30,31,31,30,31,30,31/
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      julday_laps=0
      do i=1,month-1
         julday_laps=julday_laps+ndays(i)
      enddo
      julday_laps=julday_laps+day
      if (mod(year,4) .eq. 0 .and. month .gt. 2)
     &   julday_laps=julday_laps+1
      return
!
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
!!! Oriol
      subroutine es_ini
!
!-----------------------------------------------------------------------
!
      common /estab/esat(15000:45000),es(15000:45000)
!
!-----------------------------------------------------------------------
!***  Create tables of the saturation vapour pressure with up to
!***  two decimal figures of accuraccy.
!-----------------------------------------------------------------------
!
      do it=15000,45000
        t=it*0.01
        p1 = 11.344-0.0303998*t
        p2 = 3.49149-1302.8844/t
        c1 = 23.832241-5.02808*alog10(t)
        esat(it) = 10.**(c1-1.3816E-7*10.**p1+
     &             8.1328E-3*10.**p2-2949.076/t)
        es(it) = 610.78*exp(17.269*(t-273.16)/(t-35.86))
      enddo
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!!! Oriol


