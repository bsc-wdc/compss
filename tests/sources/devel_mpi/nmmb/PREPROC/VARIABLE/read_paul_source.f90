! This is part of the netCDF package.
! Copyright 2006 University Corporation for Atmospheric Research/Unidata.
! See COPYRIGHT file for conditions of use.
      
! This is a simple example which reads a small dummy array, from a
! netCDF data file created by the companion program simple_xy_wr.f90.
      
! This is intended to illustrate the use of the netCDF fortran 77
! API. This example program is part of the netCDF tutorial, which can
! be found at:
! http://www.unidata.ucar.edu/software/netcdf/docs/netcdf-tutorial
      
! Full documentation of the netCDF Fortran 90 API can be found at:
! http://www.unidata.ucar.edu/software/netcdf/docs/netcdf-f90

! $Id: simple_xy_rd.f90,v 1.7 2006/12/09 18:44:58 russ Exp $

program read_paul_source
  use netcdf 
  implicit none
  include 'modelgrid.inc'
  include 'ginouxgrid.inc'  

  ! This is the name of the data file we will read. 
  character (len = *), parameter :: FILE_NAME = "dust_source_0.25x0.25.nc"

  ! We are reading 2D data, a 6 x 12 grid. 
  integer, parameter :: NX = 1440, NY = 720
  real :: source(1440,720), source_interp(imi,jmi),lat(imi),lon(imi)
  real :: lats(NY), lons(NX)

  ! This will be the netCDF ID for the file and data variable.
  integer :: ncid, ncid2, varid, lat_varid, lon_varid, rlat_varid,rlon_varid
  integer :: lon_dimid, lat_dimid, south_north(jmi),west_east(imi)
  ! Loop indexes, and error handling.
  integer :: x, y, i, j
!
  real :: almd,aphd,tlm,tph,wb,sb,tph0,ctph0,stph0,dlm,dph
  real :: coh(3,imi,jmi),sm(imi,jmi)
  integer :: inh(4,imi,jmi),jnh(4,imi,jmi)

  logical :: global
!
  real, parameter :: dtr=3.1415926535897932384626433832795/180., rtd=1./dtr
!
  integer :: source_interp_id
!
  integer :: dimids(2)
!
  character (len = *), parameter :: LAT_NAME = "latitude"
  character (len = *), parameter :: LON_NAME = "longitude"
  real :: rlat(imi,jmi), rlon(imi,jmi)
  character (len = *), parameter :: UNITS = "units"
  character (len = *), parameter :: LAT_UNITS = "degrees_north"
  character (len = *), parameter :: LON_UNITS = "degrees_east"

!-----------------------------------------------------------------------
!
  global=im.gt.imi
!
!-----------------------------------------------------------------------


  ! Open the file. NF90_NOWRITE tells netCDF we want read-only access to
  ! the file.
  call check( nf90_open(FILE_NAME, NF90_NOWRITE, ncid) )

  ! Get the varids of the latitude and longitude coordinate variables.
  call check( nf90_inq_varid(ncid, "lat", lat_varid) )
  call check( nf90_inq_varid(ncid, "lon", lon_varid) )

  ! Read the latitude and longitude data.
  call check( nf90_get_var(ncid, lat_varid, lats) )
  call check( nf90_get_var(ncid, lon_varid, lons) )

!  print *, lats, lons

  ! Get the varid of the data variable, based on its name.
  call check( nf90_inq_varid(ncid, "source", varid) )

!  print*, "carlos"

  ! Read the data.
!  call check( nf90_get_var(ncid, varid, source, start=(/1,1/), count=(/y,x/)) )
  call check( nf90_get_var(ncid, varid, source) )

! print*, "carlos2"

  ! Check the data.
 ! do x = 1, NX
 !    do y = 1, NY
 !       if (source(x, y) /= 0. ) then
 !          print *, "source(", x, ", ", y, ") = ", source(x, y)
!!           stop "Stopped"
 !       end if
 !    end do
 ! end do

  ! Close the file, freeing all resources.
  call check( nf90_close(ncid) )

  print *,"*** SUCCESS reading example file ", FILE_NAME, "! "


!-----------------------------------------------------------------------
!***  umo domain geometry
!-----------------------------------------------------------------------
!
      wb=wbd*dtr
      sb=sbd*dtr
      tph0=tph0d*dtr
      ctph0=cos(tph0)
      stph0=sin(tph0)
      dlm=dlmd*dtr
      dph=dphd*dtr
!
      call preina (global,wb,sb,tlm0d,ctph0,stph0,dlm,dph,imi,jmi,coh,inh,jnh)
!
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,source,source_interp)

      open(unit=3,file='../output/seamask',status='unknown',form='unformatted')
      read(3) sm
      close(3)      

      do i=1,imi
      do j=1,jmi
!
       if(sm(i,j).gt.0.5) source_interp(i,j)=0.
! 
      enddo
      enddo      


      open(unit=1,file='../output/source',status='unknown',form='unformatted')
      write(1)source_interp
      close(1)

!       print*, jnh

!      do j=1,jmll,50
!        write(*,7777) (source(i,jmll+1-j),i=1,imll,75)
!      enddo

!      do j=1,jmi,1
!        write(*,7777) (source_interp(i,jmi+1-j),i=1,imi,1)
!      enddo

7777 format(' ',20f6.2)


      tph=sb-dph
      do j=1,jmi
        tph=tph+dph
        tlm=wb-dlm
        do i=1,imi
        tlm=tlm+dlm
!
!-----------------------------------------------------------------------
!***  tll to ll conversion
!-----------------------------------------------------------------------
!
       if(.not.global) then
         call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
       else
         almd=tlm*rtd
         aphd=tph*rtd
       endif

        rlat(i,j)=aphd
        rlon(i,j)=almd


      enddo
    enddo

    do i=1,jmi
      south_north(i)=i
    enddo

    do i=1,imi
      west_east(i)=i
    enddo



      
  ! Create the netCDF file. The nf90_clobber parameter tells netCDF to
  ! overwrite this file, if it already exists.
  call check( nf90_create("source_out.nc", NF90_CLOBBER, ncid) )


  ! Define the dimensions. NetCDF will hand back an ID for each. 
  call check( nf90_def_dim(ncid, "west_east", imi, lon_dimid) )
  call check( nf90_def_dim(ncid, "south_north", jmi, lat_dimid) )

  ! The dimids array is used to pass the IDs of the dimensions of
  ! the variables. Note that in fortran arrays are stored in
  ! column-major format.
  dimids =  (/ lon_dimid, lat_dimid /)

  ! Define the variable. The type of the variable in this case is
  ! NF90_INT (4-byte integer).
  call check( nf90_def_var(ncid, "source", NF90_REAL, dimids, varid) )

! if(.not.global) then
  call check( nf90_put_att(ncid, varid, "coordinates", "lon lat") )
  call check( nf90_def_var(ncid, "west_east", NF90_INT, lon_dimid, lon_varid) )
  call check( nf90_def_var(ncid, "south_north", NF90_INT, lat_dimid, lat_varid) )
  call check( nf90_def_var(ncid, "lat", NF90_REAL, dimids, rlat_varid) )
  call check( nf90_def_var(ncid, "lon", NF90_REAL, dimids, rlon_varid) )
  call check( nf90_put_att(ncid, lat_varid, "axis", "Y") )
  call check( nf90_put_att(ncid, lon_varid, "axis", "X") )
  call check( nf90_put_att(ncid, rlat_varid, "units", "degrees_north") )
  call check( nf90_put_att(ncid, rlat_varid, "title", "latitude") )
  call check( nf90_put_att(ncid, rlon_varid, "units", "degrees_east") )
  call check( nf90_put_att(ncid, rlon_varid, "title", "longitude") )
! endif

  ! End define mode. This tells netCDF we are done defining metadata.
  call check( nf90_enddef(ncid) )

  ! Write the pretend data to the file. Although netCDF supports
  ! reading and writing subsets of data, in this case we write all the
  ! data in one operation.
  call check( nf90_put_var(ncid, varid, source_interp) )

! if(.not.global) then
  call check( nf90_put_var(ncid, lat_varid, south_north) )
  call check( nf90_put_var(ncid, lon_varid, west_east) )
  call check( nf90_put_var(ncid, rlat_varid, rlat) )
  call check( nf90_put_var(ncid, rlon_varid, rlon) )
! endif  

  ! Close the file. This frees up any internal netCDF resources
  ! associated with the file, and flushes any buffers.
  call check( nf90_close(ncid) )

  print *, "*** SUCCESS writing example file simple_xy.nc! "







contains
  subroutine check(status)
    integer, intent ( in) :: status
    
    if(status /= nf90_noerr) then 
      print *, trim(nf90_strerror(status))
      stop "Stopped"
    end if
  end subroutine check


  subroutine preina (global,wb,sb,tlm0d,ctph0,stph0,dlm,dph,imi,jmi,coh,inh,jnh)
!
!-----------------------------------------------------------------------
      include 'ginouxgrid.inc'
!-----------------------------------------------------------------------
!
      real(kind=4),parameter ::  dtr=3.1415926535897932384626433832795/180.,rtd=1./dtr
!
!-----------------------------------------------------------------------
!
      logical :: global
      integer :: imi, jmi
      real ::  x,y,coh(3,imi,jmi),almd,aphd,tlm,tph,wb,sb,tlm0d,ctph0,stph0,dlm,dph
      integer :: inh(4,imi,jmi),jnh(4,imi,jmi),indec,jndec
!
!-----------------------------------------------------------------------
!***  entry to the umo i,j loop
!***  neighbour avn index identification (avn data defined in ll system)
!-----------------------------------------------------------------------
!-----------------------------------------------------------------------
!
      tph=sb-dph
              do j=1,jmi
          tph=tph+dph
          tlm=wb-dlm
          do i=1,imi
      tlm=tlm+dlm
!
!-----------------------------------------------------------------------
!***  tll to ll conversion
!-----------------------------------------------------------------------
!
      if(.not.global) then
        call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
      else
        almd=tlm*rtd
        aphd=tph*rtd
      endif
!
!-----------------------------------------------------------------------
!-------------conversion from -180,180 range to 0,360 range-------------
!-----------------------------------------------------------------------
!
!      if(almd.lt.0.) almd=360.+almd    NO NEED HERE
!
!
!-----------------------------------------------------------------------
!-------------check if model point is out of llgrid domain--------------
!-----------------------------------------------------------------------
!
      if(almd.lt.bowest-delon/2      .or.  &
         almd.gt.boeast+delon/2+0.1  ) then
!     &   almd.gt.boeast+delon.or.
!     &   aphd.lt.bosout      .or.
!     &   aphd.gt.bonort          ) then
        print*,'h point i=',i,' j=',j,almd,aphd &
     ,' is out of the llgrid domain SFC2'
        print*,'program will stop'
        stop
      endif
!
!----------------------------------------------------------------------
!
      if(aphd.ge.bosout.or.  &
        aphd.le.bonort) then

      x=almd-bowest
      y=aphd-bosout
!
      indec=x/delon+1
      jndec=y/delat+1
!
      if(indec.eq.0) indec=imll
      if(indec.gt.imll) indec=1
      if(jndec.eq.0) jndec=1
      if(jndec.ge.jmll) jndec=jmll-1
!
      x=x-(indec-1)*delon
      y=y-(jndec-1)*delat
!-----------------------------------------------------------------------
      coh(1,i,j)=x/delon
      coh(2,i,j)=y/delat
      coh(3,i,j)=coh(1,i,j)*coh(2,i,j)
!-----------------------------------------------------------------------
!-----------------------------------------------------------------------
      inh(1,i,j)=indec
      inh(3,i,j)=indec
          if(indec.lt.imll) then
      inh(2,i,j)=indec+1
      inh(4,i,j)=indec+1
          else
      inh(2,i,j)=1
      inh(4,i,j)=1
          endif
!
      jnh(1,i,j)=jndec
      jnh(2,i,j)=jndec
      jnh(3,i,j)=jndec+1
      jnh(4,i,j)=jndec+1
     else

      coh(1,i,j)=0.
      coh(2,i,j)=0.
      coh(3,i,j)=coh(1,i,j)*coh(2,i,j)
!-----------------------------------------------------------------------
!-----------------------------------------------------------------------
      inh(1,i,j)=1
      inh(3,i,j)=1
          if(indec.lt.imll) then
      inh(2,i,j)=1
      inh(4,i,j)=1
          else
      inh(2,i,j)=1
      inh(4,i,j)=1
          endif
!
      jnh(1,i,j)=1
      jnh(2,i,j)=1
      jnh(3,i,j)=1
      jnh(4,i,j)=1
!-----------------------------------------------------------------------

      endif

          enddo
              enddo
!-----------------------------------------------------------------------
      end subroutine
!-----------------------------------------------------------------------

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
!-----------------------------------------------------------------------
!
      real, parameter :: dtr=3.1415926535897932384626433832795/180.
      real :: almd,aphd,tph,tlm,ctph0,stph0,tlm0d,stlm,ctlm,stph,ctph
      real :: sph,aph,anum,denom,relm

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
      relm=atan2(anum,denom)
      almd=relm/dtr+tlm0d
!
      if(almd.gt. 180.)    almd=almd-360.
      if(almd.lt.-180.)    almd=almd+360.
!
!-----------------------------------------------------------------------
!
      end subroutine

      subroutine bilinb(cob,inb,jnb,imll,jmll,imi,jmi,ww,wfb)
!
!-----------------------------------------------------------------------
!
      real :: cob(3,imi,jmi)
      integer :: inb(4,imi,jmi),jnb(4,imi,jmi),imll,jmll,imi,jmi
      real :: ww(imll,jmll),wfb(imi,jmi)
      integer :: i00,i10,i01,i11,j00,j10,j01,j11
      real :: p,q,pq,z
!
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
      +p*(ww(i10,j10)-ww(i00,j00))  &
      +q*(ww(i01,j01)-ww(i00,j00))  &
      +pq*(ww(i00,j00)-ww(i10,j10)-ww(i01,j01)+ww(i11,j11))
!
      wfb(i,j)=z
!
          enddo
              enddo
!-----------------------------------------------------------------------
!
      end subroutine



end program read_paul_source
