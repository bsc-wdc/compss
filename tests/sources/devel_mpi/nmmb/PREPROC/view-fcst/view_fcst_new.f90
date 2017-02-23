program view_fcst
  use netcdf
  implicit none 
!how to compile
! xlf90 -q32 -o view_fcst-high44.x view_fcst_new.f90 -I/gpfs/apps/NETCDF/32/include -L/gpfs/apps/NETCDF/32/lib -lnetcdf
!
  include 'netcdf.inc'
!-----------------------------------------------------------------------
!  integer, parameter :: im=1021, jm=701, lm=40, nwets=4
  integer, parameter :: im=259, jm=183, lm=40, nwets=4
  integer, parameter :: nfields=29, ndyn=6
!
  logical :: run,global
  integer :: i, l, fcst_ncid, stat
  integer :: lx_id, ly_id, lm_id, wets_id
  integer, dimension(3) :: idat
  integer :: ihrst,ihrend,ntsd
  integer :: pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
  integer :: dzsoil_id
  integer :: i_par_sta,j_par_sta,imm,jmm,lmm,lnsh
  real    :: dlmd,dphd,wbd,sbd,tlm0d,tph0d
  real    :: pt
  real,dimension(lm)    :: dzsoil
!
  integer, dimension(nfields) ::  field_id
  real,dimension(im,jm,nfields) :: field
  integer, dimension(im,jm,nfields) :: ifield
  character(10), dimension(nfields) :: field_name
!
  integer, dimension(ndyn) ::  dyn_id
!  real,dimension(im,jm,lm,nfields) :: dyn
  real,dimension(im,jm,lm) :: dyn
  character(10), dimension(ndyn) :: dyn_name
!
  real,dimension(nwets,im,jm) :: stmp, smst, sh20
  integer                     :: stmp_id, smst_id, sh20_id
 
  field_name=(/'fis       ', 'stdh      ', 'sm        ', 'pd        ', 'albedo    ', &
               'albase    ',  &
               'albedrrtm1', 'albedrrtm2', 'albedrrtm3', 'albedrrtm4', 'albedrrtm5', &
               'albedrrtm6',                                                         &
               'emissivity', 'snowalbedo', 'tskin     ', 'sst       ', 'snow      ', &
               'snowheight', 'cice      ', 'deeptemp  ', 'canopwater', 'fz_prec_ra', &
               'ustar     ', 'z0        ', 'z0base    ', & !'stdh2     ', &
               'topsoiltyp', 'landuse   ', 'landnewcrr', &
               'vegfraccrr'/)

  dyn_name=(/'u         ', 'v         ', 't         ', 'q         ', 'w         ' ,  &
             'o3        '/)

      print*, 'im, jm, lm: ', im, jm, lm

      stat=nf90_create('fcst.nc', nf90_write,fcst_ncid)
      call check_stat(stat,'Opening output/fcst.nc ')

      stat=nf90_def_dim(fcst_ncid, 'i_dim',im,lx_id)
      stat=nf90_def_dim(fcst_ncid, 'j_dim',jm,ly_id)
      stat=nf90_def_dim(fcst_ncid, 'l_dim',lm,lm_id)
      stat=nf90_def_dim(fcst_ncid, 'nwets',nwets,wets_id)
      call check_stat(stat,'Defining dimensions ')

      do i=1, nfields
         if(field_name(i).eq.'topsoiltyp' .or. field_name(i).eq.'landuse   '  &
              .or. field_name(i).eq.'landnewcrr') then
            stat=nf90_def_var(fcst_ncid,field_name(i),NF90_INT,(/lx_id,ly_id/),field_id(i))
            call check_stat(stat,'Defining integer fields')
         else
            stat=nf90_def_var(fcst_ncid,field_name(i),NF90_FLOAT,(/lx_id,ly_id/),field_id(i))
            call check_stat(stat,'Defining real fields')
         endif
      enddo

      do i=1, ndyn
         stat=nf90_def_var(fcst_ncid,dyn_name(i),NF90_FLOAT,(/lx_id,ly_id,lm_id/),dyn_id(i))
         call check_stat(stat,'Defining dynamic fields')
      enddo
      
      stat=nf90_def_var(fcst_ncid,'stmp',NF90_FLOAT,(/wets_id,lx_id,ly_id/),stmp_id)
      call check_stat(stat,'Defining stmp')
      stat=nf90_def_var(fcst_ncid,'smst',NF90_FLOAT,(/wets_id,lx_id,ly_id/),smst_id)
      call check_stat(stat,'Defining smst')
      stat=nf90_def_var(fcst_ncid,'sh20',NF90_FLOAT,(/wets_id,lx_id,ly_id/),sh20_id)
      call check_stat(stat,'Defining sh20')
      stat=nf90_def_var(fcst_ncid, 'dzsoil', NF90_FLOAT,(/lm_id/), dzsoil_id)
      call check_stat(stat,'Defining pd')

      stat=nf90_enddef(fcst_ncid)

      open(unit=21,file='fcst',status='old',form='unformatted')

      read(21) run,idat,ihrst,ihrend,ntsd
      read(21) pt,pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2

      print*, run,idat,ihrst,ihrend,ntsd
      print*, pt,pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2

      read(21) i_par_sta,j_par_sta
      read(21) dlmd,dphd,wbd,sbd,tlm0d,tph0d
      read(21) imm,jmm,lmm,lnsh

      print *,i_par_sta,j_par_sta
      print *,dlmd,dphd,wbd,sbd,tlm0d,tph0d
      print *,imm,jmm,lmm,lnsh

      do i=1,4
         read(21) field(:,:,i)
         stat=nf90_put_var(fcst_ncid,field_id(i),field(:,:,i),start=(/1,1/),count=(/im,jm/))
         call check_stat(stat,'Writting field ')
         print *,field_name(i)
      enddo

      do i=1,ndyn
         do l=1,lm
            read(21) dyn(:,:,l) !,i)
!            stat=nf90_put_var(fcst_ncid,dyn_id(i),dyn(:,:,l,i),start=(/1,1,l/),count=(/im,jm,1/))
            stat=nf90_put_var(fcst_ncid,dyn_id(i),dyn(:,:,l),start=(/1,1,l/),count=(/im,jm,1/))
            call check_stat(stat,'Writting dynamic field ')
         enddo
         print *,dyn_name(i)
      enddo

      do i=5,nfields-4
         read(21) field(:,:,i)
         stat=nf90_put_var(fcst_ncid,field_id(i),field(:,:,i),start=(/1,1/),count=(/im,jm/))
         call check_stat(stat,'Writting field ')
         print *,field_name(i)
      enddo

      read(21) stmp
      stat=nf90_put_var(fcst_ncid,stmp_id,stmp,start=(/1,1,1/),count=(/nwets,im,jm/))
      call check_stat(stat,'Writting stmp ')

      read(21) smst
      stat=nf90_put_var(fcst_ncid,smst_id,smst,start=(/1,1,1/),count=(/nwets,im,jm/))
      call check_stat(stat,'Writting smst ')

      read(21) sh20
      stat=nf90_put_var(fcst_ncid,sh20_id,sh20,start=(/1,1,1/),count=(/nwets,im,jm/))
      call check_stat(stat,'Writting sh20 ')

      do i=nfields-3,nfields 
         read(21) ifield(:,:,i)
         stat=nf90_put_var(fcst_ncid,field_id(i),ifield(:,:,i),start=(/1,1/),count=(/im,jm/))
         call check_stat(stat,'Writting field ')
         print *,field_name(i)
      enddo

!     read(21) field(:,:,22)
!     stat=nf90_put_var(fcst_ncid,field_id(i),field(:,:,i),start=(/1,1/),count=(/im,jm/))
!     call check_stat(stat,'Writting field ')
!         print *,field_name(22)

      read(21) dzsoil
      stat=nf90_put_var(fcst_ncid,dzsoil_id,dzsoil,start=(/1/),count=(/lm/))
      call check_stat(stat,'Writting dzsoil ')
               print *,"dzsoil"
      
      stat=nf90_close(fcst_ncid)
!-----------------------------------------------------------------------
      end program
subroutine check_stat(stat,message)
     use netcdf
     implicit none
     include 'netcdf.inc'
     integer, intent(in)        :: stat
     character*100, intent(in)   :: message

     if ( stat /= nf90_NoErr ) then
        print*, 'Error in netcdf while...'
        print*, TRIM(message)
       STOP 
     endif
end subroutine check_stat
