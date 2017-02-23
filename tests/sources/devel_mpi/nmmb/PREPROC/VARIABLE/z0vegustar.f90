program z0ustar
!-----------------------------------------------------------------------
!     z. janjic, aug. 2007
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include 'modelgrid.inc'
!-----------------------------------------------------------------------
character(23):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

integer(kind=4):: &
 i,j

integer(kind=4),dimension(1:imi,1:jmi):: &
 insoil,landuse

real(kind=4),dimension(1:30):: &
 z0vegtbl

real(kind=4),dimension(1:imi,1:jmi):: &
 height &                    !
,seamask &                   !
,stdh &                      !
,ustar &                     !
,vegfrac &                   !
,z0 &                        !
,z0base                      !
!-----------------------------------------------------------------------
data z0vegtbl / &
 1.000,  0.070,  0.070,  0.070,  0.070,  0.150 &
,0.080,  0.030,  0.050,  0.860,  0.800,  0.850 &
,2.650,  1.090,  0.800,  0.001,  0.040,  0.050 &
,0.010,  0.040,  0.060,  0.050,  0.030,  0.001 &
,0.010,  0.150,  0.010,  0.000,  0.000,  0.000/
!--the following are modified by the ecmwf values for forests-----------
! 1.000,  0.070,  0.070,  0.070,  0.070,  0.150 &
!,0.080,  0.030,  0.050,  0.860,  2.000,  2.000 &
!,4.000,  2.000,  2.000,  0.001,  0.040,  0.050 &
!,0.010,  0.040,  0.060,  0.050,  0.030,  0.001 &
!,0.010,  0.150,  0.010,  0.000,  0.000,  0.000/
!-----------------------------------------------------------------------
 1100 format(25f5.0)
 1200 format(25f5.4)
      infile='                                                         '
      outfile='                                                        '
!--read in the sea-mask-------------------------------------------------
      infile ='../output/seamask'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) seamask
      close(1)
!--read in the landuse data---------------------------------------------
      infile ='../output/landuse'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) landuse
      close(1)
!--read in top soil data------------------------------------------------
      infile ='../output/topsoiltype'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) insoil
      close(1)
!--read in the topography data------------------------------------------
      infile ='../output/height'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) height
      close(1)
!--read in the topography standard deviation data-----------------------
      infile ='../output/stdh'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) stdh
      close(1)
!--read in the vegetation fraction--------------------------------------
      infile='../output/vegfrac'
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) vegfrac
      close(1)
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          ustar(i,j)=0.1
          if(seamask(i,j).gt.0.5.or. &
             insoil (i,j).eq.16 .or. &
             landuse(i,j).eq.24     ) then
            z0base(i,j)=0.0013
            z0    (i,j)=0.0013
          else
!            z0base(i,j)=0.1+max(height(i,j),0.)*0.0+stdh(i,j)*0.002 &
!                       +z0vegtbl(landuse(i,j)) !*vegfrac(i,j)
!            z0    (i,j)=z0base(i,j)
            z0base(i,j)=0.1 &
                       +z0vegtbl(landuse(i,j))*vegfrac(i,j)
            z0    (i,j)=z0base(i,j)
          endif
        enddo
      enddo
!-----------------------------------------------------------------------
      print*,'z0base'
      do j=jmi,1,-10
        write(*,1100) (z0base(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile='../output/z0base'
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) z0base
      close(1)
!
      print*,'z0base written to ../output/z0base'
!-----------------------------------------------------------------------
      print*,'z0'
      do j=jmi,1,-10
        write(*,1100) (z0(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile='../output/z0'
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) z0
      close(1)
!
      print*,'z0 written to ../output/z0'
!-----------------------------------------------------------------------
      print*,'ustar'
      do j=jmi,1,-10
        write(*,1200) (ustar(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile='../output/ustar'
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) ustar
      close(1)
!
      print*,'ustar written to ../output/ustar'
!----------------------------------------------------------------------
      print*,'Enjoy your z0 and ustar!'
!----------------------------------------------------------------------
endprogram z0ustar
!-----------------------------------------------------------------------
