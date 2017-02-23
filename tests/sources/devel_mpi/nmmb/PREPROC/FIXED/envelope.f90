program envelope
!-----------------------------------------------------------------------
!     z. janjic, feb. 2008
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
real(kind=4),parameter:: &
 stdfac=0.                   !

character(23):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

integer(kind=4):: &
 i,im1,ip1,j

real(kind=4):: &
 d2h

real(kind=4),dimension(1:imi,1:jmi):: &
 height &                    !
,stdh                        !

integer num, length, stat
character*256 input_heightmean, input_stdh, output_height

if (command_argument_count().gt.0) then
  call get_command_argument(1,input_heightmean,length,stat)
  call get_command_argument(2,input_stdh,length,stat)
  call get_command_argument(3,output_height,length,stat)
end if

!-----------------------------------------------------------------------
 1100 format(25f5.0)
      infile='                                                         '
      outfile='                                                        '
!--read in the mean topography height-----------------------------------
      infile = input_heightmean
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) height
      close(1)
!--read in the topography standard deviation data-----------------------
      infile = input_stdh
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) stdh
      close(1)
!-----------------------------------------------------------------------
      print*,'Mean orography'
      do j=jmi,1,-10
        write(*,1100) (height(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
!      do j=2,jmi-1
      do j=1,jmi
        do i=1,imi
!          im1=i-1
!          ip1=i+1
!          if(im1.lt.1  ) im1=imi
!          if(ip1.gt.imi) ip1=1
!
!          d2h=height(i,j-1)+height(im1,j)+height(ip1,j)+height(i,j+1) &
!             -4.*height(i,j)
!          if(d2h.lt.0.) then
            height(i,j)=stdh(i,j)*stdfac+height(i,j)
!          endif
        enddo
      enddo
!-----------------------------------------------------------------------
      print*,'Envelope orography, stdfac=',stdfac
      do j=jmi,1,-10
        write(*,1100) (height(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile = output_height
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) height
      close(1)
!
      print*,'Envelope topography written to ../output/height'
!-----------------------------------------------------------------------
      print*,'Enjoy your envelope topography file!'
!-----------------------------------------------------------------------
endprogram envelope
!-----------------------------------------------------------------------
