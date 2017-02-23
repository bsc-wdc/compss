!------ llgrid05.inc for 0.5 deg new albedo data-------------------------------
      real(kind=4),parameter :: &
     bowest=0.5,bosout=-89.5,boeast=359.5,bonort=89.5  &
     ,delon=1.0,delat=1.0
!-----------------------------------------------------------------------
      integer (kind=4), parameter :: &
     imll=(boeast-bowest)/delon+1,jmll=(bonort-bosout)/delat+1
!------ llgrid05.inc - end ---------------------------------------------

