!
!-----------------------------------------------------------------------
!***  modelgrid.inc -  START
!-----------------------------------------------------------------------
!
!-----------------------------------------------------------------------
!***  model grid domain boundaries
!-----------------------------------------------------------------------
!
real(kind=4),parameter :: tlm0d=0.0 &                             !<-- center point longitudinal (E/W)
                         ,tph0d=0.0 &                             !<-- center point latitudinal (S/N)
                         ,wbd=-180.0 &                               !<-- western boundary (from center point)
                         ,sbd=-90.0 &                               !<-- southern boundary (from center point)
                         ,pt=100.    &                               !<-- pressure at top of atmosphere
                         ,ptsgm=30000. &                           !<-- pressure sigma at toa
                         ,dlmd=1.40625 &                              !<-- longitudinal grid resolution
                         ,dphd=1.0                                !<-- latitudinal grid resolution
!
!-----------------------------------------------------------------------
!***  model grid domain coordinates
!***  IMI,JMI,LM MUST AGREE WITH IM,JM,LM IN CONFIGFILE_GLOBAL
!-----------------------------------------------------------------------
!
integer(kind=4),parameter :: imi=257 &                             !<-- longitudinal mesh size
                            ,jmi=181 &                             !<-- latitudinal mesh size
                            ,im=259,jm=183 &                     !<-- mesh size with boundaries
                            ,lm=24 &                              !<-- horizontal model layers
!
!-----------------------------------------------------------------------
!***  additional grid parameters
!-----------------------------------------------------------------------
!
                            ,lnsh=1 &
                            ,lnsv=lnsh &
                            ,kms=4 &
                            ,ksnos=1 &
                            ,nosnos=kms-ksnos &
                            ,nwets=4
!
!-----------------------------------------------------------------------
!***  modelgrid.inc - END
!-----------------------------------------------------------------------
