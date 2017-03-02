      program degribsst
!----------------------------------------------------------------------!
!     z. janjic, feb. 2005                                             !
!----------------------------------------------------------------------!
      include 'include/llgrid05.inc'
!-----------------------------------------------------------------------
!    note:  this subroutine degribs sst and the algorithm assumes
!    a 0.5 deg global sst field in the llgrid05.inc format
!-----------------------------------------------------------------------
      character*64 infile,outfil
!
      dimension gllsst05(imll,jmll)
!-----------------------------------------------------------------------

      character*256 param1, sstfileinPath
      call getarg(1,param1)
      call getarg(2,sstfileinPath)

      insst=39
      indxst=0

      call gribst(insst,indxst,imll,jmll,gllsst05,ierr)
!-----------------------------------------------------------------------
      open(unit=3,file=param1,status='unknown'
     1,    form='unformatted')
      write(3) gllsst05
      close(3)
!-----------------------------------------------------------------------
 7777 format(' ',40f4.0)
          do j=1,jmll,20
      write(*,7777) (gllsst05(i,jmll+1-j),i=1,imll,30)
          enddo
!-----------------------------------------------------------------------
      stop
      end
!++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine gribst(insst,indxst,imll,jmll,gllsst05,ierr)
!----------------------------------------------------------------------
!$$$  sub  program documentation block
!                .      .    .                                       .
! sub  program:  gribst      unpack global grib sst
!   prgmmr: gerald           org: w/nmc21     date: 95-08-01
!
! abstract: decodes global avn, fnl, or mrf grib gridded fields.
!
! program history log:
!   95-08-01  gerald
!mp     changed 99-02-22 by pyle to use a normal version of getgb
!zj     changed 99-10-25 by janjic to f90 and to suit his version
!zj     changed 05-02-08 by janjic for 0.5 deg 2dvar sst
!
! usage:
!   input files:
!
!   output files:  (including scratch files)
!
!   subprograms called: (list all called from anywhere in codes)
!     unique:    - routines that accompany source for compile
!     library:
!
!   exit states:
!     cond =   0 - successful run
!          =nnnn - trouble or special flag - specify nature
!
! remarks: list caveats, other helpful hints or information
!
! attributes:
!   language: indicate extensions, compiler options
!$$$-------------------------------------------------------------------
      integer ipds,igds,igrid
      dimension jpds(400),jgds(400),igrd(5,3)
      dimension kpds(400),kgds(400)
      dimension fld(imll*jmll)
      dimension hold(jmll),gllsst05(imll,jmll)
!
      logical lb(imll*jmll)
!----------------------------------------------------------------------
!sstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsstsst
!----------------------------------------------------------------------
      data igrd/ 11, 34,  2, 52, 11,
     &            1,105,102,105,105,
     &            0, 10,  0,  2,  2/
!-------------input units for decoding grib file-----------------------
      lugb=insst
!mp      lugi=indxst
      lugi=0
      ierr=0
      j=0
!-------------decode the fields----------------------------------------
              do igrid=1,1
!----------------------------------------------------------------------
          do ipds=1,200
      jpds(ipds) = -1
          enddo
          do igds=1,200
      jgds(igds) = -1
          enddo
!-------------get select fields----------------------------------------
      jpds(5) = igrd(igrid,1)
      jpds(6) = igrd(igrid,2)
      jpds(7) = igrd(igrid,3)
!
      call baopen(lugb,sstfileinPath,iretba)
!      print*,'*** gribst back from baopen unit#, iret=',lugb,iretba
!      print*,lugb,lugi,imll*jmll,j,jpds,jgds,kf,k,kpds,kgds

      call getgb
     &(lugb,lugi,imll*jmll,j,jpds,jgds,kf,k,kpds,kgds,lb,fld,iret)
!      print*,'getgb',iret
!----------------------------------------------------------------------
          if(iret.ne.0) then
      ierr=1
!      print*,'*** gribst iret=',iret
!      print*,'*** Sorry, leaving gribst'
          endif
!-------------convert 1d field into 2d field---------------------------
      k=0
              do j=1,jmll
          do i=1,imll-1
      k=k+1
      gllsst05(i,j)=fld(k) 
          enddo
              enddo
!      print*,k
!-------------flip grid to pt(1,1)=(0e,-90)----------------------------
              do iwe=1,imll-1
          do jne=1,jmll
      hold(jne)=gllsst05(iwe,jne)
          enddo
          do jne=1,jmll
      gllsst05(iwe,jmll+1-jne)=hold(jne)
          enddo
              enddo
          do jne=1,jmll
      gllsst05(imll,jne)=gllsst05(1,jne)
          enddo
!----------------------------------------------------------------------
      print*,'*** gribst, sst data flipped.'
!      print*,'*** leaving gribest, kpds= ',(kpds(i),i=1,12)
!      print*,'*** leaving gribest, kgds= ',(kgds(i),i=1,25)
!----------------------------------------------------------------------
              enddo
!----------------------------------------------------------------------
      return
      end
