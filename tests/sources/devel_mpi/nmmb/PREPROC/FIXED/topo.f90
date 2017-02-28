program topo
!-----------------------------------------------------------------------
!     z. janjic, aug. 2007
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/datagrid30s.inc'
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
integer(kind=4),parameter:: &
 idatamax=1200 &             ! max i index for data within a tile
,itilemax=36 &               ! max i index for a tile
,itilemin=1 &                ! min i index for a tile
,jdatamax=1200 &             ! max j index for data within a tile
,jtilemax=18 &               ! max j index for a tile
,jtilemin=1 &                ! min j index for a tile
,nbndry=3 &                  ! # of extra boundary lines
,isize=idatamax+2*nbndry &   !
,jsize=jdatamax+2*nbndry &   !
,lrecl=(idatamax+2*nbndry) &
      *(jdatamax+2*nbndry) &
      *2                     !tile size in bytes

real(kind=4),parameter:: &
 pi=3.141592653589793238 &   !
,dtr=pi/180. &               !
,scalex=1.00 &               !
,scaley=1.00                 !

character(23):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

integer(kind=4):: &
 i,i1,i2,iavg,iclass,idata,ie,is,itile &
,j,j1,j2,jdata,je,js,jtile &
,kht

real(kind=4):: &
 avg,ctph,ctph0,dlm,dph,edlm,edph,elmh,elml,ephh,ephl,ht &
,rctph,rdlm,rdph,sb,stph0,tlm,tlm0,tph,tph0,wb,xdata,ydata

integer(kind=2),dimension(1:isize,1:jsize):: &
 ichar                       !

integer(kind=4),dimension(1:imi,1:jmi):: &
 kountht                     !

real(kind=4),dimension(1:imi,1:jmi):: &
 height                      !

integer num, length, stat
character*256 topo_dir, output_heightmean

if (command_argument_count().gt.0) then
  call get_command_argument(1,topo_dir,length,stat)
  call get_command_argument(2,output_heightmean,length,stat)
end if

!-----------------------------------------------------------------------
 1000 format(i5.5,'-',i5.5,'.',i5.5,'-',i5.5)
 1100 format(25f5.0)
      infile='                                                         '
      outfile='                                                        '
!-----------------------------------------------------------------------
      tlm0=tlm0d*dtr
      tph0=tph0d*dtr
      stph0=sin(tph0)
      ctph0=cos(tph0)
!
      wb=wbd*dtr
      sb=sbd*dtr
      dlm=dlmd*dtr
      dph=dphd*dtr
!
      rdlm=1./dlm
      rdph=1./dph
!
      do j=1,jmi
        do i=1,imi
          kountht(i,j)=0
          height(i,j)=0.
        enddo
      enddo
!--process data tile by tile--------------------------------------------
      do jtile=jtilemin,jtilemax
        j1=(jtile-1)*jdatamax+1
        j2=j1+jdatamax-1
        do itile=itilemin,itilemax
!--define the local file name-------------------------------------------
          i1=(itile-1)*idatamax+1
          i2=i1+idatamax-1
          write(fname,1000) i1,i2,j1,j2
!-----------------------------------------------------------------------
          infile = trim(topo_dir) // fname
          open(unit=1,file=infile,status='old' &
              ,access='direct',form='unformatted',recl=lrecl,CONVERT="BIG_ENDIAN") 
          read(1,rec=1) ichar
          if(itile.eq.itilemin) print*,'Processing ',infile
!-----------------------------------------------------------------------
          do jdata=nbndry+1,nbndry+jdatamax
            ydata=((jtile-1)*jdatamax+jdata-nbndry-1)*dy+y0
            do idata=nbndry+1,nbndry+idatamax
              xdata=((itile-1)*idatamax+idata-nbndry-1)*dx+x0
!
              iclass=ichar(idata,jdata)
!-----------------------------------------------------------------------
              if(im.gt.imi) then ! global domain
!-----------------------------------------------------------------------
                tlm=xdata*dtr
                tph=ydata*dtr
!
                ctph=abs(cos(tph))
                if(ctph.gt.0.00001) then
                  rctph=max(dph/(ctph*dlm),1.)
                else
                  rctph=999999.
                endif
!
                edlm=min(dlm*rctph*scalex,pi)
                elml=tlm-edlm
                elmh=tlm+edlm
!
                if(elml.lt.-pi) elml=elml+pi+pi
                if(elmh.gt. pi) elmh=elmh-pi-pi
!
                is=int((elml-wb)*rdlm+0.99999)+1
                ie=int((elmh-wb)*rdlm        )+1
!
                if(is.lt.1  ) is=1
                if(ie.lt.1  ) ie=1
                if(is.gt.imi) is=imi
                if(ie.gt.imi) ie=imi
!
                edph=dph*scaley
                ephl=tph-edph
                ephh=tph+edph
!
                if(ephl.lt. sb) ephl= sb
                if(ephh.gt.-sb) ephh=-sb
!
                js=int((ephl-sb)*rdph+0.99999)+1
                je=int((ephh-sb)*rdph+0.00001)+1
!
                if(js.lt.1  ) js=1
                if(je.lt.1  ) je=1
                if(js.gt.jmi) js=jmi
                if(je.gt.jmi) je=jmi
!
                kht=1
                ht=iclass
!
                do j=js,je
                  if(is.gt.ie) then
                    do i=is,imi
                      kountht(i,j)=kountht(i,j)+kht
                      height(i,j)=height(i,j)+ht
                    enddo
                    do i=1,ie
                      kountht(i,j)=kountht(i,j)+kht
                      height(i,j)=height(i,j)+ht
                    enddo
                  else
                    do i=is,ie
                      kountht(i,j)=kountht(i,j)+kht
                      height(i,j)=height(i,j)+ht
                    enddo
                  endif
                enddo
!-----------------------------------------------------------------------
              else ! regional domain
!-----------------------------------------------------------------------
                call tll(xdata,ydata,tlm0d,dtr,ctph0,stph0,tlm,tph)
!
                ctph=cos(tph)
                rctph=max(dph/(ctph*dlm),1.)
!
                edlm=dlm*rctph*scalex
                elml=tlm-edlm
                elmh=tlm+edlm
!
                is=int((elml-wb)*rdlm+0.99999)+1
                ie=int((elmh-wb)*rdlm        )+1
!
                edph=dph*scaley
                ephl=tph-edph
                ephh=tph+edph
!
                js=int((ephl-sb)*rdph+0.99999)+1
                je=int((ephh-sb)*rdph+0.00001)+1
!
                if(ie.ge.1.and.is.le.imi.and. &
                   je.ge.1.and.js.le.jmi) then ! data point inside
!
                  if(is.lt.1  ) is=1
                  if(ie.gt.imi) ie=imi
                  if(js.lt.1  ) js=1
                  if(je.gt.jmi) je=jmi
!
                  kht=1
                  ht=iclass
!
                  do j=js,je
                    do i=is,ie
                      kountht(i,j)=kountht(i,j)+kht
                      height(i,j)=height(i,j)+ht
                    enddo
                  enddo
                endif
!-----------------------------------------------------------------------
              endif ! end of global/regional branching
!-----------------------------------------------------------------------
            enddo
          enddo
!-----------------------------------------------------------------------
          close(1) ! the data tile is finished
!-----------------------------------------------------------------------
        enddo
      enddo
!-----------------------------------------------------------------------
      if(im.gt.imi) then
        do j=1,jmi
          iavg=(kountht(1  ,j)+kountht(imi,j))/2
          kountht(1  ,j)=iavg
          kountht(imi,j)=iavg
          avg=(height(1  ,j)+height(imi,j))*0.5
          height(1  ,j)=avg
          height(imi,j)=avg
        enddo
      endif
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          if(kountht(i,j).gt.0) then
            height(i,j)=height(i,j)/kountht(i,j)
          else
            height(i,j)=0.
            print*,'Singular point ',i,',',j
!            stop 1
          endif
        enddo
      enddo
!-----------------------------------------------------------------------
      print*,'Mean topography height'
      do j=jmi,1,-10
        write(*,1100) (height(i,j),i=1,imi,35)
      enddo
!-----------------------------------------------------------------------
      outfile = output_heightmean
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) height
      close(1)
!
      print*,'Mean height file written to ../output/heightmean'
      print*,'Enjoy your mountains!'
!-----------------------------------------------------------------------
endprogram topo
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine tll(almd,aphd,tlm0d,dtr,ctph0,stph0,tlm,tph)
!     ****************************************************************
!     *                                                              *
!     *  programer: z. janjic, shmz, feb. 1981                       *
!     *  ammended:  z. janjic, ncep, jan. 1996                       *
!     *                                                              *
!     *  transformation from lat-lon to rotated lat-lon coordinates  *
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
      relm=(almd-tlm0d)*dtr
      srlm=sin(relm)
      crlm=cos(relm)
      aph=aphd*dtr
      sph=sin(aph)
      cph=cos(aph)
      cc=cph*crlm
      anum=cph*srlm
      denom=ctph0*cc+stph0*sph
!
      tlm=atan2(anum,denom)
      tph=asin(ctph0*sph-stph0*cc)
!
      return
      endsubroutine tll
!-----------------------------------------------------------------------
