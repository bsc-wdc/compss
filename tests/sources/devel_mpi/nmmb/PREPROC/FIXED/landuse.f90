program landuseprog
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
,nbndry=0 &                  !
,isize=idatamax+2*nbndry &   !
,jsize=jdatamax+2*nbndry &   !
,lrecl=(idatamax+2*nbndry) & !
      *(jdatamax+2*nbndry) & ! tile size in bytes
,lclass=30                   ! max # of categorical classes

real(kind=4),parameter:: &
 pi=3.141592653589793238 &   !
,dtr=pi/180. &               !
,scalex=0.50 &               !
,scaley=0.50                 !

character(23):: &
 fname                       !

character(128):: &
 infile &
 ,outfile

logical(kind=4):: &
 majority

integer(kind=4):: &
 i,i1,i2,iavg,iclass,idata,idatabase,ie,is,itile &
,j,j1,j2,jdata,je,js,jtile &
,l,lsum &
,maxclass,maxkount

real(kind=4):: &
 ctph,ctph0,dlm,dph,edlm,edph,elmh,elml,ephh,ephl &
,rctph,rdlm,rdph,sb,stph0,tlm,tlm0,tph,tph0,wb,xdata,ydata

integer(kind=1),dimension(1:isize,1:jsize):: &
 ichar                       !

integer(kind=4),dimension(1:imi,1:jmi):: &
 landuse                     !

integer(kind=4),dimension(1:lclass,1:imi,1:jmi):: &
 kount                       !

integer num, length, stat
character*256 landuse_dir, output_landuse, output_kountlanduse

if (command_argument_count().gt.0) then
  call get_command_argument(1,landuse_dir,length,stat)
  call get_command_argument(2,output_landuse,length,stat)
  call get_command_argument(3,output_kountlanduse,length,stat)
end if

!-----------------------------------------------------------------------
 1000 format(i5.5,'-',i5.5,'.',i5.5,'-',i5.5)
 1100 format(40i3)
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
          landuse(i,j)=13 ! default
          do l=1,lclass
            kount(l,i,j)=0
          enddo
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
          infile = landuse_dir // fname
          open(unit=1,file=infile,status='old' &
              ,access='direct',form='unformatted',recl=lrecl,CONVERT="BIG_ENDIAN") 
          read(1,rec=1) ichar
          if(itile.eq.itilemin) print*,'Processing ',infile
!-----------------------------------------------------------------------
          do jdata=nbndry+1,nbndry+jdatamax
            ydata=((jtile-1)*jdatamax+jdata-nbndry-1)*dy+y0   !+20.*dy !?
            idatabase=(jdata-1)*idatamax
            do idata=nbndry+1,nbndry+idatamax
              xdata=((itile-1)*idatamax+idata-nbndry-1)*dx+x0 !+20.*dx !?
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
                do j=js,je
                  if(is.gt.ie) then
                    do i=is,imi
                      kount(iclass,i,j)=kount(iclass,i,j)+1
                    enddo
                    do i=1,ie
                      kount(iclass,i,j)=kount(iclass,i,j)+1
                    enddo
                  else
                    do i=is,ie
                      kount(iclass,i,j)=kount(iclass,i,j)+1
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
                  do j=js,je
                    do i=is,ie
                      kount(iclass,i,j)=kount(iclass,i,j)+1
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
          do l=1,lclass
            iavg=(kount(l,1,j)+kount(l,imi,j))/2
            kount(l,1  ,j)=iavg
            kount(l,imi,j)=iavg
          enddo
        enddo
      endif
!--decide on a single class for a single gridbox------------------------
      do j=1,jmi
        do i=1,imi
!--first check if a class has absolute majority in model's gridbox------
          lsum=0
          majority=.false.
          do l=1,lclass
            lsum=kount(l,i,j)+lsum
          enddo
          lsum=lsum/2
!
          do l=1,lclass
            if(kount(l,i,j).gt.lsum) then
              landuse(i,j)=l
              majority=.true.
            endif
          enddo
!--if not, exclude non-land classes and look for ordinary majority------
          if(.not.majority) then
!            kount(16,i,j)=0 ! usgs water
!
            maxkount=-99
            maxclass=-99
            do l=1,lclass
              if(kount(l,i,j).gt.maxkount) then
                maxkount=kount(l,i,j)
                maxclass=l
              endif
            enddo
            landuse(i,j)=maxclass
          endif
!-----------------------------------------------------------------------
        enddo
      enddo
!-----------------------------------------------------------------------
! class usgs-wrf vegetation/surface type
! ----- --------------------------------
!
!   1   urban and built-up land
!   2   dryland cropland and pasture
!   3   irrigated cropland and pasture
!   4   mixed dryland/irrigated cropland and pasture
!   5   cropland/grassland mosaic
!   6   cropland/woodland mosaic
!   7   grassland
!   8   shrubland
!   9   mixed shrubland/grassland
!  10   savanna
!  11   deciduous broadleaf forest
!  12   deciduous needleleaf forest
!  13   evergreen broadleaf forest
!  14   evergreen needleleaf forest
!  15   mixed forest
!  16   water bodies
!  17   herbaceous wetland
!  18   wooded wetland
!  19   barren or sparsely vegetated
!  20   herbaceous tundra
!  21   wooded tundra
!  22   mixed tundra
!  23   bare ground tundra
!  24   snow or ice
!  25   playa
!  26   lava
!  27   white sand
!-----------------------------------------------------------------------
      do j=jmi,1,-10
        write(*,1100) (landuse(i,j),i=1,imi,20)
      enddo
!-----------------------------------------------------------------------
      outfile = output_landuse
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) landuse
      close(1)
      print*,'Landuse file written to ../output/landuse.'
      print*,'Enjoy your landuse!'
!-----------------------------------------------------------------------
!!!CPEREZ2
      outfile = output_kountlanduse
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) kount
      close(1)
      print*,'Landusenew file written to ../output/kount_landuse'
      print*,'Enjoy your kount landuse!'
!!!CPEREZ2
!-----------------------------------------------------------------------
endprogram landuseprog
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
