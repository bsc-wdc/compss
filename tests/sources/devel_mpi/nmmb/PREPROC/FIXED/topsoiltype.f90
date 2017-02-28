program topsoiltype
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
,kdatamax=idatamax*jdatamax &! tile size
,lclass=30                   ! max # of categorical classes

real(kind=4),parameter:: &
 pi=3.141592653589793238 &   !
,dtr=pi/180. &               !
,scalex=0.50 &               !
,scaley=0.50                 !

character(23):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

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

integer(kind=1),dimension(1:idatamax,1:jdatamax):: &
 ichar                       !

integer(kind=4),dimension(1:imi,1:jmi):: &
 ltype                        !

integer(kind=4),dimension(1:lclass,1:imi,1:jmi):: &
 kount                       !

real(kind=4),dimension(1:imi,1:jmi):: &
 seamask

integer num, length, stat
character*256 input_seamaskDEM, soiltype_dir, output_topsoiltype

if (command_argument_count().gt.0) then
  call get_command_argument(1,input_seamaskDEM,length,stat)
  call get_command_argument(2,soiltype_dir,length,stat)
  call get_command_argument(3,output_topsoiltype,length,stat)
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
          infile = trim(soiltype_dir) // fname
          open(unit=1,file=infile,status='old' &
              ,access='direct',form='unformatted',recl=kdatamax) 
          read(1,rec=1) ichar
          if(itile.eq.itilemin) print*,'Processing ',infile
!-----------------------------------------------------------------------
          do jdata=1,jdatamax
!
            ydata=((jtile-1)*jdatamax+jdata-1)*dy+y0
!
            idatabase=(jdata-1)*idatamax
!
            do idata=1,idatamax
              xdata=((itile-1)*idatamax+idata-1)*dx+x0
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
              ltype(i,j)=l
              majority=.true.
            endif
          enddo
!--if not, exclude non-land classes and look for ordinary majority------
          if(.not.majority) then
            kount(14,i,j)=0 ! statsgo water
!
            maxkount=-99
            maxclass=-99
            do l=1,lclass
              if(kount(l,i,j).gt.maxkount) then
                maxkount=kount(l,i,j)
                maxclass=l
              endif
            enddo
            ltype(i,j)=maxclass
          endif
!-----------------------------------------------------------------------
        enddo
      enddo
!--read in the sea-mask-------------------------------------------------
      infile = input_seamaskDEM
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) seamask
      close(1)
!-----------------------------------------------------------------------
! soil  statsgo
! type  class
! ----  -------
!   1   sand
!   2   loamy sand
!   3   sandy loam
!   4   silt loam
!   5   silt
!   6   loam
!   7   sandy clay loam
!   8   silty clay loam
!   9   clay loam
!  10   sandy clay
!  11   silty clay
!  12   clay
!  13   organic material
!  14   water
!  15   bedrock
!  16   other (land-ice)
!  17   playa
!  18   lava
!  19   white sand
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          if(ltype(i,j).eq.0) ltype(i,j)=9
          if(seamask(i,j).gt.0.5) then ! water point
            ltype(i,j)=14
          else ! land point
            if(ltype(i,j).eq.14) ltype(i,j)=7 ! water at land point
          endif
        enddo
      enddo
!-----------------------------------------------------------------------
      do j=jmi,1,-10
        write(*,1100) (ltype(i,j),i=1,imi,20)
      enddo
!-----------------------------------------------------------------------
      outfile = output_topsoiltype
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) ltype
      close(1)
      print*,'topsoiltype file written to ../output/topsoiltype'
      print*,'Enjoy your top soil type!'
!-----------------------------------------------------------------------
endprogram topsoiltype
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
