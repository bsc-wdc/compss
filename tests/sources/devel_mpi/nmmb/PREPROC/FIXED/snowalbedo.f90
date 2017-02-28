program snowalb
!-----------------------------------------------------------------------
!     z. janjic, aug. 2007
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/datagrid1deg.inc'
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
integer(kind=4),parameter:: &
 idatamax=180 &              ! max i index for data within a tile
,idatamax2=idatamax*2 &      !
,jdatamax=180 &              ! max j index for data within a tile
,nbndry=3 &                  ! # of boundary lineas
,kdatamax= &                 !
 (idatamax+2*nbndry) &       !
*(jdatamax+2*nbndry)         !

real(kind=4),parameter:: &
 pi=3.141592653589793238 &   !
,dtr=pi/180.                 !

character(64):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

integer(kind=4):: &
 i,idata,idatabase,idatap1 &
,j,jdata,jdatap1 &
,ksum

real(kind=4):: &
 ctph,ctph0,data00,data10,data01,data11,deltax,dlm,dph,pp,qq &
,rdx,rdy,sb,stph0,sum,tlm0,tph0,txgrid,tygrid,wb,xgrid,ygrid

integer(kind=1),dimension(1:kdatamax):: &
 ialbd1 &                    !
,ialbd2                      !

real(kind=4),dimension(1:idatamax2,1:jdatamax):: &
 data                        !

real(kind=4),dimension(1:imi,1:jmi):: &
 snowalbedo                  !

integer num, length, stat
character*256 maxsnowalb_dir, output_snowalbedo

if (command_argument_count().gt.0) then
  call geT_command_argument(1,maxsnowalb_dir,length,stat)
  call get_command_argument(2,output_snowalbedo,length,stat)
end if


!-----------------------------------------------------------------------
 1100 format(25f5.2)
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
      rdx=1./dx
      rdy=1./dy
!
      do j=1,jmi
        do i=1,imi
          snowalbedo(i,j)=-9999.
        enddo
      enddo
!-----------------------------------------------------------------------
      fname='00001-00180.00001-00180'
      infile = trim(maxsnowalb_dir) // fname
      open(unit=1,file=infile,status='old' &
          ,access='direct',form='unformatted',recl=kdatamax)
      read(1,rec=1) ialbd1
      close(1)
      print*,'Read ',infile
!
      fname='00181-00360.00001-00180'
      infile = trim(maxsnowalb_dir) // fname
      open(unit=1,file=infile,status='old' &
          ,access='direct',form='unformatted',recl=kdatamax)
      read(1,rec=1) ialbd2
      close(1)
      print*,'Read ',infile
!--merge the two data files cropping the boundary rows-----------------
      do jdata=1,jdatamax
        idatabase=(nbndry+jdata-1)*(idatamax+2*nbndry)
        do idata=1,idatamax
          data(idata,jdata)=ialbd2(idatabase+nbndry+idata)
        enddo
      enddo
      do jdata=1,jdatamax
        idatabase=(nbndry+jdata-1)*(idatamax+2*nbndry)
        do idata=1,idatamax
          data(idatamax+idata,jdata)=ialbd1(idatabase+nbndry+idata)
        enddo
      enddo
!--interpolation loop--------------------------------------------------
      do j=1,jmi
        tygrid=(j-1)*dph+sb
        do i=1,imi
          txgrid=(i-1)*dlm+wb
!
          call rtll(txgrid,tygrid,tlm0d,ctph0,stph0,xgrid,ygrid)
!
          jdata=(ygrid-y0)*rdy+1
          if(jdata.lt.1         ) jdata=1
          if(jdata.gt.jdatamax-1) jdata=jdatamax-1
!
          qq=(ygrid-((jdata-1)*dy+y0))*rdy
          jdatap1=jdata+1
!
          deltax=xgrid-x0
          if(deltax.ge.0.) then
            idata=deltax*rdx+1.
            pp=(xgrid-((idata-1)*dx+x0))*rdx
          else
            idata=idatamax*2
            pp=dx+deltax
          endif
!
          if(idata.ne.idatamax*2)  then
            idatap1=idata+1
          else
            idatap1=1
          endif
!
          sum=0.
          ksum=0
!
          if(   data(idata  ,jdata  ).gt.1.) then
            sum=data(idata  ,jdata  )+sum
            ksum=ksum+1
          endif
          if(   data(idatap1,jdata  ).gt.1.) then
            sum=data(idatap1,jdata  )+sum
            ksum=ksum+1
          endif
          if(   data(idata  ,jdatap1).gt.1.) then
            sum=data(idata  ,jdatap1)+sum
            ksum=ksum+1
          endif
          if(   data(idatap1,jdatap1).gt.1.) then
            sum=data(idatap1,jdatap1)+sum
            ksum=ksum+1
          endif
!
          data00=data(idata  ,jdata  )
          data10=data(idatap1,jdata  )
          data01=data(idata  ,jdatap1)
          data11=data(idatap1,jdatap1)
!
          if(ksum.lt.4.and.ksum.gt.0) then
            sum=sum/ksum
            if(data00.lt.1.) &
               data00=sum
            if(data10.lt.1.) &
               data10=sum
            if(data01.lt.1.) &
               data01=sum
            if(data11.lt.1.) &
               data11=sum
          endif
!
          snowalbedo(i,j)= &
              (data00+(data10-data00)*pp &
                     +(data01-data00)*qq &
                     +(data00-data10-data01+data11)*pp*qq)*0.01

!print*,'i,j',i,j,'idata,idatap1,jdata,jdatap1',idata,idatap1,jdata,jdatap1
!print*,'data',data(idata,jdata),data(idatap1,jdata  ) &
!             ,data(idata  ,jdatap1),data(idatap1,jdatap1)
!print*,'pp,qq,snoalb',pp,qq,snowalbedo(i,j)
!stop
        enddo
!stop
      enddo
!----------------------------------------------------------------------
      do j=jmi,1,-10
        write(*,1100) (snowalbedo(i,j),i=1,imi,40)
      enddo
!----------------------------------------------------------------------
      outfile = output_snowalbedo
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) snowalbedo
      close(1)
      print*,'snowalbedo file written to ../output/snowalbedo'
      print*,'Enjoy your maximum snow albedo!'
!----------------------------------------------------------------------
endprogram snowalb
!----------------------------------------------------------------------
!++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
      parameter(pi=3.1415927,dtr=pi/180.)
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
      relm=atan2(anum,denom)-pi
      almd=relm/dtr+tlm0d
!
      if(almd.gt. 180.)    almd=almd-360.
      if(almd.lt.-180.)    almd=almd+360.
!
      endsubroutine rtll
!----------------------------------------------------------------------

