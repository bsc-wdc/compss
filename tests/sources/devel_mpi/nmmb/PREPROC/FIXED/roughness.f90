!
!-----------------------------------------------------------------------
!
      program roughnessprog
!
!-----------------------------------------------------------------------
!     k. haustein, aug. 2009
!-----------------------------------------------------------------------
!
      implicit none
!
!-----------------------------------------------------------------------
      include './include/datagrid025.inc'
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
!
      integer(kind=4),parameter :: idatamax=1439                        & !<-- max i index for data within a tile (360/0.25)
                                  ,idatamax1=104                        & !<-- max i index for african domain
                                  ,idatamax2=46                         & !<-- max i index for asian domain
                                  ,jdatamax=719                         & !<-- max j index for data within a tile (180/0.25)
                                  ,jdatamax1=316                        & !<-- max j index for african domain
                                  ,jdatamax2=208                        & !<-- max j index for asian domain
                                  ,nbndry=0                             &
                                  ,isize=idatamax+2*nbndry              &
                                  ,jsize=jdatamax+2*nbndry              &
                                  ,isize1=idatamax1+2*nbndry            &
                                  ,jsize1=jdatamax1+2*nbndry            &
                                  ,isize2=idatamax2+2*nbndry            &
                                  ,jsize2=jdatamax2+2*nbndry            &
                                  ,kdatamax1=(isize1*jsize1)            &
                                  ,kdatamax2=(isize2*jsize2)
!
      real(kind=4),parameter :: pi=3.141592653589793238                 &
                               ,dtr=pi/180.
!
      character(128) :: infile1,infile2,outfile
!
      integer(kind=4) :: i,idata,idatabase,m,n,jflip                    &
                        ,j,jdata,l,ksum,jdatap1,idatap1
!
      real(kind=4) :: ctph,ctph0,dlm,dph,edlm,edph,elmh,elml,ephh,ephl  &
                     ,rctph,sb,stph0,tlm,tlm0,tph,tph0                  &
                     ,ht,avg,txgrid,tygrid,wb,xgrid,ygrid,rdx,rdy       &
                     ,data00,data10,data01,data11,deltax,nsum,pp,qq
!
      real(kind=4),dimension(1:kdatamax1) :: rdata3,rdata4
      real(kind=4),dimension(1:kdatamax1) :: soilp3,soilp4
      real(kind=4),dimension(1:kdatamax2) :: rdata2
!
      real(kind=4),dimension(1:imi,1:jmi) :: roughness
!
      real(kind=4),dimension(1:isize,1:jsize) :: z00
      real(kind=4),dimension(1:isize1,1:jsize1) :: z0nafr
      real(kind=4),dimension(1:isize2,1:jsize2) :: z0asia

      integer num, length, stat
      character*256 roughness_dir,output_roughness

      if (command_argument_count().gt.0) then
        call get_command_argument(1,roughness_dir,length,stat)
        call get_command_argument(2,output_roughness,length,stat)
      end if

!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
 1100 format(50I3)

      infile1='                                                        '
      infile2='                                                        '
      outfile='                                                        '
! 1200 format(28X,F4.2,14X,E8.2,32X,F4.2,14X,E8.2)
 1201 format(43X,E8.2)
!
!-----------------------------------------------------------------------
!
      tlm0=tlm0d*dtr               !<-- tlm0 = 00.00*pi/180 = 0.0
      tph0=tph0d*dtr               !<-- tph0 = 00.00*pi/180 = 0.0
      stph0=sin(tph0)              !<-- stph0 = 0.0
      ctph0=cos(tph0)              !<-- ctph0 = 1.0
!
      wb=wbd*dtr                   !<-- wb = -180.00*pi/180 = -pi
      sb=sbd*dtr                   !<-- sb = -90.00*pi/180 = -pi/2
      dlm=dlmd*dtr                 !<-- dlm = -wbd/384.*pi/180 = 180.00/384.*pi/180 = 0.0081812308
      dph=dphd*dtr                 !<-- dph = -sbd/270.*pi/180 = 90.00/270.*pi/180 = 0.0058177642
!
      rdx=1./dx                    !<-- rdx = 1./0.25 = 4.
      rdy=1./dy                    !<-- rdy = 1./0.25 = 4.
!
!-----------------------------------------------------------------------
!***  set initial fields zero
!-----------------------------------------------------------------------
!
      do j=1,jmi                   !<-- jmi = -2.*sbd/dphd+1.5 = -2.*(-90.00)/(90.00/270.)+1.5 = 540.5 = 540
        do i=1,imi                 !<-- imi = -2.*wbd/dlmd+1.5 = -2.*(-180.00)/(180.00/384.)+1.5 = 769.5 = 769
          roughness(i,j)=-9999.
        enddo
      enddo
!
      do j=1,jsize                 !<-- jsize = 720
        do i=1,isize               !<-- isize = 1440
          z00(i,j)=0.
        enddo
      enddo
!
      do j=1,jsize1                !<-- jsize1 = 316
        do i=1,isize1              !<-- isize1 = 104
          z0nafr(i,j)=0.
        enddo
      enddo
!
      do j=1,jsize2                !<-- jsize2 = 208
        do i=1,isize2              !<-- isize2 = 46
          z0asia(i,j)=0.
        enddo
      enddo
!
!-----------------------------------------------------------------------
!***  define the local file name
!-----------------------------------------------------------------------
!
      infile1 = trim(roughness_dir) &
// 'surface_N_Africa_1_4x1_4deg_V01.dat'
      infile2 = trim(roughness_dir) &
// 'surface_NE_Asia_1_4x1_4deg_V01.dat'
!
 202  format(28X,F4.2)
 203  format(46X,E8.2)
 204  format(86X,F4.2)
 205  format(104X,E8.2)
!
      open(unit=1,file=infile1,status='unknown' &
                 ,form='formatted',recl=kdatamax1)
      read(1,202) soilp3
      close(1)
      open(unit=2,file=infile1,status='unknown' &
                 ,form='formatted',recl=kdatamax1)
      read(2,203) rdata3
      close(2)
      open(unit=3,file=infile1,status='unknown' &
                 ,form='formatted',recl=kdatamax1)
      read(3,204) soilp4
      close(3)
      open(unit=4,file=infile1,status='unknown' &
                 ,form='formatted',recl=kdatamax1)      
      read(4,205) rdata4
      close(4)

      open(unit=5,file=infile2,status='unknown' &
                 ,form='formatted',recl=kdatamax2)
      read(5,1201) rdata2
      close(5)
!
!-----------------------------------------------------------------------
!
      do jdata=1,idatamax1
        idatabase=(nbndry+jdata-1)*(jdatamax1+2*nbndry)
        do idata=1,jdatamax1
          z0nafr(jdata,idata)=(soilp3(idatabase+nbndry+idata)  &
                             * rdata3(idatabase+nbndry+idata)) &
                             +(soilp4(idatabase+nbndry+idata)  &
                             * rdata4(idatabase+nbndry+idata))
!          z0nafr(jdata,idata)=rdata1(idatabase+nbndry+idata)
!          print*,'jdata(X) nafr:',jdata,'idata(Y) nafr:',idata,'index:',idatabase+idata,'z0afr:',z0nafr(jdata,idata)
        enddo
      enddo
!
      do jdata=1,idatamax2
        idatabase=(nbndry+jdata-1)*(jdatamax2+2*nbndry)
        do idata=1,jdatamax2
          z0asia(jdata,idata)=rdata2(idatabase+nbndry+idata)
!          print*,'jdata(X) asia:',jdata,'idata(Y) asia:',idata,'index:',idatabase+idata,'z0asia:',z0asia(jdata,idata)
        enddo
      enddo
!
!-----------------------------------------------------------------------
!***  assignment of north african and asian domain
!-----------------------------------------------------------------------
!
      do j=jsize,1,-1                !<-- jsize =  719
        do i=1,isize                 !<-- isize = 1439
!          if(j.ge.408.and.j.le.511.and.i.ge.644.and.i.le.959) then
!          if(j.ge.209.and.j.le.312.and.i.ge.644.and.i.le.959) then
          if(j.ge.209.and.j.le.312.and.i.ge.1.and.i.le.239) then
!            n=i-643
            n=i
            m=j-208
            jflip=jsize+1-j
            z00(i+1364,jflip)=z0nafr(m,n)
!          elseif(j.ge.408.and.j.le.511.and.i.ge.1364.and.i.le.1440) then
          elseif(j.ge.209.and.j.le.312.and.i.ge.1364.and.i.le.1440) then
            n=i-1363
            m=j-208
            jflip=jsize+1-j
            z00(i,jflip)=z0nafr(m,n)
!          elseif(j.ge.502.and.j.le.547.and.i.ge.1012.and.i.le.1219) then
!          elseif(j.ge.173.and.j.le.218.and.i.ge.1012.and.i.le.1219) then
          elseif(j.ge.173.and.j.le.218.and.i.ge.292.and.i.le.500) then
!            n=i-1011
            n=i-291
            m=j-172
            jflip=jsize+1-j
            z00(i,jflip)=z0asia(m,n)
          else
            z00(i,j)=z00(i,j)
          endif
        enddo
      enddo
!
! 1110 format(100(F2.1,1X))
 1110 format(100(F3.0))
      write(*,*)
      do j=jsize,1,-20
        write(*,1110) (z00(i,j),i=1,isize,30)
      enddo
      write(*,*)
!
!-----------------------------------------------------------------------
!***  interpolation loop
!-----------------------------------------------------------------------
!
      do j=1,jmi
        tygrid=(j-1)*dph+sb
!
        do i=1,imi
          txgrid=(i-1)*dlm+wb
!
!-----------------------------------------------------------------------
          call rtll(txgrid,tygrid,tlm0d,ctph0,stph0,xgrid,ygrid)
!-----------------------------------------------------------------------
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
          nsum=0.
          ksum=0
!
          if(    z00(idata,jdata).gt.0.000001) then
            nsum=z00(idata,jdata)+nsum
            ksum=ksum+1
          endif
          if(    z00(idatap1,jdata).gt.0.000001) then
            nsum=z00(idatap1,jdata)+nsum
            ksum=ksum+1
          endif
          if(    z00(idata,jdatap1).gt.0.000001) then
            nsum=z00(idata,jdatap1)+nsum
            ksum=ksum+1
          endif
          if(    z00(idatap1,jdatap1).gt.0.000001) then
            nsum=z00(idatap1,jdatap1)+nsum
            ksum=ksum+1
          endif
!
          data00=z00(idata  ,jdata)
          data10=z00(idatap1,jdata)
          data01=z00(idata  ,jdatap1)
          data11=z00(idatap1,jdatap1)
!
          if(ksum.lt.4.and.ksum.gt.0) then
            nsum=nsum/ksum
            if(data00.eq.0.) data00=nsum
            if(data10.eq.0.) data10=nsum
            if(data01.eq.0.) data01=nsum
            if(data11.eq.0.) data11=nsum
!            print*,nsum
          endif
!
          roughness(i,j)=(data00+(data10-data00)*pp+(data01-data00)*qq    &
                        +(data00-data10-data01+data11)*pp*qq)
!
        enddo
!
      enddo
!
!-----------------------------------------------------------------------
!
      do j=jmi,1,-10
        write(*,1110) (roughness(i,j),i=1,imi,20)
      enddo
! 1120 format(1000(F3.1,1X))
!      do j=420,370,-1
!        write(*,1120) (roughness(i,j),i=520,660,1)
!      enddo
!
!-----------------------------------------------------------------------
!***  write output to oufile
!-----------------------------------------------------------------------
!
      outfile = output_roughness
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) roughness
      close(1)
      print*,' '
      print*,'Roughness file written to ../output/roughness.'
      print*,'Enjoy your roughness!'
!
!-----------------------------------------------------------------------
!
      endprogram roughnessprog
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
      subroutine rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
!
!-----------------------------------------------------------------------
!
!     ****************************************************************
!     *                                                              *
!     *  programer: z. janjic, shmz, feb. 1981                       *
!     *  ammended:  z. janjic, ncep, jan. 1996                       *
!     *                                                              *
!     *  transformation from lat-lon to rotated lat-lon coordinates  *
!     ****************************************************************
!     ****************************************************************
!     *  almd  - geographical longitude, degs, range -180.,180       *      !<-- = xdata
!     *  aphd  - geographical latitude,  degs, range - 90., 90.,     *      !<-- = ydata
!     *          poles are singular                                  *
!     *  tlm0d - the angle of rotation of the transformed lat-lon    *      !<-- = 0.0
!     *          system in the longitudinal direction, degs          *
!     *  ctph0 - cos(tph0), tph0 is the angle of rotation of the     *      !<-- = 0.0
!     *          transformed lat-lon system in the latitudinal       *
!     *          direction, precomputed                              *
!     *  stph0 - sin(tph0), tph0 is the angle of rotation of the     *      !<-- = 1.0
!     *          transformed lat-lon system in the latitudinal       *
!     *          direction, precomputed                              *
!     *  tlm   - transformed longitude, rad.                         *      !<-- = atan2(anum,denom)
!     *  tph   - transformed latitude, rad.                          *      !<-- = asin(ctph0*sph-stph0*cc)
!     ****************************************************************
!
!-----------------------------------------------------------------------
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
!-----------------------------------------------------------------------
!
      return
!
!-----------------------------------------------------------------------
!
      endsubroutine rtll
!
!-----------------------------------------------------------------------
