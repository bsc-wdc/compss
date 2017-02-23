!-----------------------------------------------------------------------
!
      program allprep
!
!-----------------------------------------------------------------------
!
      include 'lmimjm.inc'
      logical global
!
!-----------------------------------------------------------------------
!
      global=im.gt.imi
!
!-----------------------------------------------------------------------
      call sst05(global)
      call gblprep(global)
!-----------------------------------------------------------------------
!
      write(*,*) 'That would be all.  Have a nice day.'
      stop
!
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine sst05(global)
!
!-----------------------------------------------------------------------
!     z. janjic, feb. 2003
!     k. haustein, aug. 2009
!-----------------------------------------------------------------------
!
      include 'lmimjm.inc'
      include 'llgrid05.inc'                !<-- for 0.5 deg sst data
!
!-----------------------------------------------------------------------
      parameter(dtr=3.1415926535897932384626433832795/180.)
!-----------------------------------------------------------------------
!
      character*128 infile,fname
!
      dimension idat(3)
      dimension coh(3,imi,jmi),inh(4,imi,jmi),jnh(4,imi,jmi)
!
      dimension gsst(imll,jmll)
      dimension sst(imi,jmi)
      logical run,global
!
!-----------------------------------------------------------------------
      data
     & infile/'
     &       '/
     &,fname /'
     &       '/
!-----------------------------------------------------------------------
!
      infile='../output/llspl.000'
      open(unit=1,file=infile,status='old',form='unformatted')
      read(1) run,idat,ihrst
      print*,'*** sst read cycle data from ',infile
      close(1)
!
      print*,'*** sst idat=',idat,ihrst,' UTC'
!
!-----------------------------------------------------------------------
!
      fname='../output/llgsst05'
      open(unit=2,file=fname,status='unknown',form='unformatted'
     &    ,iostat=ios)
      read (2) gsst
      close(2)
      write(*,*)'*** read from file: ',fname
!
!-----------------------------------------------------------------------
      call gtllhsst(coh,inh,jnh)
!-----------------------------------------------------------------------
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gsst,sst)
!-----------------------------------------------------------------------
!
      open(unit=1,file='../output/sst05'
     &    ,status='unknown'
     &    ,form='unformatted')
      write(1) sst
      close(1)
!
!-----------------------------------------------------------------------
!
      write(*,*) '0.5 Deg SST'
 7777 format(' ',20f4.0)
          do j=1,jmi,40
      write(*,7777) (sst(i,jmi+1-j),i=1,imi,40)
          enddo
      write(*,*) '*** All done sst05.  Good job!'
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine gtllhsst(coh,inh,jnh)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
      include 'llgrid05.inc'                 !<-- for 0.5 deg sst data
!-----------------------------------------------------------------------
!
      parameter(dtr=3.1415926535897932384626433832795/180.)
!
!-----------------------------------------------------------------------
!
      dimension coh(3,imi,jmi),inh(4,imi,jmi),jnh(4,imi,jmi)
!
      print*,'*** model dimensions imi=',imi,' jmi=',jmi
      print*,'*** data dimensions imll=',imll,' jmll=',jmll
!
!-----------------------------------------------------------------------
!***  umo domain geometry
!-----------------------------------------------------------------------
!
      wb=wbd*dtr
      sb=sbd*dtr
      tph0=tph0d*dtr
      ctph0=cos(tph0)
      stph0=sin(tph0)
      dlm=dlmd*dtr
      dph=dphd*dtr
!
!-----------------------------------------------------------------------
!***  entry to the umo i,j loop
!***  neighbour avn index identification (avn data defined in ll system)
!-----------------------------------------------------------------------
!***               umo height pts
!-----------------------------------------------------------------------
!
      tph=sb-dph
              do j=1,jmi
          tph=tph+dph
          tlm=wb-dlm
!
          do i=1,imi
      tlm=tlm+dlm
!
!-----------------------------------------------------------------------
!***  tll to ll conversion
!-----------------------------------------------------------------------
!
      call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
!
!-----------------------------------------------------------------------
!***  conversion from -180,180 range to 0,360 range
!-----------------------------------------------------------------------
!
      if(almd.lt.0.) almd=360.+almd
!
!-----------------------------------------------------------------------
!***  check if umo pt is out of avn domain
!-----------------------------------------------------------------------
!
      x=almd-bowest
      y=aphd-bosout
!
      indll=x/delon+1
      if(x.lt.0.) indll=imll
      jndll=y/delat+1
!
      if(indll.eq.0)      indll=imll-1
      if(indll.gt.imll)   indll=1
      if(jndll.eq.0)      jndll=1
      if(jndll.ge.jmll-1) jndll=jmll-1
!
          if(x.ge.0.) then
      x=x-(indll-1)*delon
          else
      x=x+delon
          endif
      y=y-(jndll-1)*delat
!
          if((indll.lt.1..or.indll.gt.imll).or.
     &       (jndll.lt.1..or.jndll.gt.jmll))    then
      print *,'*** At h point i,j=',i,j,' nearest AVN point is indll=',
     &        indll,' jndll=',jndll
      stop
          endif
!
!-----------------------------------------------------------------------
      coh(1,i,j)=x/delon
      coh(2,i,j)=y/delat
      coh(3,i,j)=coh(1,i,j)*coh(2,i,j)
!-----------------------------------------------------------------------
!
      inh(1,i,j)=indll
      inh(3,i,j)=indll
          if(indll.lt.imll) then
      inh(2,i,j)=indll+1
      inh(4,i,j)=indll+1
          else
      inh(2,i,j)=1
      inh(4,i,j)=1
          endif      
!
      jnh(1,i,j)=jndll
      jnh(2,i,j)=jndll
      jnh(3,i,j)=jndll+1
      jnh(4,i,j)=jndll+1
          enddo
              enddo
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine gblprep(global)
!
!-----------------------------------------------------------------------
      include 'llgrid.inc'
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      logical run,global
      dimension idat(3)
      dimension hgt(imi,jmi),stdh(imi,jmi),sm(imi,jmi),pmsl(imi,jmi)
      dimension hsp(imi,jmi,lmll),tsp(imi,jmi,lmll)
      dimension usp(imi,jmi,lmll),vsp(imi,jmi,lmll)
      dimension qsp(imi,jmi,lmll),wsp(imi,jmi,lmll)
!-----------------------------------------------------------------------
      dimension pdbs(imi,lnsh,2),pdbn(imi,lnsh,2)
     &         ,pdbw(lnsh,jmi,2),pdbe(lnsh,jmi,2)
!
      dimension tbs(imi,lnsh,lm,2),tbn(imi,lnsh,lm,2)
     &         ,tbw(lnsh,jmi,lm,2),tbe(lnsh,jmi,lm,2)
      dimension qbs(imi,lnsh,lm,2),qbn(imi,lnsh,lm,2)
     &         ,qbw(lnsh,jmi,lm,2),qbe(lnsh,jmi,lm,2)
      dimension wbs(imi,lnsh,lm,2),wbn(imi,lnsh,lm,2)
     &         ,wbw(lnsh,jmi,lm,2),wbe(lnsh,jmi,lm,2)
!
      dimension ubs(imi,lnsv,lm,2),ubn(imi,lnsv,lm,2)
     &         ,ubw(lnsv,jmi,lm,2),ube(lnsv,jmi,lm,2)
      dimension vbs(imi,lnsv,lm,2),vbn(imi,lnsv,lm,2)
     &         ,vbw(lnsv,jmi,lm,2),vbe(lnsv,jmi,lm,2)
!-----------------------------------------------------------------------
      dimension pdbsa(imi,lnsh),pdbna(imi,lnsh)
     &         ,pdbwa(lnsh,jmi),pdbea(lnsh,jmi)
!
      dimension tbsa(imi,lnsh,lm),tbna(imi,lnsh,lm)
     &         ,tbwa(lnsh,jmi,lm),tbea(lnsh,jmi,lm)
      dimension qbsa(imi,lnsh,lm),qbna(imi,lnsh,lm)
     &         ,qbwa(lnsh,jmi,lm),qbea(lnsh,jmi,lm)
      dimension wbsa(imi,lnsh,lm),wbna(imi,lnsh,lm)
     &         ,wbwa(lnsh,jmi,lm),wbea(lnsh,jmi,lm)
!
      dimension ubsa(imi,lnsv,lm),ubna(imi,lnsv,lm)
     &         ,ubwa(lnsv,jmi,lm),ubea(lnsv,jmi,lm)
      dimension vbsa(imi,lnsv,lm),vbna(imi,lnsv,lm)
     &         ,vbwa(lnsv,jmi,lm),vbea(lnsv,jmi,lm)
!-----------------------------------------------------------------------
      dimension pdbsb(imi,lnsh),pdbnb(imi,lnsh)
     &         ,pdbwb(lnsh,jmi),pdbeb(lnsh,jmi)
!
      dimension tbsb(imi,lnsh,lm),tbnb(imi,lnsh,lm)
     &         ,tbwb(lnsh,jmi,lm),tbeb(lnsh,jmi,lm)
      dimension qbsb(imi,lnsh,lm),qbnb(imi,lnsh,lm)
     &         ,qbwb(lnsh,jmi,lm),qbeb(lnsh,jmi,lm)
      dimension wbsb(imi,lnsh,lm),wbnb(imi,lnsh,lm)
     &         ,wbwb(lnsh,jmi,lm),wbeb(lnsh,jmi,lm)
!
      dimension ubsb(imi,lnsv,lm),ubnb(imi,lnsv,lm)
     &         ,ubwb(lnsv,jmi,lm),ubeb(lnsv,jmi,lm)
      dimension vbsb(imi,lnsv,lm),vbnb(imi,lnsv,lm)
     &         ,vbwb(lnsv,jmi,lm),vbeb(lnsv,jmi,lm)
!
!-----------------------------------------------------------------------
      character*128 fname
      data fname/'                                '/
!-----------------------------------------------------------------------
!
      open(unit=2,file='../output/height'
     &    ,status='unknown',form='unformatted')
      open(unit=3,file='../output/seamask'
     &    ,status='unknown',form='unformatted')
      open(unit=4,file='../output/stdh'
     &    ,status='unknown',form='unformatted')
      read(2) hgt
      read(3) sm
      read(4) stdh
      print*,'*** topography in ../output/height'
      print*,'*** seamask in ../output/seamask'
      print*,'*** height std. dev. in ../output/stdh'
      close(2)
      close(3)
      close(4)
!
!-----------------------------------------------------------------------
!
!!! KARSTEN
      ihr=0
      if(global) then
        ihrend=0
      else
        ihrend=nhours
      endif
      ihboco=tboco
!      tboco=ihboco*3600.
      tboco2=ihboco*3600.
!      rtboco=1./tboco
      rtboco=1./tboco2
!!! KARSTEN
!
!-----------------------------------------------------------------------
!
      call gtlli(run,global,idat,ihrst,ihr,wb,sb,dlm,dph,sm,hgt
     &          ,pmsl,hsp,tsp,usp,vsp,qsp,wsp)
!
!-----------------------------------------------------------------------
!
      call p2hyb(run,global,idat,ihrst,ihrend,0,hgt,stdh,sm
     &,pmsl,hsp,tsp,usp,vsp,qsp,wsp
     &,pdbs,pdbn,pdbw,pdbe
     &,tbs,tbn,tbw,tbe
     &,qbs,qbn,qbw,qbe
     &,wbs,wbn,wbw,wbe
     &,ubs,ubn,ubw,ube
     &,vbs,vbn,vbw,vbe)
!
!-----------------------------------------------------------------------
!
                      if(.not.global) then
!
!-----------------------------------------------------------------------
!***  lateral boundary conditions
!***  southern and northern boundaries at h points
!-----------------------------------------------------------------------
!
                  do jb=1,lnsh
              do ib=1,imi
      pdbsb(ib,jb)=pdbs(ib,jb,1)
      pdbnb(ib,jb)=pdbn(ib,jb,1)
          do l=1,lm
      tbsb(ib,jb,l)=tbs(ib,jb,l,1)
      tbnb(ib,jb,l)=tbn(ib,jb,l,1)
      qbsb(ib,jb,l)=qbs(ib,jb,l,1)
      qbnb(ib,jb,l)=qbn(ib,jb,l,1)
      wbsb(ib,jb,l)=wbs(ib,jb,l,1)
      wbnb(ib,jb,l)=wbn(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at h points----------------
                  do jb=1,jmi
              do ib=1,lnsh
      pdbwb(ib,jb)=pdbw(ib,jb,1)
      pdbeb(ib,jb)=pdbe(ib,jb,1)
          do l=1,lm
      tbwb(ib,jb,l)=tbw(ib,jb,l,1)
      tbeb(ib,jb,l)=tbe(ib,jb,l,1)
      qbwb(ib,jb,l)=qbw(ib,jb,l,1)
      qbeb(ib,jb,l)=qbe(ib,jb,l,1)
      wbwb(ib,jb,l)=wbw(ib,jb,l,1)
      wbeb(ib,jb,l)=wbe(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at v points--------------
                  do jb=1,lnsv
              do ib=1,imi-1
          do l=1,lm
      ubsb(ib,jb,l)=ubs(ib,jb,l,1)
      ubnb(ib,jb,l)=ubn(ib,jb,l,1)
      vbsb(ib,jb,l)=vbs(ib,jb,l,1)
      vbnb(ib,jb,l)=vbn(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at v points----------------
                  do jb=1,jmi-1
              do ib=1,lnsv
          do l=1,lm
      ubwb(ib,jb,l)=ubw(ib,jb,l,1)
      ubeb(ib,jb,l)=ube(ib,jb,l,1)
      vbwb(ib,jb,l)=vbw(ib,jb,l,1)
      vbeb(ib,jb,l)=vbe(ib,jb,l,1)
          enddo
              enddo
                  enddo
!
!-----------------------------------------------------------------------
      do ihr=ihboco,ihrend,ihboco
!-----------------------------------------------------------------------
!
      call gtlli(run,global,idat,ihrst,ihr,wb,sb,dlm,dph,sm,hgt
     &          ,pmsl,hsp,tsp,usp,vsp,qsp,wsp)
!
!-----------------------------------------------------------------------
!
      call p2hyb(run,global,idat,ihrst,ihrend,ihr,hgt,stdh,sm
     &,pmsl,hsp,tsp,usp,vsp,qsp,wsp
     &,pdbs,pdbn,pdbw,pdbe
     &,tbs,tbn,tbw,tbe
     &,qbs,qbn,qbw,qbe
     &,wbs,wbn,wbw,wbe
     &,ubs,ubn,ubw,ube
     &,vbs,vbn,vbw,vbe)
!
!-----------------------------------------------------------------------
!***  southern and northern boundaries at h points
!-----------------------------------------------------------------------
!
                  do jb=1,lnsh
              do ib=1,imi
      pdbsa(ib,jb)=pdbs(ib,jb,1)
      pdbna(ib,jb)=pdbn(ib,jb,1)
          do l=1,lm
      tbsa(ib,jb,l)=tbs(ib,jb,l,1)
      tbna(ib,jb,l)=tbn(ib,jb,l,1)
      qbsa(ib,jb,l)=qbs(ib,jb,l,1)
      qbna(ib,jb,l)=qbn(ib,jb,l,1)
      wbsa(ib,jb,l)=wbs(ib,jb,l,1)
      wbna(ib,jb,l)=wbn(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at h points----------------
                  do jb=1,jmi
              do ib=1,lnsh
      pdbwa(ib,jb)=pdbw(ib,jb,1)
      pdbea(ib,jb)=pdbe(ib,jb,1)
          do l=1,lm
      tbwa(ib,jb,l)=tbw(ib,jb,l,1)
      tbea(ib,jb,l)=tbe(ib,jb,l,1)
      qbwa(ib,jb,l)=qbw(ib,jb,l,1)
      qbea(ib,jb,l)=qbe(ib,jb,l,1)
      wbwa(ib,jb,l)=wbw(ib,jb,l,1)
      wbea(ib,jb,l)=wbe(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at v points--------------
                  do jb=1,lnsv
              do ib=1,imi-1
          do l=1,lm
      ubsa(ib,jb,l)=ubs(ib,jb,l,1)
      ubna(ib,jb,l)=ubn(ib,jb,l,1)
      vbsa(ib,jb,l)=vbs(ib,jb,l,1)
      vbna(ib,jb,l)=vbn(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at v points----------------
                  do jb=1,jmi-1
              do ib=1,lnsv
          do l=1,lm
      ubwa(ib,jb,l)=ubw(ib,jb,l,1)
      ubea(ib,jb,l)=ube(ib,jb,l,1)
      vbwa(ib,jb,l)=vbw(ib,jb,l,1)
      vbea(ib,jb,l)=vbe(ib,jb,l,1)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at h points--------------
                  do jb=1,lnsh
              do ib=1,imi
      pdbs(ib,jb,2)=(pdbsa(ib,jb)-pdbsb(ib,jb))*rtboco
      pdbn(ib,jb,2)=(pdbna(ib,jb)-pdbnb(ib,jb))*rtboco
      pdbs(ib,jb,1)=pdbsb(ib,jb)
      pdbn(ib,jb,1)=pdbnb(ib,jb)
          do l=1,lm
      tbs(ib,jb,l,2)=(tbsa(ib,jb,l)-tbsb(ib,jb,l))*rtboco
      tbn(ib,jb,l,2)=(tbna(ib,jb,l)-tbnb(ib,jb,l))*rtboco
      qbs(ib,jb,l,2)=(qbsa(ib,jb,l)-qbsb(ib,jb,l))*rtboco
      qbn(ib,jb,l,2)=(qbna(ib,jb,l)-qbnb(ib,jb,l))*rtboco
      wbs(ib,jb,l,2)=(wbsa(ib,jb,l)-wbsb(ib,jb,l))*rtboco
      wbn(ib,jb,l,2)=(wbna(ib,jb,l)-wbnb(ib,jb,l))*rtboco
      tbs(ib,jb,l,1)=tbsb(ib,jb,l)
      tbn(ib,jb,l,1)=tbnb(ib,jb,l)
      qbs(ib,jb,l,1)=qbsb(ib,jb,l)
      qbn(ib,jb,l,1)=qbnb(ib,jb,l)
      wbs(ib,jb,l,1)=wbsb(ib,jb,l)
      wbn(ib,jb,l,1)=wbnb(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at h points----------------
                  do jb=1,jmi
              do ib=1,lnsh
      pdbw(ib,jb,2)=(pdbwa(ib,jb)-pdbwb(ib,jb))*rtboco
      pdbe(ib,jb,2)=(pdbea(ib,jb)-pdbeb(ib,jb))*rtboco
      pdbw(ib,jb,1)=pdbwb(ib,jb)
      pdbe(ib,jb,1)=pdbeb(ib,jb)
          do l=1,lm
      tbw(ib,jb,l,2)=(tbwa(ib,jb,l)-tbwb(ib,jb,l))*rtboco
      tbe(ib,jb,l,2)=(tbea(ib,jb,l)-tbeb(ib,jb,l))*rtboco
      qbw(ib,jb,l,2)=(qbwa(ib,jb,l)-qbwb(ib,jb,l))*rtboco
      qbe(ib,jb,l,2)=(qbea(ib,jb,l)-qbeb(ib,jb,l))*rtboco
      wbw(ib,jb,l,2)=(wbwa(ib,jb,l)-wbwb(ib,jb,l))*rtboco
      wbe(ib,jb,l,2)=(wbea(ib,jb,l)-wbeb(ib,jb,l))*rtboco
      tbw(ib,jb,l,1)=tbwb(ib,jb,l)
      tbe(ib,jb,l,1)=tbeb(ib,jb,l)
      qbw(ib,jb,l,1)=qbwb(ib,jb,l)
      qbe(ib,jb,l,1)=qbeb(ib,jb,l)
      wbw(ib,jb,l,1)=wbwb(ib,jb,l)
      wbe(ib,jb,l,1)=wbeb(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at v points--------------
                  do jb=1,lnsv
              do ib=1,imi-1
          do l=1,lm
      ubs(ib,jb,l,2)=(ubsa(ib,jb,l)-ubsb(ib,jb,l))*rtboco
      ubn(ib,jb,l,2)=(ubna(ib,jb,l)-ubnb(ib,jb,l))*rtboco
      vbs(ib,jb,l,2)=(vbsa(ib,jb,l)-vbsb(ib,jb,l))*rtboco
      vbn(ib,jb,l,2)=(vbna(ib,jb,l)-vbnb(ib,jb,l))*rtboco
      ubs(ib,jb,l,1)=ubsb(ib,jb,l)
      ubn(ib,jb,l,1)=ubnb(ib,jb,l)
      vbs(ib,jb,l,1)=vbsb(ib,jb,l)
      vbn(ib,jb,l,1)=vbnb(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at v points----------------
                  do jb=1,jmi-1
              do ib=1,lnsv
          do l=1,lm
      ubw(ib,jb,l,2)=(ubwa(ib,jb,l)-ubwb(ib,jb,l))*rtboco
      ube(ib,jb,l,2)=(ubea(ib,jb,l)-ubeb(ib,jb,l))*rtboco
      vbw(ib,jb,l,2)=(vbwa(ib,jb,l)-vbwb(ib,jb,l))*rtboco
      vbe(ib,jb,l,2)=(vbea(ib,jb,l)-vbeb(ib,jb,l))*rtboco
      ubw(ib,jb,l,1)=ubwb(ib,jb,l)
      ube(ib,jb,l,1)=ubeb(ib,jb,l)
      vbw(ib,jb,l,1)=vbwb(ib,jb,l)
      vbe(ib,jb,l,1)=vbeb(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at h points--------------
                  do jb=1,lnsh
              do ib=1,imi
      pdbsb(ib,jb)=pdbsa(ib,jb)
      pdbnb(ib,jb)=pdbna(ib,jb)
          do l=1,lm
      tbsb(ib,jb,l)=tbsa(ib,jb,l)
      tbnb(ib,jb,l)=tbna(ib,jb,l)
      qbsb(ib,jb,l)=qbsa(ib,jb,l)
      qbnb(ib,jb,l)=qbna(ib,jb,l)
      wbsb(ib,jb,l)=wbsa(ib,jb,l)
      wbnb(ib,jb,l)=wbna(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at h points----------------
                  do jb=1,jmi-2*lnsh
              do ib=1,lnsh
      pdbwb(ib,jb)=pdbwa(ib,jb)
      pdbeb(ib,jb)=pdbea(ib,jb)
          do l=1,lm
      tbwb(ib,jb,l)=tbwa(ib,jb,l)
      tbeb(ib,jb,l)=tbea(ib,jb,l)
      qbwb(ib,jb,l)=qbwa(ib,jb,l)
      qbeb(ib,jb,l)=qbea(ib,jb,l)
      wbwb(ib,jb,l)=wbwa(ib,jb,l)
      wbeb(ib,jb,l)=wbea(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------southern and northern boundaries at v points--------------
                  do jb=1,lnsv
              do ib=1,imi-1
          do l=1,lm
      ubsb(ib,jb,l)=ubsa(ib,jb,l)
      ubnb(ib,jb,l)=ubna(ib,jb,l)
      vbsb(ib,jb,l)=vbsa(ib,jb,l)
      vbnb(ib,jb,l)=vbna(ib,jb,l)
          enddo
              enddo
                  enddo
!-------------western and eastern boundaries at v points----------------
                  do jb=1,jmi-1-2*lnsv
              do ib=1,lnsv
          do l=1,lm
      ubwb(ib,jb,l)=ubwa(ib,jb,l)
      ubeb(ib,jb,l)=ubea(ib,jb,l)
      vbwb(ib,jb,l)=vbwa(ib,jb,l)
      vbeb(ib,jb,l)=vbea(ib,jb,l)
          enddo
              enddo
                  enddo
!
!-----------------------------------------------------------------------
!
      nbc=18
!OJORBA3      write(fname,'(a,i3.3)')'../output/boco.'
      write(fname,'(a,i4.4)')'../output/boco.'
     &,ihr-ihboco
      open(unit=nbc,file=fname
     &    ,status='unknown',form='unformatted')
      run=.true.
!!! KARSTEN
!      write(nbc) run,idat,ihrst,tboco
      write(nbc) run,idat,ihrst,tboco2
!!! KARSTEN
      write(nbc) pdbs,pdbn,pdbw,pdbe
      write(nbc) tbs,tbn,tbw,tbe
      write(nbc) qbs,qbn,qbw,qbe
      write(nbc) wbs,wbn,wbw,wbe
      write(nbc) ubs,ubn,ubw,ube
      write(nbc) vbs,vbn,vbw,vbe
      close(unit=nbc)
      print*,fname
!
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
!
      nbc=18
!OJORBA3      write(fname,'(a,i3.3)')'../output/boco.'
      write(fname,'(a,i4.4)')'../output/boco.'
     &,ihrend
      open(unit=nbc,file=fname
     &    ,status='unknown',form='unformatted')
      run=.true.
!!! KARSTEN
!      write(nbc) run,idat,ihrst,tboco
      write(nbc) run,idat,ihrst,tboco2
!!! KARSTEN
      write(nbc) pdbs,pdbn,pdbw,pdbe
      write(nbc) tbs,tbn,tbw,tbe
      write(nbc) qbs,qbn,qbw,qbe
      write(nbc) wbs,wbn,wbw,wbe
      write(nbc) ubs,ubn,ubw,ube
      write(nbc) vbs,vbn,vbw,vbe
      close(unit=nbc)
      print*,fname
!
!-----------------------------------------------------------------------
                      endif ! .not.global
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine gtlli(run,global,idat,ihrst,ihr,wb,sb,dlm,dph,sm,hgt
     &                ,pmsl,hsp,tsp,usp,vsp,qsp,wsp)
!
!-----------------------------------------------------------------------
!
      use module_flt,only:prefft,fftfhn,fftfwn
!
!-----------------------------------------------------------------------
      include 'llgrid.inc'
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      parameter(dtr=3.1415926535897932384626433832795/180.)
      parameter(epsw=1.e-8)
!
      integer,parameter:: 
     & klog=4
     &,kint=4
     &,kfpt=4
!
      integer(kind=kint),dimension(1:15)::
     & nfftrh
     &,nfftrw 
!
      real(kind=kfpt),dimension(1:2*(imi-1))::
     & wfftrh
     &,wfftrw
!
      integer(kind=kint),dimension(1:jmi)::
     & khfilt
     &,kvfilt
!
      real(kind=kfpt),dimension(1:imi,1:jmi)::
     & hfilt
     &,vfilt
!
      dimension dgwtbl(6),dgztbl(7),dznoah(lm)
!
      dimension sm(imi,jmi),hgt(imi,jmi),pmsl(imi,jmi)
     &         ,landuse(imi,jmi),ltopsoil(imi,jmi)
      dimension sst(imi,jmi),sstg(imi,jmi),snow(imi,jmi),cice(imi,jmi)
     &         ,epsr(imi,jmi),cmc(imi,jmi),sr(imi,jmi),dgt(imi,jmi)
     &         ,skint(imi,jmi),snowh(imi,jmi),snoalb(imi,jmi)
     &         ,vegfrac(imi,jmi),z0(imi,jmi),z0base(imi,jmi)
!!!CPEREZ1 
     &         ,landusenew(imi,jmi)
!!!CPEREZ1
      dimension alvsf(imi,jmi),alnsf(imi,jmi),alvwf(imi,jmi)
     &         ,alnwf(imi,jmi),facsf(imi,jmi),facwf(imi,jmi)
     &         ,albedorrtm(4,imi,jmi)
!
      real,parameter::
     & alblmx=.80
     &,alblmn=.06
     &,albomx=.06
     &,albomn=.06
     &,albimx=.80
     &,albimn=.06
     &,albjmx=.80
     &,albjmn=.06
     &,albsmx=.80
     &,albsmn=.06
     &,epsalb=.001
!!!CPEREZ1
      dimension smt(im,jm),cic(im,jm)
      dimension gsst(imll,jmll),gsnow(imll,jmll),gcice(imll,jmll)
      dimension psin(imll,jmll)
!
!zjlsm      dimension smst(nwets,imi,jmi),stmp(kms,imi,jmi)
      dimension smst(nwets,imi,jmi),sh2o(nwets,imi,jmi)
     &         ,stmp(nwets,imi,jmi)
      dimension gsmst(4,imll,jmll),gstmp(4,imll,jmll)
!
      dimension hin(imll,jmll,lmll)
      dimension tin(imll,jmll,lmll)
      dimension uin(imll,jmll,lmll)
      dimension vin(imll,jmll,lmll)
      dimension qin(imll,jmll,lmll)
      dimension win(imll,jmll,lmll)
!
      dimension hsp(imi,jmi,lmll)
      dimension tsp(imi,jmi,lmll)
      dimension usp(imi,jmi,lmll)
      dimension vsp(imi,jmi,lmll)
      dimension qsp(imi,jmi,lmll)
      dimension wsp(imi,jmi,lmll)
!
      integer inv(4,imi,jmi),jnv(4,imi,jmi)
      integer inh(4,imi,jmi),jnh(4,imi,jmi)
      dimension coh(3,imi,jmi)
      dimension cov(3,imi,jmi)
!
      dimension ww(imll,jmll),wfb(imi,jmi)
!
      logical run,global
!
      integer idat(3),ihrst,ihr
!
      character*3  ced
      character*128 fname
      data fname/'                                '/
!
!-----------------------------------------------------------------------
!
      write(fname,'(a,i3.3)')
     & '../output/llspl.',ihr
      print*,'fname=',fname
      open (unit=2,file=fname
     &     ,status='old',form='unformatted')
!
      read (2) run,idat,ihrst,ihrf
      read (2) ((psin(i,j),i=1,imll),j=1,jmll)
      read (2) (((hin(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)
      read (2) (((tin(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)
      read (2) (((uin(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)
      read (2) (((vin(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)
      read (2) (((qin(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)
      read (2) (((win(i,j,l),i=1,imll),j=1,jmll),l=1,lmll)      
      close(2)
!
      print*,' read from file: ',fname
      print*,' run=',run,' idat=',idat,' ihrst=',ihrst,' ihr=',ihr
!
!      do i=1,imll
!        print*,'i=',i,' us=',uin(i,2,15),' vs=',vin(i,2,15)
!     &        ,' un=',uin(i,jmll-1,15),' vn=',vin(i,jmll-1,15)
!      enddo
!
!-----------------------------------------------------------------------
!***  umo domain geometry
!-----------------------------------------------------------------------
!
      wb=wbd*dtr
      sb=sbd*dtr
      tph0=tph0d*dtr
      ctph0=cos(tph0)
      stph0=sin(tph0)
      dlm=dlmd*dtr
      dph=dphd*dtr
!
!-----------------------------------------------------------------------
!
      call preina
     &(global
     &,wb,sb,tlm0d,ctph0,stph0,dlm,dph
     &,imi,jmi
     &,coh,inh,jnh
     &,cov,inv,jnv)
!
!-----------------------------------------------------------------------
!***  mean sea level pressure
!-----------------------------------------------------------------------
!
              do j=1,jmll
          do i=1,imll
      ww(i,j)=psin(i,j)
          enddo
              enddo
              call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
              do j=1,jmi
          do i=1,imi
      pmsl(i,j)=wfb(i,j)
          enddo
              enddo
!-----------------------------------------------------------------------
                  do l=1,lmll
!-------------geopotential----------------------------------------------
              do j=1,jmll
          do i=1,imll
      ww(i,j)=hin(i,j,l)
          enddo
              enddo
              call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
              do j=1,jmi
          do i=1,imi
      hsp(i,j,l)=wfb(i,j)
          enddo
              enddo
!-------------temperature----------------------------------------------
              do j=1,jmll
          do i=1,imll
      ww(i,j)=tin(i,j,l)
          enddo
              enddo
              call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
              do j=1,jmi
          do i=1,imi
      tsp(i,j,l)=wfb(i,j)
          enddo
              enddo
!-------------u wind----------------------------------------------------
        do j=1,jmll
          do i=1,imll
            ww(i,j)=uin(i,j,l)
          enddo
        enddo

!          do i=1,imll
!            write(0,*) 'u ',l,ww(i,jmll-2),ww(i,jmll-1),ww(i,jmll)
!          enddo
!
        call bilinb(cov,inv,jnv,imll,jmll,imi,jmi,ww,wfb)
!
        do j=1,jmi-1
          tpv=sb+0.5*dph+(j-1)*dph
          do i=1,imi
            usp(i,j,l)=wfb(i,j)  !zjtest  *cos(tpv)
          enddo
        enddo
!
        do i=1,imi
          usp(i,jmi,l)=usp(i,jmi-1,l)
        enddo
!-------------v wind----------------------------------------------------
        do j=1,jmll
          do i=1,imll
            ww(i,j)=vin(i,j,l)
          enddo
        enddo

!          do i=1,imll
!            write(0,*) 'v ',l,ww(i,jmll-2),ww(i,jmll-1),ww(i,jmll)
!          enddo

!
        call bilinb(cov,inv,jnv,imll,jmll,imi,jmi,ww,wfb)
!
        do j=1,jmi-1
          tpv=sb+0.5*dph+(j-1)*dph
          do i=1,imi
            vsp(i,j,l)=wfb(i,j)  !zjtest  *cos(tpv)
          enddo
        enddo
!
        do i=1,imi
          vsp(i,jmi,l)=vsp(i,jmi-1,l)
        enddo
!-------------spec. hum.------------------------------------------------
              do j=1,jmll
          do i=1,imll
      ww(i,j)=qin(i,j,l)
          enddo
              enddo
              call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
              do j=1,jmi
          do i=1,imi
      qsp(i,j,l)=wfb(i,j)
          enddo
              enddo
!-------------cloud water-----------------------------------------------
              do j=1,jmll
          do i=1,imll
      ww(i,j)=win(i,j,l)
          enddo
              enddo
              call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
              do j=1,jmi
          do i=1,imi
      wsp(i,j,l)=wfb(i,j)
          enddo
              enddo
!-----------------------------------------------------------------------
                  enddo
!
!-----------------------------------------------------------------------
!***  transform ll wind into tll
!-----------------------------------------------------------------------
!
      if(.not.global) then
        do l=1,lmll
!
!-----------------------------------------------------------------------
!
          tph=sb-dph+0.5*dph
!
          do j=1,jmi-1
            tph=tph+dph
            do i=1,imi
              tlm=wb+dlm*(i-1)+0.5*dlm
!
              pus=usp(i,j,l)
              pvs=vsp(i,j,l)
!
              call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
              call ltlwin(almd,aphd,pus,pvs,tlm0d,tph0d,tpus,tpvs)
!
              usp(i,j,l)=tpus
              vsp(i,j,l)=tpvs
            enddo
          enddo
          do i=1,imi
            usp(i,jmi,l)=usp(i,jmi-1,l)
            vsp(i,jmi,l)=vsp(i,jmi-1,l)
          enddo
!
!-----------------------------------------------------------------------
        enddo
!-----------------------------------------------------------------------
!
      else
!
!-----------------------------------------------------------------------
!
        go to 4444

        call prefft
     &  (imi,jmi
     &  ,dlmd,dphd,sbd
     &  ,khfilt,kvfilt
     &  ,hfilt,vfilt
     &  ,wfftrh,nfftrh,wfftrw,nfftrw)
!
         print*,'Fourier filtering'
!
         call fftfhn
     &   (imi,jmi,lmll
     &   ,khfilt
     &   ,hfilt
     &   ,hsp
     &   ,wfftrh,nfftrh)
!
         call fftfhn
     &   (imi,jmi,lmll
     &   ,khfilt
     &   ,hfilt
     &   ,qsp
     &   ,wfftrh,nfftrh)
!
         call fftfhn
     &   (imi,jmi,lmll
     &   ,khfilt
     &   ,hfilt
     &   ,wsp
     &   ,wfftrh,nfftrh)
!
         call fftfwn
     &   (imi,jmi,lmll
     &   ,kvfilt
     &   ,vfilt
     &   ,usp
     &   ,wfftrw,nfftrw)
!
         call fftfwn
     &   (imi,jmi,lmll
     &   ,kvfilt
     &   ,vfilt
     &   ,vsp
     &   ,wfftrw,nfftrw)

 4444 continue


!-----------------------------------------------------------------------
      endif
!-----------------------------------------------------------------------
!      do l=1,lmll
!      umx=-99999999.
!      umn=99999999.
!      vmx=-99999999.
!      vmn=99999999.
!      hmx=-99999999.
!      hmn=99999999.
!      qmx=-99999999.
!      qmn=99999999.
!          do j=1,jmi
!              do i=1,imi
!                  umx=max(usp(i,j,l),umx)
!                  umn=min(usp(i,j,l),umn)
!                  vmx=max(vsp(i,j,l),vmx)
!                  vmn=min(vsp(i,j,l),vmn)
!                  hmx=max(hsp(i,j,l),hmx)
!                  hmn=min(hsp(i,j,l),hmn)
!                  qmx=max(qsp(i,j,l),qmx)
!                  qmn=min(qsp(i,j,l),qmn)
!              enddo
!          enddo
!      write(11,*)'l=',l,'umx=',umx,' umn=',umn,' vmx=',vmx,' vmn=',vmn
!      write(11,*)'l=',l,'hmx=',hmx,' hmn=',hmn,' qmx=',qmx,' qmn=',qmn
!      enddo
!
!-----------------------------------------------------------------------
!
      if(ihr.gt.0) return
!
!-----------------------------------------------------------------------
!***  surface and soil data
!-----------------------------------------------------------------------
!
      write(ced,'(i3.3)')ihr
!-----------------------------------------------------------------------
      fname='../output/deeptemperature'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) dgt
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/snowalbedo'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) snoalb
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/sst05'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) sst
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/z0'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) z0
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/z0base'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) z0base
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/landuse'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) landuse
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
!!!CPEREZ1
      fname='../output/landusenew'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) landusenew
      close(2)
      write(*,*)'*** read ',fname
!!!CPEREZ1
!-----------------------------------------------------------------------
      fname='../output/topsoiltype'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) ltopsoil
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
      fname='../output/vegfrac'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) vegfrac
      close(2)
      write(*,*)'*** read ',fname
!-----------------------------------------------------------------------
!!!CPEREZ1
      fname='../output/albedorrtm'
      open(unit=2,file=fname,status='old',form='unformatted')
      read(2) alvsf
      read(2) alnsf
      read(2) alvwf
      read(2) alnwf
      read(2) facsf
      read(2) facwf
      close(2)
      write(*,*)'*** read ',fname

      write(*,*) 'new albedos'
      write(*,*) 'alvsf'
      do j=1,jmi,40
        write(*,7777) (alvsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alnsf'
      do j=1,jmi,40
        write(*,7777) (alnsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alvwf'
      do j=1,jmi,40
        write(*,7777) (alvwf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alnwf'
      do j=1,jmi,40
        write(*,7777) (alnwf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'facsf'
      do j=1,jmi,40
        write(*,7777) (facsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'facwf'
      do j=1,jmi,40
        write(*,7777) (facwf(i,jmi+1-j),i=1,imi,40)
      enddo
!!!CPEREZ1


!!!CPEREZ1
!-----------------------------------------------------------------------
      fname='../output/llgsst'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) gsst
      close(2)
      write(*,*)'*** read ',fname
!
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gsst,sstg)
!-----------------------------------------------------------------------
      fname='../output/llgsno'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) gsnow
      close(2)
      write(*,*)'*** read ',fname
!
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gsnow,snow)
!
      do j=1,jmi
        do i=1,imi
!zjtest          if(snow(i,j).lt.2. ) snow(i,j)=0.
!not for wrf          snow(i,j)=snow(i,j)*0.001 ! convert from mm/m2 to m/m2
          snowh(i,j)=snow(i,j)*5.   ! snow height from water equiv.
        enddo
      enddo
!
!-----------------------------------------------------------------------
!
      fname='../output/llgcic'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) gcice
      close(2)
      write(*,*)'*** read ',fname
!
      call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,gcice,cice)
!
      do j=1,jmi
        do i=1,imi
          if(cice(i,j).gt.0.5.and.sm(i,j).gt.0.5
!zj     &    .or.sstg(i,j).le.273.15-1.8) then
     &      ) then
            cice(i,j)=1.
            sm(i,j)=0.
          else
            cice(i,j)=0.
          endif
        enddo
      enddo
!
!-----------------------------------------------------------------------
!***  remove single point ice bergs and water pools in ice
!-----------------------------------------------------------------------
!
      if(im.gt.imi) then
        call padh2(cice,cic)
        call padh2(sm  ,smt)
        call fix(cic,smt)
        call croph2(cic,cice)
        call croph2(smt,sm  )
      else
        call fix(cice,sm)
      endif
!
!----------------------------------------------------------------------
!
      do j=1,jmi
        do i=1,imi
          if(cice(i,j).gt.0.5) then
!            z0      (i,j)=0.0013
!            z0base  (i,j)=0.0013
            landuse (i,j)=24
!!!CPEREZ1
            landusenew (i,j)=15
!!!CPEREZ1
            ltopsoil(i,j)=16
            vegfrac (i,j)=0.
            sst     (i,j)=271.35
          endif
          if(sm  (i,j).gt.0.5) then
            snow    (i,j)=0.
            snowh   (i,j)=0.
            z0      (i,j)=0.0013
            z0base  (i,j)=0.0013
            landuse (i,j)=16
!!!CPEREZ1
            landusenew (i,j)=15
!!!CPEREZ1
            ltopsoil(i,j)=14
            vegfrac (i,j)=0.
            if(sst (i,j).lt.271.15) sst (i,j)=271.15
            if(sst (i,j).gt.308.15) sst (i,j)=308.15
            if(sstg(i,j).lt.271.15) sstg(i,j)=271.15
            if(sstg(i,j).gt.308.15) sstg(i,j)=308.15
          endif
        enddo
      enddo
!-----------------------------------------------------------------------
!      do j=1,jmi
!        do i=1,imi
!          if(snoalb(i,j).lt.0.1.and.
!     &       (sm(i,j).lt.0.5.or.cice(i,j).gt.0.5)) snoalb(i,j)=0.70
!        enddo
!      enddo
!
!-----------------------------------------------------------------------
!***  soil moisture and temperature
!-----------------------------------------------------------------------
!
      fname='../output/llsmst'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) gsmst
      close(2)
      write(*,*)'*** read ',fname
!
      fname='../output/llstmp'
      open(unit=2,file=fname,status='old',form='unformatted')
      read (2) gstmp
      close(2)
      write(*,*)'*** read ',fname
!
!-----------------------------------------------------------------------
!
!      write(*,*) 'First cice from global'
 7777 format(' ',20f6.2)
!          do j=1,jmi,20
!      write(*,7777) (cice(i,jmi+1-j),i=1,imi,40)
!          enddo
!
!-----------------------------------------------------------------------
!***  adjustment and interpolation of soil parameters
!-----------------------------------------------------------------------
!
      do l=1,4
!
!-----------------------------------------------------------------------
!
        write(*,*) 'gsmst, l ',l
        do j=jmll,1,-50
          write(*,7777) (gsmst(l,i,j),i=1,imll,60)
        enddo
        write(*,*) 'gstmp, l ',l
        do j=jmll,1,-50
          write(*,7777) (gstmp(l,i,j),i=1,imll,60)
        enddo
        write(*,*) 'gsst, l ',l
        do j=jmll,1,-50
          write(*,7777) (gsst (i,j  ),i=1,imll,60)
        enddo
!
        do j=1,jmll
          do i=1,imll
            if(gsmst(l,i,j).lt.epsw) gsmst(l,i,j)=epsw
            if(gsmst(l,i,j).gt.0.50) gsmst(l,i,j)=0.50
            ww(i,j)=gsmst(l,i,j)
          enddo
        enddo
!
        call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
!
        do j=1,jmi
          do i=1,imi
            smst(l,i,j)=wfb(i,j) 
          enddo
        enddo
!
        do j=1,jmll
          do i=1,imll
            if(gstmp(l,i,j).gt.190..and.gstmp(l,i,j).lt.356.) then
              ww(i,j)=gstmp(l,i,j)
            else
              ww(i,j)=gsst(i,j)
            endif
          enddo
        enddo
!
        call bilinb(coh,inh,jnh,imll,jmll,imi,jmi,ww,wfb)
!
        do j=1,jmi
          do i=1,imi
            stmp(l,i,j)=wfb(i,j)
          enddo
        enddo
!-----------------------------------------------------------------------
      enddo
!
!-----------------------------------------------------------------------
!*** set resolution parameters
!-----------------------------------------------------------------------
!
      dgztbl(1)=0.0 
      dgztbl(2)=0.10-dgztbl(1)
      dgztbl(3)=0.30
      dgztbl(4)=0.60
      dgztbl(5)=1.00

      dgwtbl(1)=dgztbl(1)+dgztbl(2)
      dznoah(1)=dgwtbl(1)
          do l=2,nwets
      dgwtbl(l)=dgztbl(l+1)
      dznoah(l)=dgztbl(l+1)
          enddo
!
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
!-----------------------------------------------------------------------
!
          epsr (i,j)=1.
          cmc  (i,j)=0.
          sr   (i,j)=0.
          skint(i,j)=sstg(i,j)
!
!-----------------------------------------------------------------------
          if(cice(i,j).lt.0.5.and.sm(i,j).lt.0.5) then ! land points
!-----------------------------------------------------------------------
!
!zjlsm            do l=nwets+1,2,-1
!zjlsm              stmp(l,i,j)=stmp(l-1,i,j)
!zjlsm            enddo
!zjlsm!            stmp(1,i,j)=sstg(i,j)
!zjlsm            stmp(kms,i,j)=stmp(nwets+1,i,j)
!
            do l=1,nwets
              if(smst(l,i,j).lt.0.) then 
                smst(l,i,j)=epsw
              endif
              if(smst(l,i,j).gt.1.) then 
                smst(l,i,j)=1.
              endif
            enddo
!-----------------------------------------------------------------------
          elseif(cice(i,j).gt.0.) then ! sea ice
!-----------------------------------------------------------------------
!zjlsm            do l=nwets+1,2,-1
!zjlsm              stmp(l,i,j)=stmp(l-1,i,j)
!zjlsm            enddo
!zjlsm!            stmp(1,i,j)=sstg(i,j)
!zjlsm            stmp(kms,i,j)=stmp(nwets+1,i,j)
!
            do l=1,nwets
              smst(l,i,j)=1. !zjlsm 0.
            enddo
!-----------------------------------------------------------------------
          else ! sea points
!-----------------------------------------------------------------------
!zjlsm            do l=1,kms
            do l=1,nwets
              stmp(l,i,j)=sstg(i,j)
            enddo
!
            do l=1,nwets
              smst(l,i,j)=1. !zjlsm 0.
            enddo
!-----------------------------------------------------------------------
          endif
          if(smst(1,i,j).gt.1.) print*,'cice,sm ',i,j,cice(i,j),sm(i,j)
!-----------------------------------------------------------------------
        enddo
      enddo
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          do l=1,nwets
            if(smst(l,i,j).gt.epsw) then
              if    (stmp(l,i,j).gt.273.15+5.) then
                sh2o(l,i,j)=smst(l,i,j)
              elseif(stmp(l,i,j).lt.273.15-5.) then
                sh2o(l,i,j)=0.
              else
                sh2o(l,i,j)=smst(l,i,j)*(stmp(l,i,j)-(273.15-5.0))/10.
              endif
            endif
          enddo
        enddo
      enddo
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          if(dgt(i,j).lt.173.) dgt(i,j)=max(dgt(i,j),stmp(nwets,i,j))
          if(cice(i,j).gt..05) dgt(i,j)=sstg(i,j)
        enddo
      enddo
!-----------------------------------------------------------------------
!!!CPEREZ1... NEW ALBEDOS climatology, based on surface veg types
!!!CPEREZ1... NEW ALBEDOS modis retrieval based surface albedo scheme
!      ALBLMX=.99
!      ALBLMN=.01
!      ALBOMX=.06
!      ALBOMN=.06
!      ALBIMX=.80
!      ALBIMN=.06
!      ALBJMX=.80
!      ALBJMN=.06
!      ALBSMX=.99
!      ALBSMN=.01
!      EPSALB=.001
      
       do j=1,jmi
        do i=1,imi
       albedorrtm(1,i,j)=alvsf(i,j)
       albedorrtm(2,i,j)=alnsf(i,j)
       albedorrtm(3,i,j)=alvwf(i,j)
       albedorrtm(4,i,j)=alnwf(i,j)
        enddo
       enddo

          do k=1,4

       do j=1,jmi
        do i=1,imi

!  Lower bound check over bare land
       if(cice(i,j).lt.0.5.and.sm(i,j).lt.0.5.and.
     &   snow(i,j).le.0.0.and.albedorrtm(k,i,j).lt.alblmn-epsalb) then

            albedorrtm(k,i,j)=alblmn

       endif
!  Upper bound check over bare land
       if(cice(i,j).lt.0.5.and.sm(i,j).lt.0.5.and.
     &   snow(i,j).le.0.0.and.albedorrtm(k,i,j).gt.alblmx+epsalb) then

           albedorrtm(k,i,j)=alblmx

       endif
!  Lower bound check over snow covered land
       if(cice(i,j).lt.0.5.and.sm(i,j).lt.0.5.and.
     &   snow(i,j).gt.0.0.and.albedorrtm(k,i,j).lt.albsmn-epsalb) then

          albedorrtm(k,i,j)=albsmn

       endif
!  Upper bound check over snow covered land
       if(cice(i,j).lt.0.5.and.sm(i,j).lt.0.5.and.
     &   snow(i,j).gt.0.0.and.albedorrtm(k,i,j).gt.albsmx+epsalb) then

          albedorrtm(k,i,j)=albsmx

       endif
!  Lower bound check over open ocean
       if(sm(i,j).gt.0.5.and.
     &    albedorrtm(k,i,j).lt.albomn-epsalb) then

          albedorrtm(k,i,j)=albomn

       endif
!  Upper bound check over open ocean
       if(sm(i,j).gt.0.5.and.
     &    albedorrtm(k,i,j).gt.albomx+epsalb) then

          albedorrtm(k,i,j)=albomx

       endif
!  Lower bound check over sea ice without snow
       if(cice(i,j).gt.0.5.and.snow(i,j).le.0.0.and.
     &   albedorrtm(k,i,j).lt.albimn-epsalb) then

          albedorrtm(k,i,j)=albimn

       endif
!  Upper bound check over sea ice without snow
       if(cice(i,j).gt.0.5.and.snow(i,j).le.0.0.and.
     &   albedorrtm(k,i,j).gt.albimx+epsalb) then

          albedorrtm(k,i,j)=albimx

       endif
!  Lower bound check over sea ice with snow
       if(cice(i,j).gt.0.5.and.snow(i,j).gt.0.0.and.
     &    albedorrtm(k,i,j).lt.albjmn-epsalb) then

          albedorrtm(k,i,j)=albjmn

       endif
!  Upper bound check over sea ice with snow
       if(cice(i,j).gt.0.5.and.snow(i,j).gt.0.0.and.
     &    albedorrtm(k,i,j).gt.albjmx+epsalb) then

          albedorrtm(k,i,j)=albjmx

       endif

        enddo
      enddo

        enddo

       do j=1,jmi
        do i=1,imi
         alvsf(i,j)= albedorrtm(1,i,j)
         alnsf(i,j)= albedorrtm(2,i,j)
         alvwf(i,j)= albedorrtm(3,i,j)
         alnwf(i,j)= albedorrtm(4,i,j)
        enddo
       enddo
!
!!!CPEREZ1
!
!-----------------------------------------------------------------------
      write(*,*) 'first cice from global'
 8888 format(' ',20f4.0)
      do j=1,jmi,40
        write(*,8888) (cice(i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      write(*,*) 'gfs sst'
      do j=1,jmi,40
        write(*,7777) (sstg(i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      write(*,*) 'sst05'
      do j=1,jmi,40
        write(*,7777) (sst (i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      write(*,*) 'deep ground temperature'
      do j=1,jmi,40
        write(*,7777) (dgt(i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      write(*,*) 'snow'
      do j=1,jmi,40
        write(*,7777) (snow(i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      write(*,*) 'snow albedo'
      do j=1,jmi,40
        write(*,7777) (snoalb(i,jmi+1-j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
!!!CPEREZ1
      write(*,*) 'new albedos corrected'
      write(*,*) 'alvsf'
      do j=1,jmi,40
        write(*,7777) (alvsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alnsf'
      do j=1,jmi,40
        write(*,7777) (alnsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alvwf'
      do j=1,jmi,40
        write(*,7777) (alvwf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'alnwf'
      do j=1,jmi,40
        write(*,7777) (alnwf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'facsf'
      do j=1,jmi,40
        write(*,7777) (facsf(i,jmi+1-j),i=1,imi,40)
      enddo
      write(*,*) 'facwf'
      do j=1,jmi,40
        write(*,7777) (facwf(i,jmi+1-j),i=1,imi,40)
      enddo
!!!CPEREZ1
!-----------------------------------------------------------------------
!!!CPEREZ1
      fname='../output/albedorrtmcorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) alvsf
      write(2) alnsf
      write(2) alvwf
      write(2) alnwf
      write(2) facsf
      write(2) facwf
      close(2)
      write(*,*)'*** wrote ',fname
!!!CPEREZ1
!-----------------------------------------------------------------------
      fname='../output/dzsoil'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) dznoah
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/deeptemperature'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) dgt
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/snowalbedo'
      open(unit=2,file=fname,status='old',form='unformatted')
      write(2) snoalb
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/tskin'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) skint
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/sst'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) sstg ! gfs sst
!      write(2) sst  ! sst05
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/snow'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) snow
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/snowheight'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) snowh
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/cice'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) cice
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/seamaskcorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) sm
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/landusecorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) landuse
      close(2)
      write(*,*)'*** wrote ',fname
!
!!!CPEREZ1
      fname='../output/landusenewcorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) landusenew
      close(2)
      write(*,*)'*** wrote ',fname
!!!CPEREZ1
!
      fname='../output/topsoiltypecorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) ltopsoil
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/vegfraccorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) vegfrac
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/z0corr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) z0
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/z0basecorr'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) z0base
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/emissivity'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) epsr
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/canopywater'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) cmc
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/frozenprecratio'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) sr
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/smst'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) smst
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/sh2o'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) sh2o
      close(2)
      write(*,*)'*** wrote ',fname
!
      fname='../output/stmp'
      open(unit=2,file=fname,status='unknown',form='unformatted')
      write(2) stmp
      close(2)
      write(*,*)'*** wrote ',fname
!
!----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine preina
     &(global
     &,wb,sb,tlm0d,ctph0,stph0,dlm,dph
     &,imi,jmi
     &,coh,inh,jnh
     &,cov,inv,jnv)
!
!-----------------------------------------------------------------------
      include 'llgrid.inc'
!-----------------------------------------------------------------------
!
      parameter(dtr=3.1415926535897932384626433832795/180.,rtd=1./dtr)
!
!-----------------------------------------------------------------------
!
      logical global
      dimension coh(3,imi,jmi)
      dimension cov(3,imi,jmi)
      integer inh(4,imi,jmi),jnh(4,imi,jmi)
      integer inv(4,imi,jmi),jnv(4,imi,jmi)
!
!-----------------------------------------------------------------------
!***  entry to the umo i,j loop
!***  neighbour avn index identification (avn data defined in ll system)
!-----------------------------------------------------------------------
!               umo height pts
!-----------------------------------------------------------------------
!
      tph=sb-dph
              do j=1,jmi
          tph=tph+dph
          tlm=wb-dlm
          do i=1,imi
      tlm=tlm+dlm
!
!-----------------------------------------------------------------------
!***  tll to ll conversion
!-----------------------------------------------------------------------
!
      if(.not.global) then
        call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
      else
        almd=tlm*rtd
        aphd=tph*rtd
      endif
!
!-----------------------------------------------------------------------
!-------------conversion from -180,180 range to 0,360 range-------------
!-----------------------------------------------------------------------
!
      if(almd.lt.0.) almd=360.+almd
!
!-----------------------------------------------------------------------
!-------------check if model point is out of llgrid domain--------------
!-----------------------------------------------------------------------
!
      if(almd.lt.bowest      .or.
     &   almd.gt.boeast+delon    ) then
!     &   almd.gt.boeast+delon.or.
!     &   aphd.lt.bosout      .or.
!     &   aphd.gt.bonort          ) then
        print*,'h point i=',i,' j=',j,almd,aphd
     &,' is out of the llgrid domain'
        print*,'program will stop'
        stop
      endif
!
!----------------------------------------------------------------------
!
      x=almd-bowest
      y=aphd-bosout
!
      indec=x/delon+1
      jndec=y/delat+1
!
      if(indec.eq.0) indec=imll
      if(indec.gt.imll) indec=1
      if(jndec.eq.0) jndec=1
      if(jndec.ge.jmll) jndec=jmll-1
!
      x=x-(indec-1)*delon
      y=y-(jndec-1)*delat
!-----------------------------------------------------------------------
      coh(1,i,j)=x/delon
      coh(2,i,j)=y/delat
      coh(3,i,j)=coh(1,i,j)*coh(2,i,j)
!-----------------------------------------------------------------------
      inh(1,i,j)=indec
      inh(3,i,j)=indec
          if(indec.lt.imll) then
      inh(2,i,j)=indec+1
      inh(4,i,j)=indec+1
          else
      inh(2,i,j)=1
      inh(4,i,j)=1
          endif      
!
      jnh(1,i,j)=jndec
      jnh(2,i,j)=jndec
      jnh(3,i,j)=jndec+1
      jnh(4,i,j)=jndec+1
!-----------------------------------------------------------------------
          enddo
              enddo
!
!-----------------------------------------------------------------------
!             model wind pts
!-----------------------------------------------------------------------
!
      tph=sb-dph+0.5*dph
!
              do j=1,jmi
          tph=tph+dph
          do i=1,imi
      tlm=wb+dlm*(i-1)+0.5*dlm
!
!-----------------------------------------------------------------------
!-------------tll to ll conversion--------------------------------------
!-----------------------------------------------------------------------
!
      if(.not.global) then
        call rtll(tlm,tph,tlm0d,ctph0,stph0,almd,aphd)
      else
        almd=tlm*rtd
        aphd=max(min(tph*rtd,bonort),bosout)
      endif
!
!-----------------------------------------------------------------------
!-------------conversion from -180,180 range to 0,360 range-------------
!-----------------------------------------------------------------------
!
      if(almd.lt.0.) almd=360.+almd
!
!-----------------------------------------------------------------------
!-------------check if model point is out of llgrid domain--------------
!-----------------------------------------------------------------------
!
      if(almd.lt.bowest      .or.
     &   almd.gt.boeast+delon.or.
     &   aphd.lt.bosout      .or.
     &   aphd.gt.bonort          ) then
        print*,'v point i=',i,' j=',j,' is out of llgrid domain'
        print*,'program will stop'
        stop
      endif
!-----------------------------------------------------------------------
      x=almd-bowest
      y=aphd-bosout
!
      indec=x/delon+1
      jndec=y/delat+1
!
      x=x-(indec-1)*delon
      y=y-(jndec-1)*delat
!-----------------------------------------------------------------------
      cov(1,i,j)=x/delon
      cov(2,i,j)=y/delat
      cov(3,i,j)=cov(1,i,j)*cov(2,i,j)
!-----------------------------------------------------------------------
      inv(1,i,j)=indec
      inv(3,i,j)=indec
          if(indec.lt.imll) then
      inv(2,i,j)=indec+1
      inv(4,i,j)=indec+1
          else
      inv(2,i,j)=1
      inv(4,i,j)=1
          endif
!
      jnv(1,i,j)=jndec
      jnv(2,i,j)=jndec
      jnv(3,i,j)=jndec+1
      jnv(4,i,j)=jndec+1
!
!-----------------------------------------------------------------------
          enddo
              enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine ltlwin(almd,aphd,pus,pvs,tlm0d,tph0d,tpus,tpvs)
!     ******************************************************************
!     *                                                                *
!     *  ll to tll transformation of velocity                          *
!     *                                                                *
!     *  programer: z.janjic, yugoslav fed. hydromet. inst.,           *
!     *                 beograd, 1982                                  *
!     *                                                                *
!     ******************************************************************
!
!-----------------------------------------------------------------------
      parameter(dtr=3.1415926535897932384626433832795/180.)
!-----------------------------------------------------------------------
!
      tph0=tph0d*dtr
      ctph0=cos(tph0)
      stph0=sin(tph0)
!
      relm=(almd-tlm0d)*dtr
      srlm=sin(relm)
      crlm=cos(relm)
!
      ph=aphd*dtr
      sph=sin(ph)
      cph=cos(ph)
!
      cc=cph*crlm
      tph=asin(ctph0*sph-stph0*cc)
      rctph=1./cos(tph)
!
      cray=stph0*srlm*rctph
      dray=(ctph0*cph+stph0*sph*crlm)*rctph
!
      tpus=dray*pus-cray*pvs
      tpvs=cray*pus+dray*pvs
!-----------------------------------------------------------------------
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine p2hyb
     &(run,global,idat,ihrst,ihrend,ihr,hgt,stdh,sm
     &,pmsl,htll,ttll,utll,vtll,qtll,wtll
     &,pdbs,pdbn,pdbw,pdbe
     &,tbs,tbn,tbw,tbe
     &,qbs,qbn,qbw,qbe
     &,wbs,wbn,wbw,wbe
     &,ubs,ubn,ubw,ube
     &,vbs,vbn,vbw,vbe)
!
!     *************************************************************     
!     *                                                           *
!     *  program for conversion from p to hybrid system           *     
!     *  using cubic splines.                                     *     
!     *  programers - s.nickovic & z.janjic                       *     
!     *         1981, yugoslav fed. hydromet. inst., beograd      *     
!     *                                                           *     
!     *************************************************************
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
      include 'llgrid.inc'
!-----------------------------------------------------------------------
!
      parameter
     &(g=9.8,r=287.04,gor=g/r)
      parameter 
     &(a=6376000.,pi=3.1415926535897932384626433832795)
      parameter
     &(dtr=3.1415926535897932384626433832795/180.)
      parameter
     &(tpi=pi+pi)
!-----------------------------------------------------------------------
      parameter
     &(a1=610.78,a2=17.2693882,a3=273.16,a4=35.86,pq0=379.90516
     &,tresh=1.1,pq0c=pq0*tresh
     &,elwv=2.50e6,dldt=2274.,row=1.e3
     &,eps=0.02,epsq=1.e-12)
!-----------------------------------------------------------------------
      logical run,global,bc
!
      dimension idat(3)
      dimension
     & xold(10),dold(10),y2s(10),pps(10),qqs(10)
                             d i m e n s i o n
     & dzsoil(lm)
     &,dsg(lm),sgm(lm+1)
     &,dsg1(lm),sgml1(lm),sg1(lm+1)
     &,dsg2(lm),sgml2(lm),sg2(lm+1)

                             d i m e n s i o n
     & xnew(lm)
     &,puv(lm),uhyb(lm),vhyb(lm)
     &,ph(lm+1),hhyb(lm+1)
     &,pq(lm),thyb(lm),qhyb(lm),whyb(lm)
                             d i m e n s i o n
!
!!! KARSTEN
!     & psl   (lmll),rpsl  (lmll),rhsl  (lmll)
     & psl1(26),psl2(47),psl(lmll),rpsl(lmll),rhsl(lmll)
!!! KARSTEN
     &,y2    (lmll+1),pp    (lmll+1),qq    (lmll+1)
     &,pslh  (lmll+1),hspl  (lmll+1)
     &,uspl  (lmll+1),vspl  (lmll+1)
     &,tspl  (lmll+1),qspl  (lmll+1),wspl  (lmll+1)
                             d i m e n s i o n
     & dxv   (jmi)
      logical
     & maskh (imi,jmi),maskv (imi,jmi)
      dimension
     & sm    (imi,jmi),hgt   (imi,jmi),stdh  (imi,jmi),fis  (imi,jmi)
     &,pmsl  (imi,jmi),pd    (imi,jmi)
!----------------------------------------------------------------------
      dimension
     & field(imi,jmi),ifield(imi,jmi),fieldn(nwets,imi,jmi)
!
      dimension
     & padded2(im,jm),ipadded2(im,jm),paddedn(nwets,im,jm)
!----------------------------------------------------------------------
                             d i m e n s i o n
     & utll(imi,jmi,lmll),vtll(imi,jmi,lmll)
     &,htll(imi,jmi,lmll),ttll(imi,jmi,lmll)
     &,qtll(imi,jmi,lmll),wtll(imi,jmi,lmll)
                             d i m e n s i o n
     & u(imi,jmi,lm),v(imi,jmi,lm)
     &,t(imi,jmi,lm),q(imi,jmi,lm),w(imi,jmi,lm)
     &,pint(imi,jmi,lm+1)
!
      dimension pdbs(imi,lnsh,2),pdbn(imi,lnsh,2)
     &         ,pdbw(lnsh,jmi,2),pdbe(lnsh,jmi,2)
!
      dimension tbs(imi,lnsh,lm,2),tbn(imi,lnsh,lm,2)
     &         ,tbw(lnsh,jmi,lm,2),tbe(lnsh,jmi,lm,2)
      dimension qbs(imi,lnsh,lm,2),qbn(imi,lnsh,lm,2)
     &         ,qbw(lnsh,jmi,lm,2),qbe(lnsh,jmi,lm,2)
      dimension wbs(imi,lnsh,lm,2),wbn(imi,lnsh,lm,2)
     &         ,wbw(lnsh,jmi,lm,2),wbe(lnsh,jmi,lm,2)
!
      dimension ubs(imi,lnsv,lm,2),ubn(imi,lnsv,lm,2)
     &         ,ubw(lnsv,jmi,lm,2),ube(lnsv,jmi,lm,2)
      dimension vbs(imi,lnsv,lm,2),vbn(imi,lnsv,lm,2)
     &         ,vbw(lnsv,jmi,lm,2),vbe(lnsv,jmi,lm,2)
!-----------------------------------------------------------------------
      character*128 fname
      data fname/'                                '/
!-----------------------------------------------------------------------
      data ntsd/0/
!
!-----------------------------------------------------------------------
!***  global data set
!-----------------------------------------------------------------------
!
!!! KARSTEN
      data psl2/100.,200.,300.,500.,700.,1000.,2000.,3000.,5000.,7000. !hires
     &        ,10000.,12500.,15000.,17500.,20000.,22500.,25000.,27500. !hires
     &        ,30000.,32500.,35000.,37500.,40000.,42500.,45000.,47500. !hires
     &        ,50000.,52500.,55000.,57500.,60000.,62500.,65000.,67500. !hires
     &        ,70000.,72500.,75000.,77500.,80000.,82500.,85000.,87500. !hires
     &        ,90000.,92500.,95000.,97500.,100000./
!
      data psl1/1000.,2000.,3000.,5000.,7000.,10000.,15000.,20000.     !1deg
     &        ,25000.,30000.,35000.,40000.,45000.,50000.,55000.,60000. !1deg
     &        ,65000.,70000.,75000.,80000.,85000.,90000.,92500.,95000. !1deg
     &        ,97500.,100000./
!!! KARSTEN
!
!------------------------------------------------------------------------
 2300 format(' *** ',i2,'.',i2,'.',i4,' ',i3,' gmt ***')                           
!------------------------------------------------------------------------
!
      print*,'*** Hi, this is p2hyb converting pressure data to hybrid.'
!
!------------------------------------------------------------------------
!***  BC masks
!------------------------------------------------------------------------
!
      bc=ihr.eq.0
!
!------------------------------------------------------------------------
!
      do j=1,jmi
        do i=1,imi
          maskh(i,j)=bc
          maskv(i,j)=bc
        enddo
      enddo
!---southern boundary----------------------------------------------------
      do j=1,lnsh+1 ! to provide mass data at points neighboring v points
        do i=1,imi
          maskh(i,j)=.true.
        enddo
      enddo
      do j=1,lnsv
        do i=1,imi-1
          maskv(i,j)=.true.
        enddo
      enddo
!---northern boundary----------------------------------------------------
      do j=jmi-lnsh,jmi
        do i=1,imi
          maskh(i,j)=.true.
        enddo
      enddo
      do j=jmi-lnsv,jmi-1
        do i=1,imi-1
          maskv(i,j)=.true.
        enddo
      enddo
!---western boundary-----------------------------------------------------
      do j=1,jmi
        do i=1,lnsh+1
          maskh(i,j)=.true.
        enddo
      enddo
      do j=1,jmi-1
        do i=1,lnsv
          maskv(i,j)=.true.
        enddo
      enddo
!---eastern boundary-----------------------------------------------------
      do j=1,jmi
        do i=imi-lnsh,imi
          maskh(i,j)=.true.
        enddo
      enddo
      do j=1,jmi-1
        do i=imi-lnsv,imi-1
          maskv(i,j)=.true.
        enddo
      enddo
!-----------------------------------------------------------------------
      do l=1,lmll
        umx=-99999999.
        umn=99999999.
        vmx=-99999999.
        vmn=99999999.
        hmx=-99999999.
        hmn=99999999.
        tmx=-99999999.
        tmn=99999999.
        qmx=-99999999.
        qmn=99999999.
        do j=1,jmi
          do i=1,imi
            umx=max(utll(i,j,l),umx)
            umn=min(utll(i,j,l),umn)
            vmx=max(vtll(i,j,l),vmx)
            vmn=min(vtll(i,j,l),vmn)
            hmx=max(htll(i,j,l),hmx)
            hmn=min(htll(i,j,l),hmn)
            tmx=max(ttll(i,j,l),tmx)
            tmn=min(ttll(i,j,l),tmn)
            qmx=max(qtll(i,j,l),qmx)
            qmn=min(qtll(i,j,l),qmn)
          enddo
        enddo
        write(*,*)'l=',l,'umx=',umx,' umn=',umn,' vmx=',vmx,' vmn=',vmn
        write(*,*)'l=',l,'hmx=',hmx,' hmn=',hmn
        write(*,*)'l=',l,'tmx=',tmx,' tmn=',tmn,' qmx=',qmx,' qmn=',qmn
      enddo
!
!------------------------------------------------------------------------
!***  vertical coordinate
!------------------------------------------------------------------------
!
      open(unit=1,file='../output/dsg'
     &    ,status='unknown',form='unformatted')
      read (1) pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
      close(1)
!
!------------------------------------------------------------------------
!***  p2hyb derived constants
!------------------------------------------------------------------------
!
!!! KARSTEN
      if(nz.eq.26) then
        do l=1,lmll
          psl(l)=psl1(l)
        enddo
      elseif(nz.eq.47) then
        do l=1,lmll
          psl(l)=psl2(l)
        enddo
      endif
      pld2=psl(lmll-2)                                             
      pld1=psl(lmll-1)                                                
      pld0=psl(lmll  )
!!! KARSTEN
!
!-----------------------------------------------------------------------
!
      do l=1,lm
        do j=1,jmi
          do i=1,imi
            u(i,j,l)=0.
            v(i,j,l)=0.
            t(i,j,l)=0.
            q(i,j,l)=0.
            w(i,j,l)=0.
          enddo
        enddo
      enddo
!-----------------------------------------------------------------------
      do l=1,lmll+1
        y2(l)=0.
      enddo
!-----------------------------------------------------------------------
!
      write(*,2300) idat,ihrst                                         
!
!-----------------------------------------------------------------------
!***  computation of pd, zeta and related variables
!-----------------------------------------------------------------------
!
      do j=1,jmi
        do i=1,imi
!
!-----------------------------------------------------------------------
          if(maskh(i,j)) then
!-----------------------------------------------------------------------
!
          hgtp=hgt(i,j)
!
!-----------------------------------------------------------------------
!          if(sm(i,j).gt.0.5.and.abs(hgtp).lt.1.
!     &       .and.abs(pmsl(i,j)-100000.).gt.0100.) then
!-----------------------------------------------------------------------
!            psp=pmsl(i,j)
!-----------------------------------------------------------------------
!          else
!-----------------------------------------------------------------------
            hld0=htll(i,j,lmll)
!-----------------------------------------------------------------------
!
            if(hld0.gt.hgtp)    then
!
!-----------------------------------------------------------------------
!--------------if sfc below lowest p sfc, extrapolate downwards---------
!------------------------------------------------------------------------
!
              hld2=htll(i,j,lmll-2)
              hld1=htll(i,j,lmll-1)
!
              d1=(pld0-pld1)/(hld0-hld1)
              d2=(d1-(pld1-pld2)/(hld1-hld2))/(hld0-hld2)
!             d2=0.
!
              x=hgtp-hld0
              psp=d2*x*x+d1*x+pld0
!-----------------------------------------------------------------------
!
            else
!
!-----------------------------------------------------------------------
!--------------otherwise, use spline to interpolate---------------------
!-----------------------------------------------------------------------
!
              do ld=1,lmll
                rhsl(ld)=htll(i,j,lmll+1-ld)
                rpsl(ld)=psl(lmll+1-ld)
                y2(ld)=0.
              enddo
!
              call spline(lmll,rhsl,rpsl,y2,1,hgtp,psp,pp,qq)
!              print*,'psp  ',psp
!-----------------------------------------------------------------------
            endif
!-----------------------------------------------------------------------
!          endif
!-----------------------------------------------------------------------
            pdp=psp-pt
            pd(i,j)=pdp 
!
            pint(i,j,1)=pt                                                                
            pint(i,j,lm+1)=psp                                                
            do l=2,lm                                             
              pint(i,j,l)=sg2(l)*pdp+sg1(l)*pdtop+pt                                               
            enddo
!-----------------------------------------------------------------------
          endif
!-----------------------------------------------------------------------
        enddo
      enddo
!
!-----------------------------------------------------------------------
!
!!! KARSTEN
      PRINT*,' '
      PRINT*,' PINT: '
      PRINT 753,(pint(1,1,L),L=1,LM+1)
  753 FORMAT(1H ,7F10.2)
      pdmx=-99999999.
      pdmn=99999999.
      do j=1,jmi
        do i=1,imi
          pdmx=max(pd(i,j),pdmx)
          pdmn=min(pd(i,j),pdmn)
        enddo
      enddo
      PRINT*,' '
      PRINT*,' maxval PD: ',pdmx,' minval PD: ',pdmn
      PRINT*,' '
!!! KARSTEN
!
!-----------------------------------------------------------------------
!
      print*,'*** p2hyb completed computation of surface pressure'
!
!-----------------------------------------------------------------------
!--------------velocity spline interpolation inside the domain----------
!-----------------------------------------------------------------------
!
                  do j=1,jmi-1                                                   
              do i=1,imi-1
!
!-----------------------------------------------------------------------
          if(maskv(i,j)) then
!-----------------------------------------------------------------------
!
          do l=1,lmll                                                   
      pslh(l)=psl(l)
      uspl(l)=utll(i,j,l)                                                 
      vspl(l)=vtll(i,j,l)
          enddo                                               
!                                                                  
          do l=1,lm                                                 
      puv(l)=(pint(i  ,j  ,l  )+pint(i  ,j  ,l+1)
     &       +pint(i+1,j  ,l  )+pint(i+1,j  ,l+1)
     &       +pint(i  ,j+1,l  )+pint(i  ,j+1,l+1)
     &       +pint(i+1,j+1,l  )+pint(i+1,j+1,l+1))*0.125

          enddo
!                                                                
      pslpu=(pint(i  ,j  ,lm+1)+pint(i+1,j  ,lm+1)
     &      +pint(i  ,j+1,lm+1)+pint(i+1,j+1,lm+1))*0.25
!
          if (pslpu.le.pld0+eps) then ! eps necessary here
      lold=lmll
          else
      lold=lmll+1
      pslh(lold)=pslpu
!
      uld2=uspl(lmll-2)
      uld1=uspl(lmll-1)
      uld0=uspl(lmll  )
!
      d1=(uld0-uld1)/(pld0-pld1)
      d2=(d1-(uld1-uld2)/(pld1-pld2))/(pld0-pld2)
      d2=0.
      x=pslh(lold)-pld0
!
      uspl(lold)=d2*x*x+d1*x+uld0
!      uspl(lold)=uspl(lold-1)
!
      vld2=vspl(lmll-2)
      vld1=vspl(lmll-1)
      vld0=vspl(lmll  )
!
      d1=(vld0-vld1)/(pld0-pld1)
      d2=(d1-(vld1-vld2)/(pld1-pld2))/(pld0-pld2)
      d2=0.
      x=pslh(lold)-pld0
!
      vspl(lold)=d2*x*x+d1*x+vld0
!      vspl(lold)=vspl(lold-1)
          endif
!
          do l=1,lold
      y2(l)=0.
          enddo
!
      call spline(lold,pslh,uspl,y2,lm,puv,uhyb,pp,qq)                       
      call spline(lold,pslh,vspl,y2,lm,puv,vhyb,pp,qq)
!                                                                 
          do l=1,lm                                              
      u(i,j,l)=uhyb(l)                      
      v(i,j,l)=vhyb(l)                     
          enddo
!---------------------------------------------------------------------
          endif
!---------------------------------------------------------------------
              enddo
                  enddo
!---------------------------------------------------------------------
                  do l=1,lm
              do j=1,jmi-1
          do i=1,imi-1
          if(maskv(i,j)) then
      if(abs(u(i,j,l)).gt.120.) print*,'p2hyb,i,j,l,u',i,j,l,u(i,j,l)
      if(abs(v(i,j,l)).gt.120.) print*,'p2hyb,i,j,l,v',i,j,l,v(i,j,l)
          endif
          enddo
              enddo
                  enddo
!-----------------------------------------------------------------------
!
      print*,'*** p2hyb completed interpolation of wind'
!
!-----------------------------------------------------------------------
!--------------computation of hybrid temperatures-----------------------
!-----------------------------------------------------------------------
!
                  do j=1,jmi                                                    
              do i=1,imi                                                 
!
!-----------------------------------------------------------------------
          if(maskh(i,j)) then
!-----------------------------------------------------------------------
      ph(1)=pt                                                      
          do l=2,lm+1                                               
      ph(l)=pint(i,j,l)                                               
          enddo
          do l=1,lmll
      pslh(l)=psl(l)
      hspl(l)=htll(i,j,l)
          enddo
!
          if(pint(i,j,lm+1).le.pld0) then
      lold=lmll
          else
      lold=lmll+1
      pslh(lold)=pint(i,j,lm+1)
      hspl(lold)=hgt(i,j)
          endif
!
          do l=1,lold
      y2(l)=0.
          enddo
!
!-----------------------------------------------------------------------
!-------------temperatures----------------------------------------------  
!-----------------------------------------------------------------------
!
      call spline(lold,pslh,hspl,y2,lm+1,ph,hhyb,pp,qq)                   
!                                                                
          do l=1,lm                                                 
      t(i,j,l)=-(hhyb(l+1)-hhyb(l))*0.5
     &         *(pint(i,j,l+1)+pint(i,j,l))
     &         /(pint(i,j,l+1)-pint(i,j,l))*gor
!      t(i,j,l)=-(hhyb(l+1)-hhyb(l))
!     &         /(log(pint(i,j,l+1))-log(pint(i,j,l)))*gor
!-----------------------------------------------------------------------
!
!!! KARSTEN
!          if(t(i,j,l).lt.160..or.t(i,j,l).gt.350.) then
!      print*,'*** Temperature',i,j,l,t(i,j,l),sm(i,j),hgt(i,j)
!     *      ,hhyb(l+1),hhyb(l),pint(i,j,l+1),pint(i,j,l)
!      print*,'pslh',pslh
!      print*,'hspl',hspl
!          endif
!!! KARSTEN
!
!      print*,'t   ',t(i,j,l)
          enddo
!-----------------------------------------------------------------------
          endif
!-----------------------------------------------------------------------
              enddo
                  enddo
!-----------------------------------------------------------------------
      do j=1,jmi                                                   
        do i=1,imi                                                 
          if(maskh(i,j)) then
            fis(i,j)=g*hgt(i,j)
          endif
        enddo                                         
      enddo                                         
!--------------t, q and w spline interpolation inside the domain--------
                  do j=1,jmi                                                   
              do i=1,imi                                                  
!-----------------------------------------------------------------------
          if(maskh(i,j)) then
!-----------------------------------------------------------------------
          do l=1,lmll
      pslh(l)=psl(l)
      tspl(l)=ttll(i,j,l)
      qspl(l)=qtll(i,j,l)
      wspl(l)=wtll(i,j,l)
          enddo
!
          do  l=1,lm
      pq(l)=(pint(i,j,l)+pint(i,j,l+1))*0.5
          enddo
!
          if(pint(i,j,lm+1).le.pld0)    then
      lold=lmll
          else
      lold=lmll+1
!
      pslh(lold)=pint(i,j,lm+1)
!
      tld2=tspl(lmll-2)
      tld1=tspl(lmll-1)
      tld0=tspl(lmll  )
!
      d1=(tld0-tld1)/(pld0-pld1)
      d2=(d1-(tld1-tld2)/(pld1-pld2))/(pld0-pld2)
      d2=0.
      x=pslh(lold)-pld0
!
      tspl(lold)=d2*x*x+d1*x+tld0
!
      qld2=qspl(lmll-2)
      qld1=qspl(lmll-1)
      qld0=qspl(lmll  )
!
      d1=(qld0-qld1)/(pld0-pld1)
      d2=(d1-(qld1-qld2)/(pld1-pld2))/(pld0-pld2)
      d2=0.
      x=pslh(lold)-pld0
!
      qspl(lold)=d2*x*x+d1*x+qld0
!
      wld2=wspl(lmll-2)
      wld1=wspl(lmll-1)
      wld0=wspl(lmll  )
!
      d1=(wld0-wld1)/(pld0-pld1)
      d2=(d1-(wld1-wld2)/(pld1-pld2))/(pld0-pld2)
      d2=0.
      x=pslh(lold)-pld0
!
      wspl(lold)=d2*x*x+d1*x+wld0
          endif
!
          do l=1,lold
      y2(l)=0.
          enddo
!
      call spline(lold,pslh,tspl,y2,lm,pq,thyb,pp,qq)
!
          do l=1,lold
      y2(l)=0.
          enddo
!
      call spline(lold,pslh,qspl,y2,lm,pq,qhyb,pp,qq)

!      if(i.eq.101.and.j.eq.301) then
!        print*,qspl
!        print*,qhyb
!      endif
!
          do l=1,lold
      y2(l)=0.
          enddo
!
      call spline(lold,pslh,wspl,y2,lm,pq,whyb,pp,qq)
!
!      if(i.eq.101.and.j.eq.301) then
!        print*,wspl
!        print*,whyb
!      endif
!
          do l=1,lm
!
      t(i,j,l)=thyb(l)
!
      qsgp=qhyb(l)
      wsgp=whyb(l)
!
!      if(qsgp.gt.0.1.and.l.eq.1) then
!        print*,'q ',i,j,l,qsgp
!        stop
!      endif
!
      tl=t(i,j,l)
      qsat=pq0c*2./(pint(i,j,l)+pint(i,j,l+1))
     &    *exp(a2*(tl-a3)/(tl-a4))
!
      qsgp=min(qsgp,qsat)
      qsgp=max(qsgp,epsq)
!
      wsgp=min(wsgp,qsgp)
      wsgp=max(wsgp,epsq)
!
      q(i,j,l)=qsgp
      w(i,j,l)=wsgp
!zj      t(i,j,l)=tl/(qsgp*0.608+1.-wsgp)
!zj      t(i,j,l)=tl/(qsgp*0.608+1.)
!
          enddo
!----------------------------------------------------------------------
          endif
!----------------------------------------------------------------------
!
              enddo
                  enddo
!
!----------------------------------------------------------------------
!
      do l=1,lm
        umx=-99999999.
        umn=99999999.
        vmx=-99999999.
        vmn=99999999.
        tmx=-99999999.
        tmn=99999999.
        qmx=-99999999.
        qmn=99999999.
        do j=1,jmi
          do i=1,imi
            if(maskv(i,j)) then
              umx=max(u(i,j,l),umx)
              umn=min(u(i,j,l),umn)
              vmx=max(v(i,j,l),vmx)
              vmn=min(v(i,j,l),vmn)
            endif
            if(maskh(i,j)) then
              tmx=max(t(i,j,l),tmx)
              tmn=min(t(i,j,l),tmn)
              qmx=max(q(i,j,l),qmx)
              qmn=min(q(i,j,l),qmn)
            endif
          enddo
        enddo
        write(*,*)'l=',l,'umx=',umx,' umn=',umn,' vmx=',vmx,' vmn=',vmn
        write(*,*)'l=',l,'tmx=',tmx,' tmn=',tmn,' qmx=',qmx,' qmn=',qmn
      enddo
!
!-----------------------------------------------------------------------
!
                      if(.not.global) then
!
!-----------------------------------------------------------------------
!***  lateral boundaries
!***  derived geometrical constants
!-----------------------------------------------------------------------
!
      sb=sbd*dtr                                                        
      dlm=dlmd*dtr                                                      
      dph=dphd*dtr
!
!-----------------------------------------------------------------------
!--------------derived horizontal grid constants------------------------
!-----------------------------------------------------------------------
!
      dyv=a*dph
!
      tph=sb-dph                                                                       
!
          do j=1,jmi
      tph=tph+dph
      tpv=tph+dph*0.5                                               
      dxv(j)=a*dlm*cos(tpv)
          enddo
!
      do kkk=1,0
      print*,'kkk=',kkk
!
      spf=0.
      srf=0.
!-------------southern boundary----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,1)+pd(i,2)+pd(i+1,1)+pd(i+1,2))*0.25
!
          do l=1,lm
      da=dxv(1)*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      pfyp=v(i,1,l)*da
      spf=spf+pfyp
      srf=srf+da
          enddo
              enddo
!-------------northern boundary----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,jmi-1)+pd(i,jmi)+pd(i+1,jmi-1)+pd(i+1,jmi))*0.25
!
          do l=1,lm
      da=dxv(jmi-1)*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      pfyp=v(i,jmi-1,l)*da
      spf=spf-pfyp
      srf=srf+da
          enddo
              enddo
!-------------western boundary-----------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(1,j)+pd(2,j)+pd(1,j+1)+pd(2,j+1))*0.25
!
          do l=1,lm
      da=dyv*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      pfxp=u(1,j,l)*da
      spf=spf+pfxp
      srf=srf+da
          enddo
                  enddo
!-------------eastern boundary-----------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(imi-1,j)+pd(imi,j)+pd(imi-1,j+1)+pd(imi,j+1))*0.5
!
          do l=1,lm
      da=dyv*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      pfxp=u(imi-1,j,l)*da
      spf=spf-pfxp
      srf=srf+da
          enddo
                  enddo
!----------------------------------------------------------------------
      print*,srf,(2*(imi-1)+2*(jmi-1))*lm
      print*,'------spf=',spf
!----------------------------------------------------------------------
      dspf=spf/srf
!-------------southern boundary----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,1)+pd(i,2)+pd(i+1,1)+pd(i+1,2))*0.25
          do l=1,lm
      v(i,1,l)=v(i,1,l)-dspf
          enddo
              enddo
!-------------northern boundary----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,jmi-1)+pd(i,jmi)+pd(i+1,jmi-1)+pd(i+1,jmi))*0.25
          do l=1,lm
      v(i,jmi-1,l)=v(i,jmi-1,l)+dspf
          enddo
              enddo
!-------------western boundary------------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(1,j)+pd(2,j)+pd(1,j+1)+pd(2,j+1))*0.25
          do l=1,lm
      u(1,j,l)=u(1,j,l)-dspf
          enddo
                  enddo
!-------------eastern boundary------------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(imi-1,j)+pd(imi,j)+pd(imi-1,j+1)+pd(imi,j+1))*0.5
          do l=1,lm
      u(imi-1,j,l)=u(imi-1,j,l)+dspf
          enddo
                  enddo
!-----------------------------------------------------------------------
      spf=0.
      nspf=0
!-------------southern boundary-----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,1)+pd(i,2)+pd(i+1,1)+pd(i+1,2))*0.25
!
          do l=1,lm
      pfyp=v(i,1,l)*dxv(1)*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      spf=spf+pfyp
      nspf=nspf+1
          enddo
              enddo
!-------------northern boundary-----------------------------------------
              do i=1,imi-1
          pdxy=(pd(i,jmi-1)+pd(i,jmi)+pd(i+1,jmi-1)+pd(i+1,jmi))*0.25
!
          do l=1,lm
      pfyp=v(i,jmi-1,l)*dxv(jmi-1)*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      spf=spf-pfyp
      nspf=nspf+1
          enddo
              enddo
!-------------western boundary------------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(1,j)+pd(2,j)+pd(1,j+1)+pd(2,j+1))*0.25
!
          do l=1,lm
      pfxp=u(1,j,l)*dyv*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      spf=spf+pfxp
      nspf=nspf+1
          enddo
                  enddo
!-------------eastern boundary------------------------------------------
                  do j=1,jmi-1
          pdxy=(pd(imi-1,j)+pd(imi,j)+pd(imi-1,j+1)+pd(imi,j+1))*0.5
!
          do l=1,lm
      pfxp=u(imi-1,j,l)*dyv*(dsg1(l)*pdtop+dsg2(l)*pdxy)
      spf=spf-pfxp
      nspf=nspf+1
          enddo
                  enddo
!-----------------------------------------------------------------------
      print*,'------spf=',spf
             enddo
!-------------southern boundary-----------------------------------------
          amdvel=0.
              do i=1,imi-1
          sdph=0.
          spfp=0.
          spfh=0.
          pdxy=(pd(i,1)+pd(i,2)+pd(i+1,1)+pd(i+1,2))*0.25
!
          do ld=1,lmll-1
          if((psl(ld)+psl(ld+1))*0.5.lt.pdxy+pt) then
      spfp=(vtll(i,1,ld)+vtll(i,1,ld+1))
     &    *(psl(ld+1)-psl(ld))*0.5+spfp
          endif
          enddo
!
          do l=1,lm
      dphl=(dsg1(l)*pdtop+dsg2(l)*pdxy)
      sdph=dphl+sdph
      spfh=v(i,1,l)*dphl+spfh
          enddo
!
      dvel=(spfp-spfh)/sdph
      amdvel=max(abs(dvel),amdvel)
!
          do l=1,lm
      v(i,1,l)=v(i,1,l)+dvel
          enddo
              enddo
      print*,'south max dvel=',amdvel
!-------------northern boundary----------------------------------------
          amdvel=0.
              do i=1,imi-1
          sdph=0.
          spfp=0.
          spfh=0.
          pdxy=(pd(i,jmi-1)+pd(i,jmi)+pd(i+1,jmi-1)+pd(i+1,jmi))*0.25
!
          do ld=1,lmll-1
          if((psl(ld)+psl(ld+1))*0.5.lt.pdxy+pt) then
      spfp=(vtll(i,jmi-1,ld)+vtll(i,jmi-1,ld+1))
     &    *(psl(ld+1)-psl(ld))*0.5+spfp
          endif
          enddo
!
          do l=1,lm
      dphl=(dsg1(l)*pdtop+dsg2(l)*pdxy)
      sdph=dphl+sdph
      spfh=v(i,jmi-1,l)*dphl+spfh
          enddo
!
      dvel=(spfp-spfh)/sdph
      amdvel=max(abs(dvel),amdvel)
!
          do l=1,lm
      v(i,jmi-1,l)=v(i,jmi-1,l)+dvel
          enddo
              enddo
      print*,'north max dvel=',amdvel
!-------------western boundary-----------------------------------------
          amdvel=0.
              do j=1,jmi-1
          sdph=0.
          spfp=0.
          spfh=0.
          pdxy=(pd(1,j)+pd(2,j)+pd(1,j+1)+pd(2,j+1))*0.25
!
          do ld=1,lmll-1
          if((psl(ld)+psl(ld+1))*0.5.lt.pdxy+pt) then
      spfp=(utll(1,j,ld)+utll(1,j,ld+1))
     &    *(psl(ld+1)-psl(ld))*0.5+spfp
          endif
          enddo
!
          do l=1,lm
      dphl=(dsg1(l)*pdtop+dsg2(l)*pdxy)
      sdph=dphl+sdph
      spfh=u(1,j,l)*dphl+spfh
          enddo
!
      dvel=(spfp-spfh)/sdph
      amdvel=max(abs(dvel),amdvel)
!
          do l=1,lm
      u(1,j,l)=u(1,j,l)+dvel
          enddo
              enddo
      print*,'west max dvel=',amdvel
!-------------eastern boundary-----------------------------------------
          amdvel=0.
              do j=1,jmi-1
          sdph=0.
          spfp=0.
          spfh=0.
          pdxy=(pd(imi-1,j)+pd(imi,j)+pd(imi-1,j+1)+pd(imi,j+1))*0.25
!
          do ld=1,lmll-1
          if((psl(ld)+psl(ld+1))*0.5.lt.pdxy+pt) then
      spfp=(utll(imi-1,j,ld)+utll(imi-1,j,ld+1))
     &    *(psl(ld+1)-psl(ld))*0.5+spfp
          endif
          enddo
!
          do l=1,lm
      dphl=(dsg1(l)*pdtop+dsg2(l)*pdxy)
      sdph=dphl+sdph
      spfh=u(imi-1,j,l)*dphl+spfh
          enddo
!
      dvel=(spfp-spfh)/sdph
      amdvel=max(abs(dvel),amdvel)
!
          do l=1,lm
      u(imi-1,j,l)=u(imi-1,j,l)+dvel
          enddo
              enddo
      print*,'east max dvel=',amdvel
!-------------separation of boundary values at h points-----------------
!-------------southern boundary-----------------------------------------
                  do j=1,lnsh
              jb=j
              do i=1,imi
          ib=i
      pdbs(ib,jb,1)=pd(i,j)
          do l=1,lm
      tbs(ib,jb,l,1)=t(i,j,l)
      qbs(ib,jb,l,1)=q(i,j,l)
      wbs(ib,jb,l,1)=w(i,j,l)
          enddo
              enddo
                  enddo
!-------------northern boundary-----------------------------------------
                  do j=jmi-lnsh+1,jmi
              jb=j-jmi+lnsh
              do i=1,imi
          ib=i
      pdbn(ib,jb,1)=pd(i,j)
          do l=1,lm
      tbn(ib,jb,l,1)=t(i,j,l)
      qbn(ib,jb,l,1)=q(i,j,l)
      wbn(ib,jb,l,1)=w(i,j,l)
          enddo
              enddo
                  enddo
!-------------western boundary------------------------------------------
                  do j=1,jmi
              jb=j
              do i=1,lnsh
          ib=i
      pdbw(ib,jb,1)=pd(i,j)
          do l=1,lm
      tbw(ib,jb,l,1)=t(i,j,l)
      qbw(ib,jb,l,1)=q(i,j,l)
      wbw(ib,jb,l,1)=w(i,j,l)
          enddo
              enddo
                  enddo
!-------------eastern boundary------------------------------------------
                  do j=1,jmi
              jb=j
              do i=imi-lnsh+1,imi
          ib=i-imi+lnsh
      pdbe(ib,jb,1)=pd(i,j)
          do l=1,lm
      tbe(ib,jb,l,1)=t(i,j,l)
      qbe(ib,jb,l,1)=q(i,j,l)
      wbe(ib,jb,l,1)=w(i,j,l)
          enddo
              enddo
                  enddo
!-------------separation of boundary values at v points-----------------
!-------------southern boundary-----------------------------------------
                  do j=1,lnsv
              jb=j
              do i=1,imi-1
          ib=i
          do l=1,lm
      ubs(ib,jb,l,1)=u(i,j,l)
      vbs(ib,jb,l,1)=v(i,j,l)
          enddo
              enddo
                  enddo
!-------------northern boundary-----------------------------------------
                  do j=jmi-lnsv,jmi-1
              jb=j-jmi+lnsv+1
              do i=1,imi-1
          ib=i
          do l=1,lm
      ubn(ib,jb,l,1)=u(i,j,l)
      vbn(ib,jb,l,1)=v(i,j,l)
          enddo
              enddo
                  enddo
!-------------western boundary------------------------------------------
                  do j=1,jmi-1
              jb=j
              do i=1,lnsv
          ib=i
          do l=1,lm
      ubw(ib,jb,l,1)=u(i,j,l)
      vbw(ib,jb,l,1)=v(i,j,l)
          enddo
              enddo
                  enddo
!-------------eastern boundary------------------------------------------
                  do j=1,jmi-1
              jb=j
              do i=imi-lnsv,imi-1
          ib=i-imi+lnsv+1
          do l=1,lm
      ube(ib,jb,l,1)=u(i,j,l)
      vbe(ib,jb,l,1)=v(i,j,l)
          enddo
              enddo
                  enddo
!-----------------------------------------------------------------------
!
                      endif ! .not.global
!
!-----------------------------------------------------------------------
!***  initial data
!-----------------------------------------------------------------------
!
      if (ihr.eq.0) then
!
!-----------------------------------------------------------------------
!
        nfcst=18
        write(fname,'(a)')'../output/fcst'
        open(unit=nfcst,file=fname
     &      ,status='unknown',form='unformatted')
        run=.true.
        ntsd=0
!
!-----------------------------------------------------------------------
!
        if(global) ihrend=9999
        write (nfcst) run,idat,ihrst,ihrend,ntsd
!OJORBA3        write (nfcst) pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
        write (nfcst) pt,pdtop,lpt2,sgm,sg1,dsg1,sgml1,sg2,dsg2,sgml2
        i_par_sta=1
        j_par_sta=1
        write (nfcst) i_par_sta,j_par_sta
        write (nfcst) dlmd,dphd,wbd,sbd,tlm0d,tph0d
        write (nfcst) im,jm,lm,lnsh
!OJORBA3
!
!-----------------------------------------------------------------------
!
        if(global) then
!
          call padh2(fis ,padded2)
          write(nfcst) padded2
          call padh2(stdh,padded2)
          write(nfcst) padded2
          call padh2(sm  ,padded2)
          write(nfcst) padded2
          call padh2(pd  ,padded2)
          write(nfcst) padded2
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=u(i,j,l)
              enddo
            enddo
            call padw2(field,padded2)
            write(nfcst) padded2
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=v(i,j,l)
              enddo
            enddo
            call padw2(field,padded2)
            write(nfcst) padded2
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=t(i,j,l)
              enddo
            enddo
            call padh2(field,padded2)
            write(nfcst) padded2
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=q(i,j,l)
              enddo
            enddo
            call padh2(field,padded2)
            write(nfcst) padded2
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=w(i,j,l)
              enddo
            enddo
            call padh2(field,padded2)
            write(nfcst) padded2
          enddo
!OJORBA3
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=0. !o3(i,j,l)
              enddo
            enddo
            call padh2(field,padded2)
            write(nfcst) padded2
          enddo
!OJORBA3
!--end of dynamical part, beginning of physics part---------------------
          open(unit=2,file='../output/albedo'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          write(nfcst) padded2  ! ALBASE TEMPORAL!!!
!!!CPEREZ1
          open(unit=2,file='../output/albedorrtmcorr'
     &        ,status='old',form='unformatted')
          do k=1,6
            read (2) field
            call padh2(field,padded2)
            write(nfcst) padded2
          enddo
          close(2)
!!!CPEREZ1
!
          open(unit=2,file='../output/emissivity'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/snowalbedo'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/tskin'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/sst'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/snow'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/snowheight'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/cice'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/deeptemperature'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/canopywater'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/frozenprecratio'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/ustar'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/z0corr'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/z0basecorr'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
!OJORBA3          open(unit=2,file='../output/stdh'
!OJORBA3     &        ,status='old',form='unformatted')
!OJORBA3          read (2) field
!OJORBA3          call padh2(field,padded2)
!OJORBA3          write(nfcst) padded2
!OJORBA3          close(2)
!
          open(unit=2,file='../output/stmp'
     &        ,status='old',form='unformatted')
          read (2) fieldn
          call padhn(fieldn,paddedn,nwets)
          write(nfcst) paddedn
          close(2)
!
          open(unit=2,file='../output/smst'
     &        ,status='old',form='unformatted')
          read (2) fieldn
          call padhn(fieldn,paddedn,nwets)
          write(nfcst) paddedn
          close(2)
!
          open(unit=2,file='../output/sh2o'
     &        ,status='old',form='unformatted')
          read (2) fieldn
          call padhn(fieldn,paddedn,nwets)
          write(nfcst) paddedn
          close(2)
!
          open(unit=2,file='../output/topsoiltypecorr'
     &        ,status='old',form='unformatted')
          read (2) ifield
          call padih2(ifield,ipadded2)
          write(nfcst) ipadded2
          close(2)
!
          open(unit=2,file='../output/landusecorr'
     &        ,status='old',form='unformatted')
          read (2) ifield
          call padih2(ifield,ipadded2)
          write(nfcst) ipadded2
          close(2)
!
!!!CPEREZ1
          open(unit=2,file='../output/landusenewcorr'
     &        ,status='old',form='unformatted')
          read (2) ifield
          call padih2(ifield,ipadded2)
          write(nfcst) ipadded2
          close(2)
!!!CPEREZ1
!
          open(unit=2,file='../output/vegfraccorr'
     &        ,status='old',form='unformatted')
          read (2) field
          call padh2(field,padded2)
          write(nfcst) padded2
          close(2)
!
          open(unit=2,file='../output/dzsoil'
     &        ,status='old',form='unformatted')
          read (2    ) dzsoil
          write(nfcst) dzsoil
          close(2)
!
          open(unit=2,file='../output/dzsoil'
     &        ,status='old',form='unformatted')
          read (2    ) dzsoil
          write(nfcst) dzsoil
          close(2)
!
!OJORBA3          write(nfcst) pt
!
          print*,'*** p2hyb wrote global initial data to fcst'
!
!-----------------------------------------------------------------------
        else ! regional initial data
!-----------------------------------------------------------------------
!
          write(nfcst) fis
          write(nfcst) stdh 
          write(nfcst) sm
          write(nfcst) pd
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=u(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=v(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=t(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=q(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=w(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
!OJORBA3 TEMPORAL!!! INPUT O3 CNST.
          do l=1,lm
            do j=1,jmi
              do i=1,imi
                field(i,j)=0. !o3(i,j,l)
              enddo
            enddo
            write(nfcst) field
          enddo
!OJORBA3
!
!-----------------------------------------------------------------------
!--end of dynamical part, beginning of physics part---------------------
!-----------------------------------------------------------------------
!
          open(unit=2,file='../output/albedo'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field 
          close(2)
!OJORBA3
          write(nfcst) field ! ALBASE TEMPORAL
!OJORBA3
!
!OJORBA3!!!CPEREZ1
          open(unit=2,file='../output/albedorrtmcorr'
     &        ,status='old',form='unformatted')
          do k=1,6
          read (2) field
          write(nfcst) field
          enddo
          close(2)
!OJORBA3!!!CPEREZ1
!
          open(unit=2,file='../output/emissivity'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/snowalbedo'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/tskin'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/sst'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/snow'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/snowheight'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/cice'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/deeptemperature'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/canopywater'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/frozenprecratio'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/ustar'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/z0corr'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/z0basecorr'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
!OJORBA3          open(unit=2,file='../output/stdh'
!OJORBA3     &        ,status='old',form='unformatted')
!OJORBA3          read (2    ) field
!OJORBA3          write(nfcst) field
!OJORBA3          close(2)
!
          open(unit=2,file='../output/stmp'
     &        ,status='old',form='unformatted')
          read (2    ) fieldn
          write(nfcst) fieldn
          close(2)
!
          open(unit=2,file='../output/smst'
     &        ,status='old',form='unformatted')
          read (2    ) fieldn
          write(nfcst) fieldn
          close(2)
!
          open(unit=2,file='../output/sh2o'
     &        ,status='old',form='unformatted')
          read (2    ) fieldn
          write(nfcst) fieldn
          close(2)
!
          open(unit=2,file='../output/topsoiltypecorr'
     &        ,status='old',form='unformatted')
          read (2    ) ifield
          write(nfcst) ifield
          close(2)
!
          open(unit=2,file='../output/landusecorr'
     &        ,status='old',form='unformatted')
          read (2    ) ifield
          write(nfcst) ifield
          close(2)
!
!OJORBA3!!!CPEREZ1
          open(unit=2,file='../output/landusenewcorr'
     &        ,status='old',form='unformatted')
          read (2    ) ifield
          write(nfcst) ifield
          close(2)
!OJORBA3!!!CPEREZ1
!
          open(unit=2,file='../output/vegfraccorr'
     &        ,status='old',form='unformatted')
          read (2    ) field
          write(nfcst) field
          close(2)
!
          open(unit=2,file='../output/dzsoil'
     &        ,status='old',form='unformatted')
          read (2    ) dzsoil
          write(nfcst) dzsoil
          close(2)
!
          open(unit=2,file='../output/dzsoil'
     &        ,status='old',form='unformatted')
          read (2    ) dzsoil
          write(nfcst) dzsoil
          close(2)
!
!OJORBA3          write(nfcst) pt
!
          print*,'*** p2hyb wrote regional initial data to fcst'
!
!-----------------------------------------------------------------------
        endif
!-----------------------------------------------------------------------
!
        close(unit=nfcst)
!
!-----------------------------------------------------------------------
      endif
!-----------------------------------------------------------------------
!
      return                                                            
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine spline(nold,xold,yold,y2,nnew,xnew,ynew,p,q)
!
!     ******************************************************************
!     *                                                                *
!     *  this is a one-dimensional cubic spline fitting routine        *
!     *  programed for a small scalar machine.                         *
!     *                                                                *
!     *  programer: z. janjic, yugoslav fed. hydromet. inst., beograd  *
!     *                                                                *
!     *                                                                *
!     *                                                                *
!     *  nold - number of given values of the function.  must be ge 3. *
!     *  xold - locations of the points at which the values of the     *
!     *         function are given.  must be in ascending order.       *
!     *  yold - the given values of the function at the points xold.   *
!     *  y2   - the second derivatives at the points xold.  if natural *
!     *         spline is fitted y2(1)=0. and y2(nold)=0. must be      *
!     *         specified.                                             *
!     *  nnew - number of values of the function to be calculated.     *
!     *  xnew - locations of the points at which the values of the     *
!     *         function are calculated.  xnew(k) must be ge xold(1)   *
!     *         and le xold(nold).                                     *
!     *  ynew - the values of the function to be calculated.           *
!     *  p, q - auxiliary vectors of the length nold-2.                *
!     *                                                                *
!     ******************************************************************
!
      dimension xold(nold),yold(nold),y2(nold),p(nold),q(nold)
      dimension xnew(nnew),ynew(nnew)
!-----------------------------------------------------------------------
      noldm1=nold-1
!
      dxl=xold(2)-xold(1)
      dxr=xold(3)-xold(2)
      dydxl=(yold(2)-yold(1))/dxl
      dydxr=(yold(3)-yold(2))/dxr
      rtdxc=.5/(dxl+dxr)
!
      p(1)= rtdxc*(6.*(dydxr-dydxl)-dxl*y2(1))
      q(1)=-rtdxc*dxr
!
      if(nold.eq.3) go to 700
!-----------------------------------------------------------------------
      k=3
!
 100  dxl=dxr
      dydxl=dydxr
      dxr=xold(k+1)-xold(k)
      dydxr=(yold(k+1)-yold(k))/dxr
      dxc=dxl+dxr
      den=1./(dxl*q(k-2)+dxc+dxc)
!
      p(k-1)= den*(6.*(dydxr-dydxl)-dxl*p(k-2))
      q(k-1)=-den*dxr
!
      k=k+1
      if(k.lt.nold) go to 100
!-----------------------------------------------------------------------
 700  k=noldm1
!
 200  y2(k)=p(k-1)+q(k-1)*y2(k+1)
!
      k=k-1
      if(k.gt.1) go to 200
!-----------------------------------------------------------------------
      k1=1
!
 300  xk=xnew(k1)
!
      do 400 k2=2,nold
      if(xold(k2).le.xk) go to 400
      kold=k2-1
      go to 450
 400  continue
      ynew(k1)=yold(nold)
      go to 600
!
 450  if(k1.eq.1)   go to 500
      if(k.eq.kold) go to 550
!
 500  k=kold
!
      y2k=y2(k)
      y2kp1=y2(k+1)
      dx=xold(k+1)-xold(k)
      rdx=1./dx
!
      ak=.1666667*rdx*(y2kp1-y2k)
      bk=.5*y2k
      ck=rdx*(yold(k+1)-yold(k))-.1666667*dx*(y2kp1+y2k+y2k)
!
 550  x=xk-xold(k)
      xsq=x*x
!
      ynew(k1)=ak*xsq*x+bk*xsq+ck*x+yold(k)
!
 600  k1=k1+1
      if(k1.le.nnew) go to 300
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
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
!-----------------------------------------------------------------------
!
      parameter(dtr=3.1415926535897932384626433832795/180.)
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
      relm=atan2(anum,denom)
      almd=relm/dtr+tlm0d
!
      if(almd.gt. 180.)    almd=almd-360.
      if(almd.lt.-180.)    almd=almd+360.
!
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine bilinb(cob,inb,jnb,imll,jmll,imi,jmi,ww,wfb)
!
!-----------------------------------------------------------------------
!
      dimension cob(3,imi,jmi)
      integer inb(4,imi,jmi),jnb(4,imi,jmi)
      dimension ww(imll,jmll),wfb(imi,jmi)
!
!-----------------------------------------------------------------------
              do j=1,jmi
          do i=1,imi
!
      i00=inb(1,i,j)
      i10=inb(2,i,j)
      i01=inb(3,i,j)
      i11=inb(4,i,j)
!
      j00=jnb(1,i,j)
      j10=jnb(2,i,j)
      j01=jnb(3,i,j)
      j11=jnb(4,i,j)
!
      p=cob(1,i,j)
      q=cob(2,i,j)
      pq=cob(3,i,j)
!
      z=ww(i00,j00)
     & +p*(ww(i10,j10)-ww(i00,j00))
     & +q*(ww(i01,j01)-ww(i00,j00))
     & +pq*(ww(i00,j00)-ww(i10,j10)-ww(i01,j01)+ww(i11,j11))
!
      wfb(i,j)=z
!
          enddo
              enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine padh2(h2,ph2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(imi,jmi),ph2(im,jm)
!
!-------------averaging at polar points---------------------------------
      rim=1./(imi-1)
      h2s=0.
      h2n=0.
      do i=1,imi-1
        h2s=h2(i,1)+h2s
        h2n=h2(i,jmi)+h2n
      enddo
      h2s=h2s*rim
      h2n=h2n*rim
      do i=1,imi
        h2(i,1  )=h2s
        h2(i,jmi)=h2n
      enddo
!-------------padding---------------------------------------------------
      do j=1,jmi
        do i=1,imi
          ph2(i+1,j+1)=h2(i,j)
        enddo
      enddo
!
      do i=2,im-1
        ph2(i,1 )=ph2(i,3   )
        ph2(i,jm)=ph2(i,jm-2)
      enddo
!
      do j=1,jm
        ph2(1   ,j)=ph2(im-2,j)
        ave=(ph2(2,j)+ph2(im-1,j))*0.5
        ph2(2   ,j)=ave
        ph2(im-1,j)=ave
        ph2(im  ,j)=ph2(3   ,j)
      enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine padih2(ih2,iih2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension ih2(imi,jmi),iih2(im,jm)
!
!-------------averaging at polar points---------------------------------
      rim=1./(imi-1)
      ih2s=0.
      ih2n=0.
      do i=1,imi-1
        ih2s=ih2(i,1  )+ih2s
        ih2n=ih2(i,jmi)+ih2n
      enddo
      ih2s=ih2s*rim
      ih2n=ih2n*rim
      do i=1,imi
        ih2(i,1  )=ih2s
        ih2(i,jmi)=ih2n
      enddo
!-------------padding---------------------------------------------------
      do j=1,jmi
        do i=1,imi
          iih2(i+1,j+1)=ih2(i,j)
        enddo
      enddo
!
      do i=2,im-1
        iih2(i,1 )=iih2(i,3   )
        iih2(i,jm)=iih2(i,jm-2)
      enddo
!
      do j=1,jm
        iih2(1   ,j)=iih2(im-2,j)
        iave=(iih2(2,j)+iih2(im-1,j))*0.5
        iih2(2   ,j)=iave
        iih2(im-1,j)=iave
        iih2(im  ,j)=iih2(3   ,j)
      enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine padhn(h2,ph2,km)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(km,imi,jmi),ph2(km,im,jm)
!
!-------------averaging at polar points---------------------------------
      rim=1./(imi-1)
      do l=1,km
        h2s=0.
        h2n=0.
        do i=1,imi-1
          h2s=h2(l,i,1)+h2s
          h2n=h2(l,i,jmi)+h2n
        enddo
        h2s=h2s*rim
        h2n=h2n*rim
        do i=1,imi
          h2(l,i,1)=h2s
          h2(l,i,jmi)=h2n
        enddo
      enddo
!-------------padding---------------------------------------------------
      do l=1,km
!
        do j=1,jmi
          do i=1,imi
            ph2(l,i+1,j+1)=h2(l,i,j)
          enddo
        enddo          
!
        do i=2,im-1
          ph2(l,i,1 )=ph2(l,i,3   )
          ph2(l,i,jm)=ph2(l,i,jm-2)
        enddo
!
        do j=1,jm
          ph2(l,1   ,j)=ph2(l,im-2,j)
          ave=(ph2(l,2,j)+ph2(l,im-1,j))*0.5
          ph2(l,2   ,j)=ave
          ph2(l,im-1,j)=ave
          ph2(l,im  ,j)=ph2(l,3   ,j)
        enddo
!
      enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine padw2(w2,pw2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension w2(imi,jmi),pw2(im,jm)
!
!-------------padding---------------------------------------------------
      do j=1,jmi
        do i=1,imi
          pw2(i+1,j+1)=w2(i,j)
        enddo
      enddo
!
      do i=2,im-1
        pw2(i,1   )=-pw2(i,2   )
        pw2(i,jm-1)=-pw2(i,jm-2)
        pw2(i,jm  )=-pw2(i,jm-2)
      enddo
!
      do j=1,jm
        pw2(1   ,j)=pw2(im-2,j)
        pw2(im-1,j)=pw2(2   ,j)
        pw2(im  ,j)=pw2(3   ,j)
      enddo
!-----------------------------------------------------------------------
!
      return
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine fix(cic,smt)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension cic(im,jm),smt(im,jm)
!-----------------------------------------------------------------------
      parameter(kount=30)
!
!-----------------------------------------------------------------------
!--remove small islands, small lakes and waterfalls---------------------
!-----------------------------------------------------------------------
!
      do k=1,kount
!
!-----------------------------------------------------------------------
        do j=2,jm-1
          do i=2,im-1
            sms=smt(i-1,j-1)+smt(i  ,j-1)+smt(i+1,j-1)
     &         +smt(i-1,j  )+smt(i+1,j  )
     &         +smt(i-1,j+1)+smt(i  ,j+1)+smt(i+1,j+1)
            if(smt(i,j).lt.0.5.and.sms.gt.7.5) then
              smt(i,j)=1.
              cic(i,j)=0.
            endif
            if(smt(i,j).gt.0.5.and.sms.lt.0.5) then
              smt(i,j)=0.
              cic(i,j)=1.
            endif
          enddo
        enddo
        if(im.gt.imi) then
          call swaph2(cic)
          call swaph2(smt)
          call poleh2(cic)
          call poleh2(smt)
        endif
!
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine croph2(ph2,h2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(imi,jmi),ph2(im,jm)
!
!-----------------------------------------------------------------------
!
      do j=1,jmi
        do i=1,imi
          h2(i,j)=ph2(i+1,j+1)
        enddo
      enddo
!
!-----------------------------------------------------------------------

      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine swaph2(h2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(im,jm)
!
!-----------------------------------------------------------------------
      do j=1,jm
        h2(1,j)=h2(im-2,j)
        ave=(h2(2,j)+h2(im-1,j))*0.5
        h2(2,j)=ave
        h2(im-1,j)=ave
        h2(im,j)=h2(3,j)
      enddo
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!-----------------------------------------------------------------------
!
      subroutine poleh2(h2)
!
!-----------------------------------------------------------------------
      include 'lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(im,jm)
!
!-----------------------------------------------------------------------
      do i=1,im
        h2(i,1)=h2(i,3)
        h2(i,jm)=h2(i,jm-2)
      enddo
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
