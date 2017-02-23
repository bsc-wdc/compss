program landusenew
!-----------------------------------------------------------------------
!     z. janjic, aug. 2007 (from an earlier program by d. jovic)
!     generates topography and ocean sea mask from usgs DEM 30'' data
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
integer(kind=4),parameter:: &
 idatamax=7200 &              ! max i index for data within a tile
,lclass=99

real(kind=4),parameter:: &
 dla=1./120. &               ! data resolution, latitudinal
,dlahf=dla/2. &              !
,dlo=1./120. &               ! data resolution, longitudinal
,dlohf=dlo/2. &              !
,pi=3.141592653589793238 &   !
,dtr=pi/180. &               !
,scalex=0.50 &               !
,scaley=0.50                 !


character(128):: &
 fname & 
,infile &
,outfile

character,dimension(7200):: &
 iht2

character(7),dimension(33):: &
 ctile

integer(kind=4):: &
 i,iaven,iaves,id,idat,ie,iht,is,ism &
,j,jd,je,js & 
,lrecl &
,n,nend,nlat,nlon &
,maxclass,maxkount,lsum,iavg,l

integer(kind=8):: &
 iaveh

logical(kind=4):: &
 majority

real(kind=4):: &
 aelo,anla,asla,awlo,ahtsum,almd,aphd,asmsum &
,ctph,ctph0 &
,dlm,dph,edlm,edph,elmh,elml,ephh,ephl &
,rctph,rdlm,rdph,rndbsum,sb,stph0,tlm,tlm0,tph,tph0,wb

!integer(kind=2),dimension(1:idatamax):: &
! iht2                        !

integer(kind=4),dimension(1:imi,1:jmi):: &
 ismsum &                    !
,ndbsum &
,landuse                      !

integer(kind=4),dimension(1:lclass,1:imi,1:jmi):: &
 kount 

integer(kind=8),dimension(1:imi,1:jmi):: &
 ihtsum                      !

real(kind=4),dimension(1:33):: &
 teb &                       !
,tnb &                       !
,tsb &                       !
,twb                         !

real(kind=4),dimension(1:imi,1:jmi):: &
 height &                    !
,seamask

integer num, length, stat
character*256 gtop_dir, output_landusenew, output_kountlandusenew

if (command_argument_count().gt.0) then
  call get_command_argument(1,gtop_dir,length,stat)
  call get_command_argument(2,output_landusenew,length,stat)
  call get_command_argument(3,output_kountlandusenew,length,stat)
end if


!-----------------------------------------------------------------------
      data fname &
      /'                                                              '/ 
      data &
       ctile( 1)/'W180N90'/,tsb( 1)/  40./,tnb( 1)/  90./ &
                           ,twb( 1)/-180./,teb( 1)/-140./ &
      ,ctile( 2)/'W140N90'/,tsb( 2)/  40./,tnb( 2)/  90./ &
                           ,twb( 2)/-140./,teb( 2)/-100./ &
      ,ctile( 3)/'W100N90'/,tsb( 3)/  40./,tnb( 3)/  90./ &
                           ,twb( 3)/-100./,teb( 3)/ -60./ &
      ,ctile( 4)/'W060N90'/,tsb( 4)/  40./,tnb( 4)/  90./ &
                           ,twb( 4)/ -60./,teb( 4)/ -20./ &
      ,ctile( 5)/'W020N90'/,tsb( 5)/  40./,tnb( 5)/  90./ &
                           ,twb( 5)/ -20./,teb( 5)/  20./ &
      ,ctile( 6)/'E020N90'/,tsb( 6)/  40./,tnb( 6)/  90./ &
                           ,twb( 6)/  20./,teb( 6)/  60./ &
      ,ctile( 7)/'E060N90'/,tsb( 7)/  40./,tnb( 7)/  90./ &
                           ,twb( 7)/  60./,teb( 7)/ 100./ &
      ,ctile( 8)/'E100N90'/,tsb( 8)/  40./,tnb( 8)/  90./ &
                           ,twb( 8)/ 100./,teb( 8)/ 140./ &
      ,ctile( 9)/'E140N90'/,tsb( 9)/  40./,tnb( 9)/  90./ &
                           ,twb( 9)/ 140./,teb( 9)/ 180./ &
      ,ctile(10)/'W180N40'/,tsb(10)/ -10./,tnb(10)/  40./ &
                           ,twb(10)/-180./,teb(10)/-140./ &
      ,ctile(11)/'W140N40'/,tsb(11)/ -10./,tnb(11)/  40./ &
                           ,twb(11)/-140./,teb(11)/-100./ &
      ,ctile(12)/'W100N40'/,tsb(12)/ -10./,tnb(12)/  40./ &
                           ,twb(12)/-100./,teb(12)/ -60./ &
      ,ctile(13)/'W060N40'/,tsb(13)/ -10./,tnb(13)/  40./ &
                           ,twb(13)/ -60./,teb(13)/ -20./ &
      ,ctile(14)/'W020N40'/,tsb(14)/ -10./,tnb(14)/  40./ &
                           ,twb(14)/ -20./,teb(14)/  20./ &
      ,ctile(15)/'E020N40'/,tsb(15)/ -10./,tnb(15)/  40./ &
                           ,twb(15)/  20./,teb(15)/  60./ &
      ,ctile(16)/'E060N40'/,tsb(16)/ -10./,tnb(16)/  40./ &
                           ,twb(16)/  60./,teb(16)/ 100./ &
      ,ctile(17)/'E100N40'/,tsb(17)/ -10./,tnb(17)/  40./ &
                           ,twb(17)/ 100./,teb(17)/ 140./ &
      ,ctile(18)/'E140N40'/,tsb(18)/ -10./,tnb(18)/  40./ &
                           ,twb(18)/ 140./,teb(18)/ 180./ &
      ,ctile(19)/'W180S10'/,tsb(19)/ -60./,tnb(19)/ -10./ &
                           ,twb(19)/-180./,teb(19)/-140./ &
      ,ctile(20)/'W140S10'/,tsb(20)/ -60./,tnb(20)/ -10./ &
                           ,twb(20)/-140./,teb(20)/-100./ &
      ,ctile(21)/'W100S10'/,tsb(21)/ -60./,tnb(21)/ -10./ &
                           ,twb(21)/-100./,teb(21)/ -60./ &
      ,ctile(22)/'W060S10'/,tsb(22)/ -60./,tnb(22)/ -10./ &
                           ,twb(22)/ -60./,teb(22)/ -20./ &
      ,ctile(23)/'W020S10'/,tsb(23)/ -60./,tnb(23)/ -10./ &
                           ,twb(23)/ -20./,teb(23)/  20./ &
      ,ctile(24)/'E020S10'/,tsb(24)/ -60./,tnb(24)/ -10./ &
                           ,twb(24)/  20./,teb(24)/  60./ &
      ,ctile(25)/'E060S10'/,tsb(25)/ -60./,tnb(25)/ -10./ &
                           ,twb(25)/  60./,teb(25)/ 100./ &
      ,ctile(26)/'E100S10'/,tsb(26)/ -60./,tnb(26)/ -10./ &
                           ,twb(26)/ 100./,teb(26)/ 140./ &
      ,ctile(27)/'E140S10'/,tsb(27)/ -60./,tnb(27)/ -10./ &
                           ,twb(27)/ 140./,teb(27)/ 180./ &
      ,ctile(28)/'W180S60'/,tsb(28)/ -90./,tnb(28)/ -60./ &
                           ,twb(28)/-180./,teb(28)/-120./ &
      ,ctile(29)/'W120S60'/,tsb(29)/ -90./,tnb(29)/ -60./ &
                           ,twb(29)/-120./,teb(29)/ -60./ &
      ,ctile(30)/'W060S60'/,tsb(30)/ -90./,tnb(30)/ -60./ &
                           ,twb(30)/ -60./,teb(30)/   0./ &
      ,ctile(31)/'W000S60'/,tsb(31)/ -90./,tnb(31)/ -60./ &
                           ,twb(31)/   0./,teb(31)/  60./ &
      ,ctile(32)/'E060S60'/,tsb(32)/ -90./,tnb(32)/ -60./ &
                           ,twb(32)/  60./,teb(32)/ 120./ &
      ,ctile(33)/'E120S60'/,tsb(33)/ -90./,tnb(33)/ -60./ &
                           ,twb(33)/ 120./,teb(33)/ 180./
!-----------------------------------------------------------------------
 1000 format(' Processing tile ',i2,' ',a128)
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
!----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi 
          landuse(i,j)=15 ! default
          do l=1,lclass
            kount(l,i,j)=0
          enddo
!          ihtsum(i,j)=0
!          ismsum(i,j)=0
!          ndbsum(i,j)=0
        enddo
      enddo
!----------------------------------------------------------------------
!
!!! KARSTEN
!      if(im.gt.imi) then
        nend=33
!      else
!        nend=18
!      endif
!
!!! KARSTEN
      print*, nend
!
!----------------------------------------------------------------------
!--process data tile by data tile--------------------------------------
!----------------------------------------------------------------------
!
      do n=1,nend
!
!----------------------------------------------------------------------
        fname = gtop_dir // ctile(n) // '.VEG'
!----------------------------------------------------------------------
        awlo=twb(n)
        aelo=teb(n)
        asla=tsb(n)
        anla=tnb(n)
        nlon=int((aelo-awlo)/dlo+0.5)
        nlat=int((anla-asla)/dla+0.5)
!        lrecl=nlon*2
        lrecl=nlon*2
        print*,nlon,nlat,lrecl
!----------------------------------------------------------------------
        if(n.lt.28) then 
        open(unit=1,file=fname &
            ,form='unformatted',access='direct',recl=nlon)
        write(*,1000) n,fname
        endif
!----------------------------------------------------------------------
        do jd=1,nlat ! outter loop within tile, latitudes
!----------------------------------------------------------------------
          if(n.lt.28) read(1,rec=jd) (iht2(id),id=1,nlon)
!----------------------------------------------------------------------
          do id=1,nlon  ! inner loop within tile, longitudes
!----------------------------------------------------------------------
            if(n.lt.28) then
            idat=ichar(iht2(id))
            else
            idat=15
            endif
!              print*, iht2(2000),idat
!             idat=iht2(id)
!            if(idat.eq.-9999) then
!              ism=1
!              iht=0
!            else
!              ism=0
!              iht=idat
!            endif
!
            almd=awlo+dlohf+(id-1)*dlo
            aphd=anla-dlahf-(jd-1)*dla
!---------------------------------------------------------------------
            if(im.gt.imi) then ! global domain
!---------------------------------------------------------------------
              tlm=almd*dtr
              tph=aphd*dtr
!
              ctph=abs(cos(tph))
              if(ctph.gt.0.00001) then
                rctph=dph/(ctph*dlm)
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
              is=int((elml-wb)*rdlm+0.5)+1
              ie=int((elmh-wb)*rdlm+0.5)+1
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
              js=int((ephl-sb)*rdph+0.5)+1
              je=int((ephh-sb)*rdph+0.5)+1
!
              if(js.lt.1  ) js=1
              if(je.lt.1  ) je=1
              if(js.gt.jmi) js=jmi
              if(je.gt.jmi) je=jmi
!

!
              do j=js,je
                if(is.gt.ie) then
                  do i=is,imi
!                    ihtsum(i,j)=ihtsum(i,j)+iht
!                    ismsum(i,j)=ismsum(i,j)+ism
!                    ndbsum(i,j)=ndbsum(i,j)+1
                     kount(idat,i,j)=kount(idat,i,j)+1
                  enddo
                  do i=1,ie
!                    ihtsum(i,j)=ihtsum(i,j)+iht
!                    ismsum(i,j)=ismsum(i,j)+ism
!                    ndbsum(i,j)=ndbsum(i,j)+1
                     kount(idat,i,j)=kount(idat,i,j)+1
                  enddo
                else
                  do i=is,ie
!                    ihtsum(i,j)=ihtsum(i,j)+iht
!                    ismsum(i,j)=ismsum(i,j)+ism
!                    ndbsum(i,j)=ndbsum(i,j)+1
                     kount(idat,i,j)=kount(idat,i,j)+1
                  enddo
                endif
              enddo
!---------------------------------------------------------------------
            else ! regional domain
!---------------------------------------------------------------------
              call tll(almd,aphd,tlm0d,dtr,ctph0,stph0,tlm,tph)
!
              ctph=cos(tph)
              rctph=dph/(ctph*dlm)
!
              edlm=dlm*rctph*scalex
              elml=tlm-edlm
              elmh=tlm+edlm
!
              is=int((elml-wb)*rdlm+0.5)+1
              ie=int((elmh-wb)*rdlm+0.5)+1
!
              edph=dph*scaley
              ephl=tph-edph
              ephh=tph+edph
!
              js=int((ephl-sb)*rdph+0.5)+1
              je=int((ephh-sb)*rdph+0.5)+1
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
!                    ihtsum(i,j)=ihtsum(i,j)+iht
!                    ismsum(i,j)=ismsum(i,j)+ism
!                    ndbsum(i,j)=ndbsum(i,j)+1
                     kount(idat,i,j)=kount(idat,i,j)+1
                  enddo
                enddo
              endif
!----------------------------------------------------------------------
            endif ! end of global/regional branching
!----------------------------------------------------------------------
          enddo  ! inner loop within tile, longitudes
!          print*, idat
!----------------------------------------------------------------------
        enddo ! outer loop within tile
!----------------------------------------------------------------------
        close(1)
      enddo  ! end of loop over tiles
!--all data tiles processed, now calculate height and sea mask---------
!      if(im.gt.imi.and.j.gt.jmi) then
!        do j=1,jmi
!          iaveh=ihtsum(1  ,j)+ihtsum(imi,j)
!          iaves=ismsum(1  ,j)+ismsum(imi,j)
!          iaven=ndbsum(1  ,j)+ndbsum(imi,j)
!!
!          ihtsum(1  ,j)=iaveh
!          ihtsum(imi,j)=iaveh
!          ismsum(1  ,j)=iaves
!          ismsum(imi,j)=iaves
!          ndbsum(1  ,j)=iaven
!          ndbsum(imi,j)=iaven
!        enddo
!      endif
!!
!      do j=1,jmi
!        do i=1,imi
!          if(ndbsum(i,j).eq.0) then
!            print*,'warning height ndbsum(',i,',',j,') =0'
!            asmsum=.5
!            ahtsum=0.
!          else
!            rndbsum=1./float(ndbsum(i,j))
!            ahtsum=ihtsum(i,j)*rndbsum
!            asmsum=ismsum(i,j)*rndbsum
!          endif
!!
!          if(asmsum.le..5) then
!            seamask(i,j)=0.
!            height (i,j)=ahtsum
!          else
!            seamask(i,j)=1.
!            height (i,j)=0.
!          endif
!!          if(height(i,j).lt.0.) print*,i,j,height(i,j)
!        enddo
!      enddo
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
!              print*,'carlos',landuse(i,j)
              majority=.true.
            endif
          enddo
!--if not, exclude non-land classes and look for ordinary majority------
          if(.not.majority) then
            kount(14,i,j)=0 ! usgs water
            kount(15,i,j)=0 ! usgs water
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
!            print*, 'carlos3', landuse(i,j)
          endif
!-----------------------------------------------------------------------
        enddo
      enddo

!Global Ecosystem Legend 

!          1 URBAN                                              
!          2 LOW SPARSE GRASSLAND                               
!          3 CONIFEROUS FOREST                                  
!          4 DECIDUOUS CONIFER FOREST                           
!          5 DECIDUOUS BROADLEAF FOREST                         
!          6 EVERGREEN BROADLEAF FORESTS                        
!          7 TALL GRASSES AND SHRUBS                            
!          8 BARE DESERT                                        
!          9 UPLAND TUNDRA                                      
!         10 IRRIGATED GRASSLAND                                
!         11 SEMI DESERT                                        
!         12 GLACIER ICE                                        
!         13 WOODED WET SWAMP                                   
!         14 INLAND WATER                                       
!         15 SEA WATER                                          
!         16 SHRUB EVERGREEN                                    
!         17 SHRUB DECIDUOUS                                    
!         18 MIXED FOREST AND FIELD                             
!         19 EVERGREEN FOREST AND FIELDS                        
!         20 COOL RAIN FOREST                                   
!         21 CONIFER BOREAL FOREST                              
!         22 COOL CONIFER FOREST                                
!         23 COOL MIXED FOREST                                  
!         24 MIXED FOREST                                       
!         25 COOL BROADLEAF FOREST                              
!         26 DECIDUOUS BROADLEAF FOREST                         
!         27 CONIFER FOREST                                     
!         28 MONTANE TROPICAL FORESTS                           
!         29 SEASONAL TROPICAL FOREST                           
!         30 COOL CROPS AND TOWNS                               
!         31 CROPS AND TOWN                                     
!         32 DRY TROPICAL WOODS                                 
!         33 TROPICAL RAINFOREST                                
!         34 TROPICAL DEGRADED FOREST                           
!         35 CORN AND BEANS CROPLAND                            
!         36 RICE PADDY AND FIELD                               
!         37 HOT IRRIGATED CROPLAND                             
!         38 COOL IRRIGATED CROPLAND                            
!         39 COLD IRRIGATED CROPLAND                            
!         40 COOL GRASSES AND SHRUBS                            
!         41 HOT AND MILD GRASSES AND SHRUBS                    
!         42 COLD GRASSLAND                                     
!         43 SAVANNA (WOODS)                                    
!         44 MIRE, BOG, FEN                                     
!         45 MARSH WETLAND                                      
!         46 MEDITERRANEAN SCRUB                                
!         47 DRY WOODY SCRUB                                    
!         48 DRY EVERGREEN WOODS                                
!         49 VOLCANIC ROCK                                      
!         50 SAND DESERT                                        
!         51 SEMI DESERT SHRUBS                                 
!         52 SEMI DESERT SAGE                                   
!         53 BARREN TUNDRA                                      
!         54 COOL SOUTHERN HEMISPHERE MIXED FORESTS             
!         55 COOL FIELDS AND WOODS                              
!         56 FOREST AND FIELD                                   
!         57 COOL FOREST AND FIELD                              
!         58 FIELDS AND WOODY SAVANNA                           
!         59 SUCCULENT AND THORN SCRUB                          
!         60 SMALL LEAF MIXED WOODS                             
!         61 DECIDUOUS AND MIXED BOREAL FOREST                  
!         62 NARROW CONIFERS                                    
!         63 WOODED TUNDRA                                      
!         64 HEATH SCRUB                                        
!         65 COASTAL WETLAND - NW                               
!         66 COASTAL WETLAND - NE                               
!         67 COASTAL WETLAND, SE                                
!         68 COASTAL WETLAND - SW                               
!         69 POLAR AND ALPINE DESERT                            
!         70 GLACIER ROCK                                       
!         71 SALT PLAYAS                                        
!         72 MANGROVE                                           
!         73 WATER AND ISLAND FRINGE                            
!         74 LAND, WATER, AND SHORE                             
!         75 LAND AND WATER, RIVERS                             
!         76 CROP AND WATER MIXTURES                            
!         77 SOUTHERN HEMISPHERE CONIFERS                       
!         78 SOUTHERN HEMISPHERE MIXED FOREST                   
!         79 WET SCLEROPHYLIC FOREST                            
!         80 COASTLINE FRINGE                                   
!         81 BEACHES AND DUNES                                  
!         82 SPARSE DUNES AND RIDGES                            
!         83 BARE COASTAL DUNES                                 
!         84 RESIDUAL DUNES AND BEACHES                         
!         85 COMPOUND COASTLINES                                
!         86 ROCKY CLIFFS AND SLOPES                            
!         87 SANDY GRASSLAND AND SHRUBS                         
!         88 BAMBOO                                             
!         89 MOIST EUCALYPTUS                                   
!         90 RAIN GREEN TROPICAL FOREST                         
!         91 WOODY SAVANNA                                      
!         92 BROADLEAF CROPS                                    
!         93 GRASS CROPS                                        
!         94 CROPS, GRASS, SHRUBS                               

      do j=jmi,1,-10
        write(*,1100) (landuse(i,j),i=1,imi,20)
      enddo
!-----------------------------------------------------------------------
      outfile = output_landusenew
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) landuse
      close(1)
      print*,'Landusenew file written to ../output/landusenew.'
      print*,'Enjoy your DUST landuse!'

      outfile = output_kountlandusenew
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) kount
      close(1)
      print*,'Landusenew file written to ../output/kount_landusenew.'
      print*,'Enjoy your kount landusenew!'


!-----------------------------------------------------------------------
!      print*,'Sea mask from DEM'
!      do j=jmi,1,-10
!        write(*,1100) (seamask(i,j),i=1,imi,35)
!      enddo
!-----------------------------------------------------------------------
!      outfile='../output/seamaskDEM'
!      open(unit=1,file=outfile,status='unknown' &
!          ,form='unformatted')
!      write(1) seamask
!      close(1)
!!
!      print*,'Height file written to ../output/seamaskDEM'
!      print*,'Enjoy your sea mask!'
!!-----------------------------------------------------------------------
!      print*,'Topography height from DEM'
!      do j=jmi,1,-10
!        write(*,1100) (height(i,j),i=1,imi,35)
!      enddo
!!-----------------------------------------------------------------------
!      outfile='../output/heightDEM'
!      open(unit=1,file=outfile,status='unknown' &
!          ,form='unformatted')
!      write(1) height
!      close(1)
!!
!      print*,'Height file written to ../output/heightDEM'
!      print*,'Enjoy your mountains!'
!-----------------------------------------------------------------------
endprogram landusenew
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

