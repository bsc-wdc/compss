!
!-----------------------------------------------------------------------
!
      program dust_start
!
!-----------------------------------------------------------------------
!
      include 'include/lmimjm.inc'
      include 'include/llgrid.inc'
!
!-----------------------------------------------------------------------
!
      logical run, global
      character*128 infile,fname
      integer idat(3),ihrst
     &,sizebins             !!number of dust transport bins
     &,soiltypes            !!number of soil types
     &,soilbins             !!number of soil grains categories
     &,sourcemodes          !!number of source modes
     &,vegtypes             !!number of landuse (world olson ecosystems) types
!!!CPEREZ2
     &,vegtypes2            !!number of landuse (USGS... NMMb) types  NEW!!!!!

      parameter (sizebins=8,soilbins=4,sourcemodes=3,soiltypes=16
     &          ,vegtypes=99
!!!CPEREZ2
     &          ,vegtypes2=30)

      real
     & m
     &,dum

      real
     & snow(imi,jmi)
     &,landusefrac(imi,jmi)
     &,wprim(imi,jmi)
     &,clayfrac(imi,jmi)
     &,siltfrac(imi,jmi)
     &,sandfrac(imi,jmi)
     &,snow_integer(imi,jmi)
     &,vegfrac(imi,jmi)
     &,erodfrac(imi,jmi)
     &,height(imi,jmi)
     &,seamask(imi,jmi)
!!!CPEREZ4
     &,source(imi,jmi)
!!!CPEREZ4
     &,pref(imi,jmi)
     &,mij(sourcemodes,sizebins)
     &,mn(sizebins)
     &,alpha(imi,jmi)
     &,alpha_soil(soilbins)
     &,bare_soil_fr(imi,jmi)
     &,pradi(sizebins)
     &,pradi_max(sizebins)
     &,pradi_min(sizebins)
     &,pdens(sizebins)
     &,soildens(soilbins)
     &,soil_diam(soilbins)
     &,soil_mf(imi,jmi,soilbins)
     &,soil_sa(imi,jmi,soilbins)
     &,soil_sa_tot(imi,jmi)
     &,source_diam(sourcemodes)
     &,source_sigma(sourcemodes)
     &,source_mf(sourcemodes)
     &,gama(sizebins)
     &,ustrtrk(soilbins)
     &,aa(sizebins)
     &,wpr(soiltypes)
     &,bcly(soiltypes)
     &,bslt(soiltypes)
     &,bsnd(soiltypes)
     &,t_bcly(soiltypes)
     &,t_bslt(soiltypes)
     &,t_mfsnd(soiltypes)
     &,t_csnd(soiltypes)
     &,REYNTH(soilbins)
     &,delta(imi,jmi,sizebins)
     &,gpss(sizebins)
     &,ustrtr(imi,jmi,soilbins)
     &,prueba(imi,jmi)

      integer
     & topsoiltypecorr(imi,jmi)
     &,landusecorr(imi,jmi)
     &,landusenewcorr(imi,jmi)
     &,landusedust(imi,jmi)
     &,kount_landusenew(vegtypes,imi,jmi)
!!!CPEREZ2
     &,kount_landuse(vegtypes2,imi,jmi)
     &,kount_total(imi,jmi)
     &,kount_dust(imi,jmi)
     &,n053,s026,s030,e135,e142,n010,s015,w030
      real
     & padded2(im,jm),ipadded2(im,jm),paddedn(nwets,im,jm)
     &,paddedn2(im,jm,sizebins)
!
!!! KARSTEN
     &,cdm(vegtypes),cvm(vegtypes),asmm(vegtypes),rsqcd0vm(vegtypes)
     &,qext550(sizebins),z0vegusgs(vegtypes)
!
      real
     & z0corr(imi,jmi),roughness(imi,jmi),z0new(imi,jmi)
!
      parameter (sbdtmp1=sbd+60.
     &,          sbdtmp2=sbd+58.,sbdtmp3=2.*sbd
     &,          wbdtmp1=wbd+90.,wbdtmp3=2.*wbd)
!
!      parameter (n060=jmi-jmi*sbdtmp1/sbdtmp3-tph0d*dphd                  ! model grid point at 60N
!     &,          n058=jmi-jmi*sbdtmp2/sbdtmp3-tph0d*dphd                  ! model grid point at 58N
!     &,          e090=imi-imi*wbdtmp1/wbdtmp3-tlm0d*dlmd)                 ! model grid point at 90E
!
!!!CPEREZ2
!Australia dust sources
!      parameter (sbdtmp26=sbd-26.
!     &,          sbdtmp2=sbd+58.,sbdtmp3=2.*sbd
!     &,          wbdtmp1=wbd+90.,wbdtmp3=2.*wbd)
!

!      parameter (s26=jmi-jmi*sbdtmp1/sbdtmp3-tph0d*dphd                  ! model grid point at 26N
!     &,          s30=jmi-jmi*sbdtmp2/sbdtmp3-tph0d*dphd                  ! model grid point at 30S
!     &,          e135=imi-imi*wbdtmp1/wbdtmp3-tlm0d*dlmd                 ! model grid point at 135E
!     &,          e142=imi-imi*wbdtmp1/wbdtmp3-tlm0d*dlmd)                 ! model grid point at 142E

      parameter (dlm5=5/dlmd,dph5=5/dphd)
!!! KARSTEN
!
!----------------------------------------------------------------------
!!! WPR is calculated from Fecan et al 1999 as 0.0014(%clay)^2+0.17(%clay)
!----------------------------------------------------------------------
!
      data wpr /0.52,1.07,1.84,2.44,0.88,3.51,5.61,7.40,7.40,9.61,11.08
     &,14.57,0.00,0.00,0.00,0.00/
!
!----------------------------------------------------------------------
!!! OLD  FRACTIONS  (NOT USED)
!----------------------------------------------------------------------
!
!      data bcly /0.03,0.06,0.10,0.13,0.05,0.18,0.27,0.34,0.34,0.42,0.47
!     &,0.58,0.00,0.00,0.00,0.00/
!      data bslt /0.05,0.12,0.32,0.70,0.85,0.39,0.15,0.56,0.34,0.06,0.47
!     &,0.20,0.00,0.00,0.00,0.00/
!      data bsnd /0.92,0.82,0.58,0.17,0.10,0.43,0.58,0.10,0.32,0.52,0.06
!     &,0.22,0.00,0.00,0.00,0.00/
!
!----------------------------------------------------------------------
!!! NEW TEGEN ET AL 2002
!----------------------------------------------------------------------
!
      data t_bcly /0.03,0.00,0.10,0.13,0.05,0.18,0.27,0.34,0.34
     &,0.42,0.47,0.58,0.00,0.00,0.00,0.00/
      data t_bslt /0.05,0.18,0.32,0.70,0.85,0.39,0.15,0.56,0.34
     &,0.06,0.47,0.20,0.00,0.00,0.00,0.00/
      data t_mfsnd /0.46,0.41,0.26,0.17,0.10,0.43,0.29,0.10,0.32
     &,0.52,0.06,0.22,0.00,0.00,0.00,0.00/
      data t_csnd /0.46,0.41,0.26,0.00,0.00,0.00,0.29,0.00,0.00
     &,0.00,0.00,0.00,0.00,0.00,0.00,0.00/
!
!----------------------------------------------------------------------         
!!! 8 DUST TRANSPORT SIZE BINS
!----------------------------------------------------------------------
!
      data pradi /0.15,0.25,0.47,0.80,1.36,2.29,3.93,7.24/ !dust volume radius (microns)
      data pradi_max /0.18,0.3,0.6,1.,1.8,3.,6.,10./       !max radius of dust transport bin (microns)
      data pradi_min /0.1,0.18,0.3,0.6,1.,1.8,3.,6./      !min radius of dust transport bin (microns)
      data pdens /2.50,2.50,2.50,2.50,2.65,2.65,2.65,2.65/ !dust density (g/cm3)
!
!----------------------------------------------------------------------
!!! 4 SOIL SIZE CATEGORIES
!----------------------------------------------------------------------

      data soil_diam /2.E-6,15.E-6,160.E-6,710.E-6/   !soil grains mean diameter (m)
      data soildens /2500.,2650.,2650.,2650./            !soil grains density (kg/m3)
!
!----------------------------------------------------------------------
!!! TRI-MODAL SIZE DISTRIBUTION IN SOURCE REGIONS (D'ALMEIDA 1987) FROM ZENDER 2003
!----------------------------------------------------------------------
!
      data source_diam /0.832,4.82,19.38/            !mass mean diameter (microns)
      data source_sigma /2.10,1.9,1.6/               !geometric standard deviation
      data source_mf /0.036,0.957,0.007/             !mass fraction
!!!
      data adens /0.00122/
      data G /9.80/
      data gama /0.08,0.08,0.08,0.08,1.,1.,1.,1./
      data aa  /1.,0.9,0.8,0.8,0.7,0.6,0.5,0.4/
      data qext550 /1.373,3.303,3.245,2.413,2.262,2.260,2.162,2.108/
!
!!! KARSTEN
      data cdm /.35,.35,.40,.50,.50,.50,.35,0.0,.35,.30                     !<-- local drag coefficient for vegetation
     &         ,0.0,0.0,0.0,0.0,0.0,.40,.40,.50,.50,.50
     &         ,.40,.40,.50,.50,.50,.50,.50,.50,.50,.30
     &         ,.30,.50,.50,.50,.30,.35,.30,.30,.30,.35
     &         ,.35,.35,.35,.35,.35,.40,.40,.50,0.0,0.0
     &         ,0.0,0.0,.30,.50,.50,.50,.50,.30,.40,.50
     &         ,.50,.40,.35,.40,.35,.35,.35,.35,.35,0.0
     &         ,0.0,.50,0.0,0.0,0.0,.30,.40,.50,0.0,.35
     &         ,0.0,0.0,0.0,0.0,.35,0.0,.35,0.0,.50,.50
     &         ,.35,.30,.35,.40,.50,.50,0.0,0.0,0.0/
      data cvm /.09,.09,.12,.16,.16,.16,.09,0.0,.09,.08                     !<-- local viscous drag coefficient for vegetation
     &         ,0.0,0.0,0.0,0.0,0.0,.12,.12,.16,.16,.16
     &         ,.12,.12,.16,.16,.16,.16,.16,.16,.16,.08
     &         ,.08,.16,.16,.16,.08,.09,.08,.08,.08,.09
     &         ,.09,.09,.09,.09,.09,.12,.12,.16,0.0,0.0
     &         ,0.0,0.0,.09,.16,.16,.16,.16,.09,.12,.16
     &         ,.16,.12,.09,.12,.09,.09,.09,.09,.09,0.0
     &         ,0.0,.16,0.0,0.0,0.0,.08,.12,.16,0.0,.09
     &         ,0.0,0.0,0.0,0.0,.09,0.0,.09,0.0,.16,.16
     &         ,.09,.08,.09,.12,.16,.16,0.0,0.0,0.0/
      data rsqcd0vm /3.0,3.0,2.5,2.0,2.0,2.0,3.0,0.0,3.0,3.5                !<-- root squared surface drag coefficient
     &              ,0.0,0.0,0.0,0.0,0.0,2.5,2.5,2.0,2.0,2.0
     &              ,2.5,2.5,2.0,2.0,2.0,2.0,2.0,2.0,2.0,3.5
     &              ,3.5,2.0,2.0,2.0,3.5,3.0,3.5,3.5,3.5,3.0
     &              ,3.0,3.0,3.0,3.0,3.0,2.5,2.5,2.0,0.0,0.0
     &              ,0.0,0.0,3.0,2.0,2.0,2.0,2.0,3.0,2.5,2.0
     &              ,2.0,2.5,3.0,2.5,3.0,3.0,3.0,3.0,3.0,0.0
     &              ,0.0,2.0,0.0,0.0,0.0,3.5,2.5,2.0,0.0,3.0
     &              ,0.0,0.0,0.0,0.0,3.0,0.0,3.0,0.0,2.0,2.0
     &              ,3.0,3.5,3.0,2.5,2.0,2.0,0.0,0.0,0.0/
      data asmm /3.2E-5,3.2E-5,0.0,0.0,0.0,0.0,3.2E-5,0.0,3.2E-5,1.4E-5     !<-- ratio between areas of small ...
     &          ,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0                    !<-- collectors and roughness elements
     &          ,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.4E-5
     &          ,1.4E-5,0.0,0.0,0.0,1.4E-5,3.2E-5,1.4E-5,1.4E-5,1.4E-5
     &          ,3.2E-5
     &          ,3.2E-5,3.2E-5,3.2E-5,3.2E-5,3.2E-5,0.0,0.0,0.0,0.0,0.0
     &          ,0.0,0.0,3.2E-5,0.0,0.0,0.0,0.0,3.2E-5,0.0,0.0
     &          ,0.0,0.0,3.2E-5,0.0,3.2E-5,3.2E-5,3.2E-5,3.2E-5,3.2E-5
     &          ,0.0
     &          ,0.0,0.0,0.0,0.0,0.0,1.4E-5,0.0,0.0,0.0,3.2E-5
     &          ,0.0,0.0,0.0,0.0,3.2E-5,0.0,3.2E-5,0.0,0.0,0.0
     &          ,3.2E-5,1.4E-5,3.2E-5,0.0,0.0,0.0,0.0,0.0,0.0/
!
!      data z0vegtbl / 1.000,0.070,0.070,0.070,0.070,0.150                  !<-- NMMB SSiB model vegetation roughness
!     &               ,0.080,0.030,0.050,0.860,0.800,0.850
!     &               ,2.650,1.090,0.800,0.001,0.040,0.050
!     &               ,0.010,0.040,0.060,0.050,0.030,0.001
!     &               ,0.010,0.150,0.010,0.000,0.000,0.000/
!
      data z0vegusgs /1.000,0.080,1.090,0.850,0.800,2.650,0.050,0.010       !<-- converted OLSEN vegetation roughness
     &               ,0.040,0.080,0.010,0.001,0.050,0.001,0.001,0.030
     &               ,0.030,0.800,0.800,2.650,1.090,1.090,0.800,0.800
     &               ,0.800,0.800,1.090,0.800,2.650,0.070,0.070,0.150
     &               ,2.650,0.150,0.070,0.070,0.070,0.070,0.070,0.050
     &               ,0.070,0.080,0.860,0.050,0.040,0.030,0.030,0.150
     &               ,0.150,0.010,0.010,0.010,0.010,0.050,0.150,0.150
     &               ,0.150,0.860,0.030,0.050,0.050,0.050,0.060,0.030
     &               ,0.040,0.040,0.040,0.040,0.010,0.150,0.010,0.050
     &               ,0.010,0.010,0.010,0.150,1.090,0.800,0.050,0.010
     &               ,0.010,0.010,0.010,0.010,0.010,0.150,0.050,0.030
     &               ,0.030,2.650,0.860,0.150,0.070,0.050,0.000,0.000
     &               ,0.000,0.000,0.000/
!!! KARSTEN
!
      data
     & infile/'
     &       '/
     &,fname /'
     &       '/

      character*256 param1, param2, param3, param4, param5, param6
      character*256 param7, param8, param9, param10, param11, param12
      character*256 param13, param14

      call getarg(1,param1)
      call getarg(2,param2)
      call getarg(3,param3)
      call getarg(4,param4)
      call getarg(5,param5)
      call getarg(6,param6)
      call getarg(7,param7)
      call getarg(8,param8)
      call getarg(9,param9)
      call getarg(10,param10)
      call getarg(11,param11)
      call getarg(12,param12)
      call getarg(13,param13)
      call getarg(14,param14)

!
!-----------------------------------------------------------------------
!!! reading of data
!----------------------------------------------------------------------
!
!!! KARSTEN
        print*,' '
        print*,'grid exclusions:',n058,n060,e090
        print*,' '
!!! KARSTEN
!
        infile = param1
        open(unit=1,file=infile,status='old',form='unformatted')
        read(1) run,idat,ihrst
        close(1)

        global=im.gt.imi
!
        nfcst=18
        open(unit=nfcst,file=param2
     &      ,status='unknown',form='unformatted')
        run=.true.
        write (nfcst) run,idat,ihrst
     
        print*, run,idat,ihrst
      
          open(unit=2,file=param3
     &        ,status='old',form='unformatted')
          read (2) snow
          close(2)
!
          open(unit=2,file=param4
     &        ,status='old',form='unformatted')
          read (2) topsoiltypecorr
          close(2)
!
          open(unit=2,file=param5
     &        ,status='old',form='unformatted')
          read (2) landusecorr
          close(2)

          open(unit=2,file=param6
     &        ,status='old',form='unformatted')
          read (2) landusenewcorr                               !dominant landusenew type
!          print*, landusenewcorr(100,100)
          close(2)

          open(unit=2,file=param7
     &        ,status='old',form='unformatted')
          read (2) kount_landuse                                !kount of each landusenew type
          close(2)

          open(unit=2,file=param8
     &        ,status='old',form='unformatted')
          read (2) kount_landusenew                             !kount of each landusenew type
          close(2)

          open(unit=2,file=param9
     &        ,status='old',form='unformatted')
          read (2) vegfrac
          close(2)

          open(unit=2,file=param10
     &        ,status='old',form='unformatted')
          read (2) height
          close(2)

          open(unit=2,file=param11
     &        ,status='old',form='unformatted')
          read (2) seamask
          close(2)
!
          open(unit=2,file=param12
     &        ,status='old',form='unformatted')
          read (2) source
          close(2)

!!! KARSTEN
          open(unit=2,file=param13
     &        ,status='old',form='unformatted')
          read (2) z0corr
          close(2)
!
          open(unit=2,file=param14
     &        ,status='old',form='unformatted')
          read (2) roughness
          close(2)
!!! KARSTEN
!
!-----------------------------------------------------------------------
!!! CALCULATION OF USTAR THRESHOLD DRY FOR SOIL GRAINS FROM MARTICORENA
!-----------------------------------------------------------------------
!
      densair=1.23
!
      do k=1,soilbins

        REYNTH(k)= 0.38+1331.*(100.*soil_diam(k))**1.56
        print*, REYNTH(k)
!
        IF(REYNTH(k).LE.10.)THEN
!
!          ustrtrk(k)=((((0.1666681*denspart(k)*9.81*D_SOIL(k))
!     &    /(-1+1.928*REYNTH(k)**0.0922))*(1+(6E-7/(denspart(k)
!     &    *9.81*D_SOIL(k)**2.5))))**0.5)*densair**(-0.5)
!
          ustrtrk(k)=(1.+(6.E-7)/(soildens(k)*9.81*soil_diam(k)**2.5))
     &    **0.5*0.1291*(1.928*REYNTH(k)**0.0922-1.)**(-0.5)
     &    *sqrt(soildens(k)*9.81*soil_diam(k)/densair)
!
        ELSEIF(REYNTH(k).GT.10.)THEN
!
!          ustrtrk(k)=((0.0144*denspart(k)*9.81*D_SOIL(k)
!     &    *(1-0.0858*EXP(-0.0617*(REYNTH(k)-10)))*(1+(6E-7/
!     &    (denspart(k)*9.81*D_SOIL(k)**2.5))))**0.5)*densair**(-0.5)
!
          ustrtrk(k)=(1.+(6.E-7)/(soildens(k)*9.81*soil_diam(k)**2.5))
     &    **0.5*0.120*(1.-0.0858*EXP(-0.0617*(REYNTH(k)-10.)))
     &    *sqrt(soildens(k)*9.81*soil_diam(k)/densair)
!
        ENDIF
!
      enddo
!
      do j=1,jmi
        do i=1,imi
          do k=1,soilbins
          ustrtr(i,j,k)=ustrtrk(k)
          enddo
        enddo
      enddo
!
!----------------------------------------------------------------------
!!! DUST PRODUCING CATEGORIES FROM USGS 94-cat LANDUSE TYPES 
!!! CALCULATION OF BARE SOIL FRACTION
!----------------------------------------------------------------------
!
           if(global) then
             n053=(90.+53.)/dphd
             n010=(90.+10.)/dphd
             s015=(90.-10.)/dphd
             s026=(90.-26.)/dphd
             s030=(90.-30.)/dphd
             e135=(180.+135.)/dlmd
             e142=(180.+142.)/dlmd
             w030=(180.-30.)/dlmd
           endif


        do j=1,jmi
          do i=1,imi
            kount_dust(i,j)=0
            kount_total(i,j)=0
!
            do n=1,vegtypes2
              kount_total(i,j)=kount_total(i,j)+kount_landuse(n,i,j)     !total number of kounts in each cell
            enddo

              kount_dust(i,j)=kount_landuse(8,i,j) +                     !Kounts of dust producing categories
     &                        kount_landuse(19,i,j)

            if(global) then
             if(i.ge.e135.and.i.le.e142.and.j.ge.s030.and.
     &          j.le.s026) then
              kount_dust(i,j)=kount_dust(i,j)+kount_landuse(7,i,j)
             endif
!
             if(j.gt.n053) then
              kount_dust(i,j)=0
             endif
!           
            endif
!
            bare_soil_fr(i,j)=real(kount_dust(i,j))
     &                       /real(kount_total(i,j))              !!BARE SOIL FRACTION
!
          enddo
        enddo
!
        write(*,*) ' '
        write(*,*) 'bare_soil_fr'
          do j=1,jmi,40
          write(*,7777) (bare_soil_fr(i,jmi+1-j)*100,i=1,imi,40)
          enddo
        write(*,*) ' '
        write(*,*) 'kount_dust'
          do j=1,jmi,40
          write(*,5555) (kount_dust(i,jmi+1-j),i=1,imi,40)
          enddo
        write(*,*) ' '
        write(*,*) 'kount_total'
          do j=1,jmi,40
          write(*,5555) (kount_total(i,jmi+1-j),i=1,imi,40)
          enddo
        write(*,*) ' '
        write(*,*) 'kount_dust/kount_total'
          do j=1,jmi,40
          write(*,5555) (kount_dust(i,jmi+1-j)/kount_total(i,jmi+1-j)
     &                   ,i=1,imi,40)
          enddo

5555  format(' ',20I6)
!
!----------------------------------------------------------------------
!!! Desert Mask (lansusefrac) and erodfrac
!----------------------------------------------------------------------
!
          do j=1,jmi
            do i=1,imi

             landusefrac(i,j)=0.0
             landusedust(i,j)=landusecorr(i,j)
             erodfrac(i,j)=1.0-vegfrac(i,j) 
!

             if(landusedust(i,j).eq.19) then
               landusefrac(i,j)=1.0           !barren or spaersely vegetated
             endif
             if(landusedust(i,j).eq.8) then
               landusefrac(i,j)=0.2           !shrubland
             endif
!
             if(global) then
              if(i.ge.e135.and.i.le.e142.and.j.ge.s030.and.
     &           j.le.s026.and.landusedust(i,j).eq.7) then
                landusefrac(i,j)=0.8          !grassland is a source in the lake Eyre (Australia)
              endif
!
              if(j.le.n010.and.j.ge.s015.and.i.le.w030.
     &           and.landusedust(i,j).eq.8) then
                landusefrac(i,j)=0.0  !exclude shrubland from here
              endif
!
              if(j.gt.n053) then
               landusefrac(i,j)=0.0
              endif
!
             endif !global

           enddo
          enddo
          
8888  format(' ',20I2)
        write(*,*) ' '
        write(*,*) 'landusedust'
          do j=1,jmi,40
        write(*,8888) (landusedust(i,jmi+1-j),i=1,imi,40)
          enddo
!
7777  format(' ',20f5.2)
        write(*,*) ' '
        write(*,*) 'landusefrac'
          do j=1,jmi,40
        write(*,7777) (landusefrac(i,jmi+1-j)*100,i=1,imi,40)
          enddo
        write(*,*) ' '
        write(*,*) 'seamask'
          do j=1,jmi,40
        write(*,7777) (seamask(i,jmi+1-j),i=1,imi,40)
          enddo
!
9999  format(' ',20f10.2)
        write(*,*) ' '
        write(*,*) 'height'
          do j=1,jmi,40
        write(*,9999) (height(i,jmi+1-j),i=1,imi,40)
          enddo
!
!----------------------------------------------------------------------
!!! PREFERENTIAL SOURCES form Ginoux 2001
!!! TAKES 5 DEGREES OF GLOBAL GRID AROUND ONE SPOT INTO ACCOUNT
!----------------------------------------------------------------------
!
          do j=1,jmi
            do i=1,imi
            pref(i,j)=0.
            enddo
          enddo
!
!!! KARSTEN
!          do j=20,jmi-20
!            do i=20,imi-20
          do j=3+dph5,jmi-2-dph5
            do i=3+dlm5,imi-2-dlm5
!!! KARSTEN
            if (seamask(i,j).eq.0.)then
!
            hmax=height(i,j)
            hmin=height(i,j)
!!! KARSTEN
!              do k=j-15,j+15
!              do l=i-11,i+11
              do k=j-dph5,j+dph5
                do l=i-dlm5,i+dlm5
!!! KARSTEN
                  if (seamask(l,k).eq.0.)then
                  hmax=max(hmax,height(l,k))
                  hmin=min(hmin,height(l,k))
                  endif
                enddo
              enddo
!            pref(i,j)=landusefrac(i,j)*erodfrac(i,j)
             pref(i,j)=((hmax-height(i,j))/(hmax-hmin))**2
!     &                *landusefrac(i,j)*bare_soil_fr(i,j)*erodfrac(i,j)
!            pref(i,j)=pref(i,j)*((hmax-height(i,j))/(hmax-hmin))**5
            endif
!
            enddo
          enddo
!
!!!CPEREZ4 Include directly the sources from ginoux (from read_paul_source.f90)
          do j=1,jmi
            do i=1,imi
            pref(i,j)=source(i,j)
            enddo
          enddo
!!!CPEREZ4

        write(*,*) ' '
        write(*,*) 'pref'
          do j=1,jmi,40
        write(*,9999) (pref(i,jmi+1-j),i=1,imi,40)
          enddo
!
!----------------------------------------------------------------------
!!! máximum amount of absorbed water
!----------------------------------------------------------------------
!
          do j=1,jmi
           do i=1,imi
            wprim(i,j)=0.0
            k=topsoiltypecorr(i,j)
            wprim(i,j)=wpr(k)
           enddo
          enddo
!
!----------------------------------------------------------------------
!!! MASS FRACTION OF EACH SOIL GRAIN BIN
!----------------------------------------------------------------------
!
           do j=1,jmi
           do i=1,imi
           ll=topsoiltypecorr(i,j)
            soil_mf(i,j,1)=t_bcly(ll)
            soil_mf(i,j,2)=t_bslt(ll)
            soil_mf(i,j,3)=t_mfsnd(ll)
            soil_mf(i,j,4)=t_csnd(ll)

           enddo
           enddo 
!
!----------------------------------------------------------------------
!!! RELATIVE SURFACE AREA OF EACH SOIL GRAIN BIN
!----------------------------------------------------------------------
!
           do j=1,jmi
           do i=1,imi
            soil_sa(i,j,1)=  3.*soil_mf(i,j,1)
     &                     /(2.*soildens(1)*soil_diam(1))

            soil_sa(i,j,2)=  3.*soil_mf(i,j,2)
     &                     /(2.*soildens(2)*soil_diam(2))

            soil_sa(i,j,3)=  3.*soil_mf(i,j,3)
     &                     /(2.*soildens(3)*soil_diam(3))

            soil_sa(i,j,4)=  3.*soil_mf(i,j,4)
     &                     /(2.*soildens(4)*soil_diam(4))

            soil_sa_tot(i,j)=soil_sa(i,j,1)+soil_sa(i,j,2)+
     &                      soil_sa(i,j,3)+soil_sa(i,j,4)

            if(soil_sa_tot(i,j).gt.0.)then
             do n=1,soilbins
             soil_sa(i,j,n)=soil_sa(i,j,n)/soil_sa_tot(i,j)
             enddo
            else
             do n=1,soilbins
             soil_sa(i,j,n)=0.
             enddo
            endif

           enddo
           enddo
!
!----------------------------------------------------------------------
!!! Vertical to Horizontal flux ratio alpha
!----------------------------------------------------------------------
!
           alpha_soil(1)=1.e-6  !1/cm    !CLAY (IF mclay<0.45)
           alpha_soil(2)=1.e-5  !1/cm    !SILT
           alpha_soil(3)=1.e-6  !1/cm    !FINE/MEDIUM SAND
           alpha_soil(4)=1.e-7  !1/cm    !COARSE SAND
!
           do j=1,jmi
           do i=1,imi
            alpha(i,j)=0.
             do n=1,soilbins
              dum=alpha_soil(n)
!
!              if (n==1.and.pref(i,j).gt.0.5) dum=alpha_soil(2)*pref(i,j)
!              if (n==2.and.pref(i,j).gt.0.) dum=alpha_soil(2)*pref(i,j)

              if (n==1.and.soil_mf(i,j,1).gt.0.45) dum=1.e-7   !CLAY (IF mclay>0.45)

!              alpha(i,j)=alpha(i,j)+dum*soil_sa(i,j,n)  !!! original ksn4
              alpha(i,j)=alpha(i,j)+dum*soil_mf(i,j,n)  !!new ksn4

             enddo
!             dum=1.e-5*pref(i,j)                          !If it is a preferential source we increase alpah scaling with pref
!             if(dum.gt.alpha(i,j)) alpha(i,j)=dum
           enddo
           enddo
!TEST alpha from Marticorena and Bergametti

!           do j=1,jmi
!           do i=1,imi
!           alpha(i,j)=0.

!           if(soil_mf(i,j,1).le.0.2)then
!           alpha(i,j)=0.01*exp(0.308*soil_mf(i,j,1)-13.82)
!           alpha(i,j)=100.*exp((13.4*soil_mf(i,j,1)-6.)*log(10.)) 
!           alpha(i,j)=10**(13.4*soil_mf(i,j,1)-6.)
!           endif
          
!           enddo
!           enddo

!
!----------------------------------------------------------------------
!!! Error function to distribute the emitted mass from source
!!! distribution to transport bins after Zender et al. (2003)
!----------------------------------------------------------------------
!
        do n=1,sizebins
         m=0.
         do i=1,sourcemodes
!
         mij(i,n)=erf(log(2.*pradi_max(n)/source_diam(i))/
     &               (sqrt(2.)*log(source_sigma(i))))
!
         mij(i,n)=mij(i,n)-
     &            erf(log(2.*pradi_min(n)/source_diam(i))/
     &               (sqrt(2.)*log(source_sigma(i))))
!
         mij(i,n)=mij(i,n)*0.5
!
         m=m+mij(i,n)*source_mf(i)
!
        enddo
!
         mn(n)=m                                      !Mass fraction in transport bin j
         print*,'n=',n,'m=',mn(n)
!
        enddo
!
        print*,'m=',mn(1)+mn(2)+mn(3)+mn(4)+mn(5)+mn(6)
     &             +mn(7)+mn(8)
!
!!! KARSTEN
!----------------------------------------------------------------------
!!! MODEL ROUGHNESS LENGTH NOW WITH THE UPDATED USGS DATASET WITH 96
!!! CATEGORIES (CONVERSION FROM 27 TYPES BY MEANS OF ROUGH GUESS !!!)
!----------------------------------------------------------------------
!!! PREPARATION OF NEW HIGHRES ROUGHNESS LENGTH WHICH IS A COMBINATION
!!! OF Z0CORR AND THE Z0 DATASET FOR AFRICA AND ASIA
!!! PROVIDED BY BENOIT LAURENT DERIVED FROM POLDER-I
!----------------------------------------------------------------------
!
      do j=1,jmi
        do i=1,imi
          z0new(i,j)=0.
          if (roughness(i,j).gt.0.000002) then
            z0new(i,j)=roughness(i,j)
          elseif (z0corr(i,j).eq.0.0013) then
            z0new(i,j)=z0corr(i,j)
          else
            z0new(i,j)=0.1+z0vegusgs(landusenewcorr(i,j))*vegfrac(i,j)        !compare z0vegustar.f90
          endif
        enddo
      enddo
!
7788  format(' ',20f6.2)
!
      write(*,*) ' '
      write(*,*) 'z0corr'
      do j=1,jmi,40
        write(*,7788) (z0corr(i,jmi+1-j),i=1,imi,40)
      enddo
!
      write(*,*) ' '
      write(*,*) 'roughness'
      do j=1,jmi,40
        write(*,7788) (roughness(i,jmi+1-j),i=1,imi,40)
      enddo
!
      write(*,*) ' '
      write(*,*) 'z0new'
      do j=1,jmi,40
        write(*,7788) (z0new(i,jmi+1-j),i=1,imi,40)
      enddo
!!! KARSTEN
!
!-----------------------------------------------------------------------
!!! WRITE FILE FOR NMMB
!-----------------------------------------------------------------------

        if (global) then                           ! global branch
!
!!! KARSTEN
          call padih2(landusenewcorr,ipadded2)     ! landusenewcorr
          write(nfcst) ipadded2
!!! KARSTEN
!
          call padh2(bare_soil_fr,padded2)         !bare soil fraction between 0 and 1
          write(nfcst) padded2
!
          call padh2(pref,padded2)                 !preferential sources between 0 and 1
          write(nfcst) padded2
!
          call padh2(landusefrac,padded2)          !0 or 1 Desert mask
!          call padh2(prueba,padded2)          !0 or 1 Desert mask
          write(nfcst) padded2
!
          call padh2(erodfrac,padded2)             !1 minus vegetation fraction between 0 and 1
          write(nfcst) padded2
!
          call padh2(vegfrac,padded2)              !vegetation fraction between 0 and 1
          write(nfcst) padded2
!
          call padhn2(soil_mf,paddedn2,soilbins)   !soil mass fractions
          write(nfcst) paddedn2
!
          call padhn2(soil_sa,paddedn2,soilbins)   !soil relative area fractions
          write(nfcst) paddedn2
!
          call padh2(alpha,padded2)                !vertical to horizontal flux ratio
          write(nfcst) padded2
! 
          call padh2(wprim,padded2)                !maximum amount of absorbed water in soil
          write(nfcst) padded2
!
          call padhn2(ustrtr,paddedn2,soilbins)    !threshold friction velocity
          write(nfcst) paddedn2
!!! KARSTEN
          call padh2(z0new,padded2)                !new hires rougness length for dust sources (africa/asia)
          write(nfcst) padded2
!
          call padh2(z0corr,padded2)               !model rougness length
          write(nfcst) padded2
!
          call padh2(roughness,padded2)            !POLDER-1 rougness length
          write(nfcst) padded2
!!! KARSTEN
          write(nfcst) mn                          !mass fraction of total mass in transport bin
!!! KARSTEN
          write(nfcst) aa
          write(nfcst) cdm
          write(nfcst) cvm
          write(nfcst) rsqcd0vm
          write(nfcst) asmm
          write(nfcst) qext550
!!! KARSTEN
!
          print*, ' '
          print*, 'Global branch done'
                     
        else                                               !regional branch
!
          write(nfcst) landusenewcorr
          write(nfcst) bare_soil_fr
          write(nfcst) pref 
          write(nfcst) landusefrac
          write(nfcst) erodfrac
          write(nfcst) vegfrac
          write(nfcst) soil_mf
          write(nfcst) soil_sa
          write(nfcst) alpha
          write(nfcst) wprim
          write(nfcst) ustrtr
!!! KARSTEN
          write(nfcst) z0new
          write(nfcst) z0corr
          write(nfcst) roughness
!!! KARSTEN
          write(nfcst) mn
!!! KARSTEN
          write(nfcst) aa
          write(nfcst) cdm
          write(nfcst) cvm
          write(nfcst) rsqcd0vm
          write(nfcst) asmm
          write(nfcst) qext550
!!! KARSTEN
!
          print*, ' '
          print*, 'Regional branch done'          

        endif
!
!-----------------------------------------------------------------------
!
        close(unit=nfcst)
!
!-----------------------------------------------------------------------
!
      end
!
!-----------------------------------------------------------------------
!#######################################################################
!-----------------------------------------------------------------------
!
      subroutine padhn(h2,ph2,km)
!
!-----------------------------------------------------------------------
      include 'include/lmimjm.inc'
!-----------------------------------------------------------------------
!
      dimension h2(km,imi,jmi),ph2(km,im,jm)
!
!-------------averaging at polar points---------------------------------
!
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
      return
      end
!
!!!CARLOS
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine padhn2(h2,ph2,km)
!-----------------------------------------------------------------------
      include 'include/lmimjm.inc'
!-----------------------------------------------------------------------
      dimension h2(imi,jmi,km),ph2(im,jm,km)
!-------------averaging at polar points---------------------------------
      rim=1./(imi-1)
      do l=1,km
        h2s=0.
        h2n=0.
        do i=1,imi-1
          h2s=h2(i,1,l)+h2s
          h2n=h2(i,jmi,l)+h2n
        enddo
        h2s=h2s*rim
        h2n=h2n*rim
        do i=1,imi
          h2(i,1,l)=h2s
          h2(i,jmi,l)=h2n
        enddo
      enddo
!-------------padding---------------------------------------------------
      do l=1,km
!
        do j=1,jmi
          do i=1,imi
            ph2(i+1,j+1,l)=h2(i,j,l)
          enddo
        enddo
!
        do i=2,im-1
          ph2(i,1,l )=ph2(i,3,l   )
          ph2(i,jm,l)=ph2(i,jm-2,l)
        enddo
!
        do j=1,jm
          ph2(1   ,j,l)=ph2(im-2,j,l)
          ave=(ph2(2,j,l)+ph2(im-1,j,l))*0.5
          ph2(2   ,j,l)=ave
          ph2(im-1,j,l)=ave
          ph2(im  ,j,l)=ph2(3   ,j,l)
        enddo
!
      enddo
      return
      end
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine padih2(ih2,iih2)
!-----------------------------------------------------------------------
      include 'include/lmimjm.inc'
!-----------------------------------------------------------------------
      dimension ih2(imi,jmi),iih2(im,jm)
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
      return
      end
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine padh2(h2,ph2)
!-----------------------------------------------------------------------
      include 'include/lmimjm.inc'
!-----------------------------------------------------------------------
      dimension h2(imi,jmi),ph2(im,jm)
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
      return
      end
