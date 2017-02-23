!
      PROGRAM Make_tables
!
!-------------------------------------------------------------------------------
!--- Create microphysics lookup tables
!-------------------------------------------------------------------------------
!
!   INPUT ARGUMENT LIST:
!       NONE     
!  
!   OUTPUT ARGUMENT LIST: 
!     NONE
!     
!   OUTPUT FILES:
!     lookup.dat binary file
!     
! Subprograms & Functions called:
!   * Subroutine RAIN_LOOKUP  - one-time call for setting up rain lookup tables
!   * Subroutine ICE_LOOKUP   - one-time call for setting up ice lookup tables
!   * BLOCK DATA MY600        - sets up growth rates for nucleated ice crystals
!
!     UNIQUE: NONE
!  
!     LIBRARY: NONE
!  
!   COMMON BLOCKS: CTLBLK
!                  LOOPS
!                  MASKS
!                  PHYS
!                  VRBLS
!                  CLDWTR
!                  PVRBLS
!                  ACMCLH
!                  CMY600
!   
! ATTRIBUTES:
!   LANGUAGE: FORTRAN 90
!   MACHINE : IBM
!
!------------------------------------------------------------------------- 
!-------------- Parameters & arrays for lookup tables -------------------- 
!------------------------------------------------------------------------- 
!
!--- Key parameter from riming tables used to define a variable
!
      INTEGER, PARAMETER :: Nrime=40
!
!--- Key parameters from rain lookup tables.  
!
!--- Mean rain drop diameters vary from 50 microns (0.05 mm) to 450 microns 
!      (0.45 mm).  Exponential size distributions are assumed.  
!
      REAL, PARAMETER :: DMRmin=.05E-3, DMRmax=.45E-3, BsDMR=1.E-6,
     & XMRmin=1.E6*DMRmin, XMRmax=1.E6*DMRmax
      INTEGER, PARAMETER :: MDRmin=XMRmin, MDRmax=XMRmax
!
      COMMON /RMASS_TABLES/ MASSR(MDRmin:MDRmax)
      REAL MASSR
      COMMON /RRATE_TABLES/ RRATE(MDRmin:MDRmax)
!
!--- Current scheme assumes an intercept for rain size spectra of N0r0=8*10**6 /m**4
!
      REAL, PARAMETER :: C1=1./3., C2=1./6., C3=3.31/6., 
     & DMR1=.1E-3, DMR2=.2E-3, DMR3=.32E-3, N0r0=8.E6, N0rmin=1.e4, 
     & RHO0=1.194, XMR1=1.e6*DMR1, XMR2=1.e6*DMR2, XMR3=1.e6*DMR3
      INTEGER, PARAMETER :: MDR1=XMR1, MDR2=XMR2, MDR3=XMR3
      COMMON /RVENT_TABLES/ VENTR1(MDRmin:MDRmax), VENTR2(MDRmin:MDRmax)
      COMMON /RACCR_TABLES/ ACCRR(MDRmin:MDRmax)
      COMMON /RVELR_TABLES/ VRAIN(MDRmin:MDRmax)
      COMMON /IRIME_TABLES/ VEL_RF(2:9,0:Nrime)
      real, parameter :: DMImin=.05e-3, DMImax=1.e-3, BsDMI=1.e-6,
     &  XMImin=1.e6*DMImin, XMImax=1.e6*DMImax
      integer, parameter :: MDImin=XMImin, MDImax=XMImax
      COMMON /IVENT_TABLES/ VENTI1(MDImin:MDImax), VENTI2(MDImin:MDImax)
      COMMON /IACCR_TABLES/ ACCRI(MDImin:MDImax)
      COMMON /IMASS_TABLES/ MASSI(MDImin:MDImax)
      REAL MASSI
      COMMON /IRATE_TABLES/ VSNOWI(MDImin:MDImax)

!!!CHEM_CP
      INTEGER, PARAMETER :: NUM_ASH=0,NUM_DUST=8,NUM_SALT=8,NUM_OM=0
     &                      ,NUM_BC=0,NUM_SO4=0                                          !!!CHANGE AS DESIRED
      INTEGER, PARAMETER :: NUM_AERO=NUM_ASH+NUM_DUST+NUM_SALT+NUM_OM
     &                              +NUM_BC+NUM_SO4

      COMMON /CAP_R_TABLES/ CAP_EFF_RAIN(MDRmin:MDRmax,1:NUM_AERO)
      COMMON /CAP_S_TABLES/ CAP_EFF_SNOW(MDImin:MDImax,1:NUM_AERO,1:4)
      REAL CL_MEAN(MDImin:MDImax)
!!!CHEM_CP

      INTEGER, PARAMETER :: MY_T1=1, MY_T2=35
      COMMON /CMY600/ MY_GROWTH(MY_T1:MY_T2)
      REAL MY_GROWTH
!-----------------------------------------------------------------------
      logical :: Print_diag
      data Print_diag / .true. /

      integer num, length, stat
      character*256 output_lookup_aerosol

      if (command_argument_count().gt.0) then
         call get_command_argument(1,output_lookup_aerosol,length,stat)
      end if

!-----------------------------------------------------------------------
!
!#######################################################################
!########################## Begin Execution ############################
!#######################################################################
!
      DTPH=6.*(26.+2./3.)   !--- Current parallel setup
      CALL MY_GROWTH_RATES (DTPH)       ! Lookup tables for growth of nucleated ice
      CALL ICE_LOOKUP (Print_diag)      ! Lookup tables for ice
      CALL RAIN_LOOKUP (Print_diag)     ! Lookup tables for rain
!      open(unit=1,file='lookup_aerosol.dat',form='unformatted')
!      write(1) ventr1
!      write(1) ventr2
!      write(1) accrr
!      write(1) massr
!      write(1) vrain
!      write(1) rrate
!      write(1) venti1
!      write(1) venti2
!      write(1) accri
!      write(1) massi
!      write(1) vsnowi
!      write(1) vel_rf
!      write(1) my_growth
!      close(1)
!
      open(unit=2,file=output_lookup_aerosol,form='unformatted')
      write(2) CAP_EFF_RAIN
      write(2) CAP_EFF_SNOW
      close(2)
!
!VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
      STOP
!VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
      END
!
!#######################################################################
!--------------- Creates lookup tables for ice processes ---------------
!#######################################################################
!
      subroutine ice_lookup (Print_diag)
!
      logical :: Print_diag
!-----------------------------------------------------------------------------------
!
!---- Key diameter values in mm
!
!-----------------------------------------------------------------------------------
!
!---- Key concepts:
!       - Actual physical diameter of particles (D)
!       - Ratio of actual particle diameters to mean diameter (x=D/MD)
!       - Mean diameter of exponentially distributed particles, which is the
!         same as 1./LAMDA of the distribution (MD)
!       - All quantitative relationships relating ice particle characteristics as
!         functions of their diameter (e.g., ventilation coefficients, normalized
!         accretion rates, ice content, and mass-weighted fall speeds) are a result
!         of using composite relationships for ice crystals smaller than 1.5 mm
!         diameter merged with analogous relationships for larger sized aggregates.
!         Relationships are derived as functions of mean ice particle sizes assuming
!         exponential size spectra and assuming the properties of ice crystals at
!         sizes smaller than 1.5 mm and aggregates at larger sizes.  
!
!-----------------------------------------------------------------------------------
!
!---- Actual ice particle diameters for which integrated distributions are derived
!       - DminI - minimum diameter for integration (.02 mm, 20 microns)
!       - DmaxI - maximum diameter for integration (2 cm)
!       - DdelI - interval for integration (1 micron)
!
      real, parameter :: DminI=.02e-3, DmaxI=20.e-3, DdelI=1.e-6,
     &  XImin=1.e6*DminI, XImax=1.e6*DmaxI
      integer, parameter :: IDImin=XImin, IDImax=XImax
!
!---- Meaning of the following arrays:
!        - diam - ice particle diameter (m)
!        - mass - ice particle mass (kg)
!        - vel  - ice particle fall speeds (m/s)
!        - vent1 - 1st term in ice particle ventilation factor
!        - vent2 - 2nd term in ice particle ventilation factor
!
      real diam(IDImin:IDImax),mass(IDImin:IDImax),vel(IDImin:IDImax),
     & vent1(IDImin:IDImax),vent2(IDImin:IDImax)
!
!-----------------------------------------------------------------------------------
!
!---- Found from trial & error that the m(D) & V(D) mass & velocity relationships
!       between the ice crystals and aggregates overlapped & merged near a particle
!       diameter sizes of 1.5 mm.  Thus, ice crystal relationships are used for
!       sizes smaller than 1.5 mm and aggregate relationships for larger sizes.
!
      real, parameter :: d_crystal_max=1.5
!
!---- The quantity xmax represents the maximum value of "x" in which the
!       integrated values are calculated.  For xmax=20., this means that
!       integrated ventilation, accretion, mass, and precipitation rates are
!       calculated for ice particle sizes less than 20.*mdiam, the mean particle diameter.
!
      real, parameter :: xmax=20.
!
!-----------------------------------------------------------------------------------
!
!-----------------------------------------------------------------------------------
!
!--- Parameters for ice lookup tables, which establish the range of mean ice particle
!      diameters; from a minimum mean diameter of 0.05 mm (DMImin) to a 
!      maximum mean diameter of 1.00 mm (DMImax).  The tables store solutions
!      at 1 micron intervals (BsDMI) of mean ice particle diameter.  
!
      real, parameter :: DMImin=.05e-3, DMImax=1.e-3, BsDMI=1.e-6,
     &  XMImin=1.e6*DMImin, XMImax=1.e6*DMImax
      integer, parameter :: MDImin=XMImin, MDImax=XMImax
!
!---- Meaning of the following arrays:
!        - mdiam - mean diameter (m)
!        - VENTI1 - integrated quantity associated w/ ventilation effects
!                   (capacitance only) for calculating vapor deposition onto ice
!        - VENTI2 - integrated quantity associated w/ ventilation effects
!                   (with fall speed) for calculating vapor deposition onto ice
!        - ACCRI  - integrated quantity associated w/ cloud water collection by ice
!        - MASSI  - integrated quantity associated w/ ice mass 
!        - VSNOWI - mass-weighted fall speed of snow, used to calculate precip rates
!
!--- Mean ice-particle diameters varying from 50 microns to 1000 microns (1 mm), 
!      assuming an exponential size distribution.  
!
      real mdiam
      COMMON /IVENT_TABLES/ VENTI1(MDImin:MDImax), VENTI2(MDImin:MDImax)
      COMMON /IACCR_TABLES/ ACCRI(MDImin:MDImax)
      COMMON /IMASS_TABLES/ MASSI(MDImin:MDImax)
      REAL MASSI
      COMMON /IRATE_TABLES/ VSNOWI(MDImin:MDImax)
!
!-----------------------------------------------------------------------------------
!------------- Constants & parameters for ventilation factors of ice ---------------
!-----------------------------------------------------------------------------------
!
!---- These parameters are used for calculating the ventilation factors for ice
!       crystals between 0.2 and 1.5 mm diameter (Hall and Pruppacher, JAS, 1976).  
!       From trial & error calculations, it was determined that the ventilation
!       factors of smaller ice crystals could be approximated by a simple linear
!       increase in the ventilation coefficient from 1.0 at 50 microns (.05 mm) to 
!       1.1 at 200 microns (0.2 mm), rather than using the more complex function of
!       1.0 + .14*(Sc**.33*Re**.5)**2 recommended by Hall & Pruppacher.
!
      real, parameter :: cvent1i=.86, cvent2i=.28
!
!---- These parameters are used for calculating the ventilation factors for larger
!       aggregates, where D>=1.5 mm (see Rutledge and Hobbs, JAS, 1983; 
!       Thorpe and Mason, 1966).
!
      real, parameter :: cvent1a=.65, cvent2a=.44
!
      real m_agg,m_bullet,m_column,m_ice,m_plate
!
!---- Various constants
!
      real, parameter :: c1=2./3., cexp=1./3.
!
      logical :: iprint
!
!-----------------------------------------------------------------------------------
!- Constants & parameters for calculating the increase in fall speed of rimed ice --
!-----------------------------------------------------------------------------------
!
!---- Constants & arrays for estimating increasing fall speeds of rimed ice.
!     Based largely on theory and results from Bohm (JAS, 1989, 2419-2427).
!
!-------------------- Standard atmosphere conditions at 1000 mb --------------------
!
      real, parameter :: t_std=288., dens_std=1000.e2/(287.04*288.)
!
!---- These "bulk densities" are the actual ice densities in the ice portion of the 
!     lattice.  They are based on text associated w/ (12) on p. 2425 of Bohm (JAS, 
!     1989).  Columns, plates, & bullets are assumed to have an average bulk density 
!     of 850 kg/m**3.  Aggregates were assumed to have a slightly larger bulk density 
!     of 600 kg/m**3 compared with dendrites (i.e., the least dense, most "lacy" & 
!     tenous ice crystal, which was assumed to be ~500 kg/m**3 in Bohm).  
!
      real, parameter :: dens_crystal=850., dens_agg=600.
!
!--- A value of Nrime=40 for a logarithmic ratio of 1.1 yields a maximum rime factor
!      of 1.1**40 = 45.26 that is resolved in these tables.  This allows the largest
!      ice particles with a mean diameter of MDImax=1000 microns to achieve bulk 
!      densities of 900 kg/m**3 for rimed ice.  
!
      integer, parameter :: Nrime=40
      real m_rime,
     &     rime_factor(0:Nrime), rime_vel(0:Nrime), 
     &     vel_rime(IDImin:IDImax,Nrime), ivel_rime(MDImin:MDImax,Nrime)
      COMMON /IRIME_TABLES/ VEL_RF(2:9,0:Nrime)
!
      integer, parameter :: nlines=22
!
!!!CHEM_CP
      real cl(IDImin:IDImax)
!
!!!MSPADA
!!! D2WF = r_wet/r_dry following Chin et al., 2002
      REAL, PARAMETER :: D2WF=CCCC
      REAL, PARAMETER :: 
     & RHO_WATER(8)=(/1000.,1000.,1000.,1000.,1000.,1000.,1000.,1000./)
!!!MSPADA
      INTEGER, PARAMETER :: NUM_ASH=0,NUM_DUST=8,NUM_SALT=8,NUM_OM=0
     &                      ,NUM_BC=0,NUM_SO4=0                                          !!!CHANGE AS DESIRED
      INTEGER, PARAMETER :: NUM_AERO=NUM_ASH+NUM_DUST+NUM_SALT+NUM_OM
     &                              +NUM_BC+NUM_SO4
!
      REAL, PARAMETER ::
     & RHO_DUST(8)=(/2500.,2500.,2500.,2500.,2650.,2650.,2650.,2650./)                  !!DUST DENSITY IN KG/M3
     &,R_DUST_V(8)=(/0.15E-6,0.25E-6,0.47E-6,0.80E-6,1.36E-6,2.29E-6
     &              ,3.93E-6,7.24E-6/)                                                  !!Volume radius of each bin in m
     &,R_DUST_E(8)=(/0.15E-6,0.25E-6,0.45E-6,0.78E-6,1.32E-6,2.24E-6
     &               ,3.80E-6,7.11E-6/)                                                  !!EFFECTIVE RADIUS OF EACH BIN IN M
!
!!!MSPADA
     &,RHO_SALT_D(8)=(/2160.,2160.,2160.,2160.,2160.,2160.,2160.,2160./)                  !!JUST FIRST GUESS FOR SEA-SALT
     &,RHO_SALT(8)=RHO_SALT_D(8)
!     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.47E-6,0.80E-6,1.36E-6,2.29E-6
!     &              ,3.93E-6,7.24E-6/)                                                  !!JUST FIRST GUESS FOR SEA-SALT
!!     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.46E-6,0.79E-6,1.33E-6,2.26E-6
!!     &              ,3.87E-6,7.19E-6/)                                                  !!SECOND GUESS FOR SEA-SALT (GADS)
!
     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.47E-6,0.81E-6,1.40E-6,2.37E-6
     &              ,4.30E-6,9.23E-6/) ! bin8 extended to 15um                            !! THIRD GUESS (LS04)
     &,R_SALT_V(8)=R_SALT_V_D(8)
!
!     &,R_SALT_E_D(8)=(/0.15E-6,0.25E-6,0.45E-6,0.78E-6,1.32E-6,2.24E-6
!     &              ,3.80E-6,7.11E-6/)                                                  !!JUST FIRST GUESS FOR SEA-SALT
!!     &,R_SALT_E_D(8)=(/0.15E-6,0.24E-6,0.44E-6,0.77E-6,1.30E-6,2.21E-6
!!     &              ,3.74E-6,7.06E-6/)                                                  !!SECOND GUESS FOR SEA-SALT (GADS)
!
     &,R_SALT_E_D(8)=(/0.14E-6,0.24E-6,0.45E-6,0.79E-6,1.36E-6,2.32E-6
     &              ,4.13E-6,8.64E-6/) ! bin 8 extended to 15um                           !! THIRD GUESS (LS04)
     &,R_SALT_E(8)=R_SALT_E_D(8)
!
!!!MSPADA
!
     &,RHO_OM  (2)=(/0.,0./)  !NOT ACTIVE
     &,R_OM_V  (2)=(/0.,0./)
     &,R_OM_E  (2)=(/0.,0./)
!
     &,RHO_BC  (2)=(/0.,0./)  !NOT ACTIVE
     &,R_BC_V  (2)=(/0.,0./)
     &,R_BC_E  (2)=(/0.,0./)
!
     &,RHO_SO4 (2)=(/0.,0./)  !NOT ACTIVE
     &,R_SO4_V (2)=(/0.,0./)
     &,R_SO4_E (2)=(/0.,0./)

!We assume average conditions
      REAL,PARAMETER :: T=273.0,RHO=0.8
!
!      REAL,PARAMETER :: PI=3.1416

      REAL KB,WTAIR,AVG,RGAS,VISCOS,VISCOSK
      REAL VTHERMG,FREEMP,CC,BRD,TAU,RE,SC,ST,SSTAR
      REAL GAMMAS,PP
      REAL RHO_AERO(1:NUM_AERO),R_AERO_V(1:NUM_AERO)
      REAL R_AERO_E(1:NUM_AERO),D_AERO(1:NUM_AERO)
      REAL VSNOW_GAMMA(MDImin:MDImax,1:4)
      INTEGER N,M
      COMMON /CAP_S_TABLES/ CAP_EFF_SNOW(MDImin:MDImax,1:NUM_AERO,1:4)
      REAL CL_MEAN(MDImin:MDImax)


!!!CHEM_CP
!
!-----------------------------------------------------------------------------------
!----------------------------- BEGIN EXECUTION -------------------------------------
!-----------------------------------------------------------------------------------
!
!NOW collect or read densities and radii of each aerosol type
!ASH WILL BE READ FROM FILE (ARNAU)

      IF (NUM_AERO>0)THEN
!
        IF(NUM_ASH>0)THEN
         DO N=1,MAX(1,NUM_ASH)
          RHO_AERO(N) = 0.
          R_AERO_V(N) = 0.
          R_AERO_E(N) = 0.
         ENDDO
        ENDIF !ENDIF NUM_ASH
!
        M=NUM_ASH
!
        IF(NUM_DUST>0)THEN
         DO N=1,MAX(1,NUM_DUST)
          RHO_AERO(M+N) = RHO_DUST(N)
          R_AERO_V(M+N) = R_DUST_V(N)
          R_AERO_E(M+N) = R_DUST_E(N)
         ENDDO
        ENDIF !ENDIF NUM_DUST
!
        M=M+NUM_DUST
!
        IF(NUM_SALT>0)THEN
         DO N=1,MAX(1,NUM_SALT)
          !RHO_AERO(M+N) = RHO_SALT(N)
          RHO_AERO(M+N) = D2WF**(-3)*RHO_SALT_D(N)+(1.-D2WF**(-3))*1000.
          R_AERO_V(M+N) = D2WF*R_SALT_V_D(N)
          R_AERO_E(M+N) = D2WF*R_SALT_E_D(N)
          !R_AERO_V(M+N) = R_SALT_V(N)
          !R_AERO_E(M+N) = R_SALT_E(N)
!MSPADA DEBUG
          WRITE(8,*) 'cazzo r_v= ',R_SALT_V(N),N
          WRITE(8,*) 'cazzo r_v new = ',R_AERO_V(M+N)
          WRITE(8,*) 'cazzo r_e= ',R_SALT_E(N)
          WRITE(8,*) 'cazzo r_e new = ',R_AERO_E(M+N)
          WRITE(8,*) 'rho= ',RHO_SALT(N)
          WRITE(8,*) 'rho new= ',RHO_AERO(M+N),RHO_SALT_D(N)

         ENDDO
        ENDIF !ENDIF NUM_SALT
!
        M=M+NUM_SALT
!
        IF(NUM_OM>0)THEN
         DO N=1,MAX(1,NUM_OM)
          RHO_AERO(M+N) = RHO_OM(N)
          R_AERO_V(M+N) = R_OM_V(N)
          R_AERO_E(M+N) = R_OM_E(N)
         ENDDO
        ENDIF !ENDIF NUM_OM
!
        M=M+NUM_OM
!
        IF(NUM_BC>0)THEN
         DO N=1,MAX(1,NUM_BC)
          RHO_AERO(M+N) = RHO_BC(N)
          R_AERO_V(M+N) = R_BC_V(N)
          R_AERO_E(M+N) = R_BC_E(N)
         ENDDO
        ENDIF !ENDIF NUM_BC
!
       M=M+NUM_BC
!
        IF(NUM_SO4>0)THEN
         DO N=1,MAX(1,NUM_SO4)
          RHO_AERO(M+N) = RHO_SO4(N)
          R_AERO_V(M+N) = R_SO4_V(N)
          R_AERO_E(M+N) = R_SO4_E(N)
         ENDDO
        ENDIF  !ENDIF NUM_SO4
!
       ENDIF !ENDIF NUM_AERO
!
      WRITE(0,*)'*************************************'
      WRITE(0,*)' NUM_AERO =',NUM_AERO
      WRITE(0,*)' NUM_TRACERS_CHEM=',NUM_TRACERS_CHEM
      WRITE(0,*)' NUM_DUST =',NUM_DUST
      WRITE(0,*)' NUM_SALT =',NUM_SALT
      WRITE(0,*)' NUM_OM =',NUM_OM
      WRITE(0,*)' NUM_BC=',NUM_BC
      WRITE(0,*)' NUM_SO4  =',NUM_SO4
      WRITE(0,*)' RHO_AERO =',RHO_AERO
      WRITE(0,*)' R_AERO_V  =',R_AERO_V
      WRITE(0,*)' R_AERO_E =',R_AERO_E
      WRITE(0,*)'*************************************'

      KB        = 1.3806504e-23                     !: boltzmann's 1.3806504e-23 J K-1  (= kg m**2 s-2 k-1)
      WTAIR     = 28.966e-3                         !: molecular weight of air (kg mol-1)
      AVG       = 6.02252e+23                       !: avogadro's number (molec. mol-1)
      RGAS      = 8.314472                          !: gas constant (= 8.314472 J K-1 mol-1 =  kg m**2 K-1 mol-1)
      VISCOS    = 1.718e-5                          !: dynamic viscosity of air (=1.718e-5 kg m-1 s-1)
      VISCOSK   = VISCOS / RHO                      !: kinematic viscosity = viscos / rho = (m**2 s-1)
      VTHERMG   = SQRT(8.*KB*T*AVG/(3.1416*WTAIR))  !: mean thermal velocity of air molecules (m s-1)
      FREEMP    = 2.0 * VISCOSK / VTHERMG           !: mean free path of an air molecule  = 2 x viscosk /
!!!CHEM_CP
!
      c2=1./sqrt(3.)
      pi=acos(-1.)
      cbulk=6./pi
      cbulk_ice=900.*pi/6.    ! Maximum bulk ice density allowed of 900 kg/m**3
      px=.4**cexp             ! Convert fall speeds from 400 mb (Starr & Cox) to 1000 mb
!
!--------------------- Dynamic viscosity (1000 mb, 288 K) --------------------------
!
      dynvis_std=1.496e-6*t_std**1.5/(t_std+120.)
      crime1=pi/24.
      crime2=8.*9.81*dens_std/(pi*dynvis_std**2)
      crime3=crime1*dens_crystal
      crime4=crime1*dens_agg
      crime5=dynvis_std/dens_std
      do i=0,Nrime
        rime_factor(i)=1.1**i
      enddo
!
!#######################################################################
!      Characteristics as functions of actual ice particle diameter 
!#######################################################################
!
!----   M(D) & V(D) for 3 categories of ice crystals described by Starr 
!----   & Cox (1985). 
!
!----   Capacitance & characteristic lengths for Reynolds Number calculations
!----   are based on Young (1993; p. 144 & p. 150).  c-axis & a-axis 
!----   relationships are from Heymsfield (JAS, 1972; Table 1, p. 1351).
!
      icount=nlines
!
      if (print_diag) 
     &  write(7,"(/2a/)") '---- Increase in fall speeds of rimed ice',
     &    ' particles as function of ice particle diameter ----'
      do i=IDImin,IDImax
!--- Print header every "nlines" lines
        if (icount.eq.nlines .and. print_diag) then
          write(6,"(/2a/3a)") '----- Particle masses (mg), fall ',
     &      'speeds (m/s), and ventilation factors -----',
     &      '  D(mm)  CR_mass   Mass_bull   Mass_col  Mass_plat ',
     &      '  Mass_agg   CR_vel  V_bul CR_col CR_pla Aggreg',
     &      '    Vent1      Vent2      CL(mm) '
          write(7,"(3a)") '        <----------------------------------',
     &      '---------------  Rime Factor  --------------------------',
     &      '--------------------------->'
          write(7,"(a,23f5.2)") '  D(mm)',(rime_factor(k), k=1,5),
     &       (rime_factor(k), k=6,40,2)
          icount=0
        endif
        d=(float(i)+.5)*1.e-3    ! in mm
        c_avg=0.
        c_agg=0.
        c_bullet=0.
        c_column=0.
        c_plate=0.
        cl_agg=0.
        cl_bullet=0.
        cl_column=0.
        cl_plate=0.
        m_agg=0.
        m_bullet=0.
        m_column=0.
        m_plate=0.
        v_agg=0.
        v_bullet=0.
        v_column=0.
        v_plate=0.
        if (d .lt. d_crystal_max) then
!
!---- This block of code calculates bulk characteristics based on average
!     characteristics of bullets, plates, & column ice crystals <1.5 mm size
!
!---- Mass-diameter relationships from Heymsfield (1972) & used
!       in Starr & Cox (1985), units in mg
!---- "d" is maximum dimension size of crystal in mm, 
!
! Mass of pure ice for spherical particles, used as an upper limit for the
!   mass of small columns (<~ 80 microns) & plates (<~ 35 microns)
!
          m_ice=.48*d**3   ! Mass of pure ice for spherical particle
!
          m_bullet=min(.044*d**3, m_ice)
          m_column=min(.017*d**1.7, m_ice)
          m_plate=min(.026*d**2.5, m_ice)
!
          mass(i)=m_bullet+m_column+m_plate
!
!---- These relationships are from Starr & Cox (1985), applicable at 400 mb
!---- "d" is maximum dimension size of crystal in mm, dx in microns
!
          dx=1000.*d            ! Convert from mm to microns
          if (dx .le. 200.) then
            v_column=8.114e-5*dx**1.585
            v_bullet=5.666e-5*dx**1.663
            v_plate=1.e-3*dx
          else if (dx .le. 400.) then
            v_column=4.995e-3*dx**.807
            v_bullet=3.197e-3*dx**.902
            v_plate=1.48e-3*dx**.926
          else if (dx .le. 600.) then
            v_column=2.223e-2*dx**.558
            v_bullet=2.977e-2*dx**.529
            v_plate=9.5e-4*dx
          else if (dx .le. 800.) then
            v_column=4.352e-2*dx**.453
            v_bullet=2.144e-2*dx**.581
            v_plate=3.161e-3*dx**.812
          else 
            v_column=3.833e-2*dx**.472
            v_bullet=3.948e-2*dx**.489
            v_plate=7.109e-3*dx**.691
          endif
!
!---- Reduce fall speeds from 400 mb to 1000 mb
!
          v_column=px*v_column
          v_bullet=px*v_bullet
          v_plate=px*v_plate
!
!---- DIFFERENT VERSION!  CALCULATES MASS-WEIGHTED CRYSTAL FALL SPEEDS
!
          vel(i)=(m_bullet*v_bullet+m_column*v_column+m_plate*v_plate)/
     &           mass(i)
          mass(i)=mass(i)/3.
!
!---- Shape factor and characteristic length of various ice habits,
!     capacitance is equal to 4*PI*(Shape factor)
!       See Young (1993, pp. 143-152 for guidance)
!
!---- Bullets:
!
!---- Shape factor for bullets (Heymsfield, 1975)
          c_bullet=.5*d
!---- Length-width functions for bullets from Heymsfield (JAS, 1972)
          if (d .gt. 0.3) then
            wd=.25*d**.7856     ! Width (mm); a-axis
          else
            wd=.185*d**.552
          endif
!---- Characteristic length for bullets (see first multiplicative term on right
!       side of eq. 7 multiplied by crystal width on p. 821 of Heymsfield, 1975)
          cl_bullet=.5*pi*wd*(.25*wd+d)/(d+wd)
!
!---- Plates:
!
!---- Length-width function for plates from Heymsfield (JAS, 1972)
          wd=.0449*d**.449      ! Width or thickness (mm); c-axis
!---- Eccentricity & shape factor for thick plates following Young (1993, p. 144)
          ecc_plate=sqrt(1.-wd*wd/(d*d))         ! Eccentricity
          c_plate=d*ecc_plate/asin(ecc_plate)    ! Shape factor
!---- Characteristic length for plates following Young (1993, p. 150, eq. 6.6)
          cl_plate=d+2.*wd      ! Characteristic lengths for plates
!
!---- Columns:
!
!---- Length-width function for columns from Heymsfield (JAS, 1972)
          if (d .gt. 0.2) then
            wd=.1973*d**.414    ! Width (mm); a-axis
          else
            wd=.5*d             ! Width (mm); a-axis
          endif
!---- Eccentricity & shape factor for columns following Young (1993, p. 144)
          ecc_column=sqrt(1.-wd*wd/(d*d))                     ! Eccentricity
          c_column=ecc_column*d/alog((1.+ecc_column)*d/wd)    ! Shape factor
!---- Characteristic length for columns following Young (1993, p. 150, eq. 6.7)
          cl_column=(wd+2.*d)/(c1+c2*d/wd)       ! Characteristic lengths for columns
!
!---- Convert shape factor & characteristic lengths from mm to m for 
!       ventilation calculations
!
          c_bullet=.001*c_bullet
          c_plate=.001*c_plate
          c_column=.001*c_column
          cl_bullet=.001*cl_bullet
          cl_plate=.001*cl_plate
          cl_column=.001*cl_column
!CHEM_CP
          cl(i)=(cl_bullet+cl_plate+cl_column)/3. !AVERAGE CHARACTERISTIC LENGTH
!CHEM_CP
!
!---- Make a smooth transition between a ventilation coefficient of 1.0 at 50 microns
!       to 1.1 at 200 microns
!
          if (d .gt. 0.2) then
            cvent1=cvent1i
            cvent2=cvent2i/3.
          else
            cvent1=1.0+.1*max(0., d-.05)/.15
            cvent2=0.
          endif
!
!---- Ventilation factors for ice crystals:
!
          vent1(i)=cvent1*(c_bullet+c_plate+c_column)/3.
          vent2(i)=cvent2*(c_bullet*sqrt(v_bullet*cl_bullet)
     &                    +c_plate*sqrt(v_plate*cl_plate)
     &                    +c_column*sqrt(v_column*cl_column) )
          crime_best=crime3     ! For calculating Best No. of rimed ice crystals
        else
!
!---- This block of code calculates bulk characteristics based on average
!     characteristics of unrimed aggregates >= 1.5 mm using Locatelli & 
!     Hobbs (JGR, 1974, 2185-2197) data.
!
!----- This category is a composite of aggregates of unrimed radiating 
!-----   assemblages of dendrites or dendrites; aggregates of unrimed
!-----   radiating assemblages of plates, side planes, bullets, & columns;
!-----   aggregates of unrimed side planes (mass in mg, velocity in m/s)
!
          m_agg=(.073*d**1.4+.037*d**1.9+.04*d**1.4)/3.
          v_agg=(.8*d**.16+.69*d**.41+.82*d**.12)/3.
          mass(i)=m_agg
          vel(i)=v_agg
!
!---- Assume spherical aggregates
!
!---- Shape factor is the same as for bullets, = D/2
          c_agg=.001*.5*d         ! Units of m
!---- Characteristic length is surface area divided by perimeter
!       (.25*PI*D**2)/(PI*D**2) = D/4
          cl_agg=.5*c_agg         ! Units of m
!
!!!CHEM_CP
          cl(i)=cl_agg
!!!CHEM_CP
!
!---- Ventilation factors for aggregates:
!
          vent1(i)=cvent1a*c_agg
          vent2(i)=cvent2a*c_agg*sqrt(v_agg*cl_agg)
          crime_best=crime4     ! For calculating Best No. of rimed aggregates
        endif
!
!---- Convert from shape factor to capacitance for ventilation factors
!
        vent1(i)=4.*pi*vent1(i)
        vent2(i)=4.*pi*vent2(i)
        diam(i)=1.e-3*d             ! Convert from mm to m
        mass(i)=1.e-6*mass(i)       ! Convert from mg to kg
!
!---- Calculate increase in fall speeds of individual rimed ice particles
!
        do k=0,Nrime
!---- Mass of rimed ice particle associated with rime_factor(k)
          rime_m1=rime_factor(k)*mass(i)
          rime_m2=cbulk_ice*diam(i)**3
          m_rime=min(rime_m1, rime_m2)
!---- Best Number (X) of rimed ice particle combining eqs. (8) & (12) in Bohm
          x_rime=crime2*m_rime*(crime_best/m_rime)**.25
!---- Reynolds Number for rimed ice particle using eq. (11) in Bohm
          re_rime=8.5*(sqrt(1.+.1519*sqrt(x_rime))-1.)**2
          rime_vel(k)=crime5*re_rime/diam(i)
        enddo
        do k=1,Nrime
          vel_rime(i,k)=rime_vel(k)/rime_vel(0)
        enddo
        if (print_diag) then
   !
   !---- Determine if statistics should be printed out.
   !
          iprint=.false.
          if (d .le. 1.) then
            if (mod(i,10) .eq. 0) iprint=.true.
          else
            if (mod(i,100) .eq. 0) iprint=.true.
          endif
          if (iprint) then
!CHEM_CP            write(6,"(f7.4,5e11.4,1x,5f7.4,1x,2e11.4)") 
            write(6,"(f7.4,5e11.4,1x,5f7.4,1x,3e11.4)")
     &        d,1.e6*mass(i),m_bullet,m_column,m_plate,m_agg,
     &        vel(i),v_bullet,v_column,v_plate,v_agg,
     &        vent1(i),vent2(i),1.e3*cl(i)
            write(7,"(f7.4,23f5.2)") d,(vel_rime(i,k), k=1,5),
     &        (vel_rime(i,k), k=6,40,2)
            icount=icount+1
          endif
        endif
      enddo
!
!#######################################################################
!      Characteristics as functions of mean particle diameter
!#######################################################################
!
      VENTI1=0.
      VENTI2=0.
      ACCRI=0.
      MASSI=0.
      VSNOWI=0.
!!!CHEM_CP
      CL_MEAN=0.
!!!CHEM_CP
      VEL_RF=0.
      ivel_rime=0.
      icount=0
      if (print_diag) then
        icount=nlines
        write(6,"(/3a)") '----- Statistics as functions of mean ',
     &    'diameter (integrated over size distributions) for ',
     &    'unrimed ice particles -----'
        write(7,"(/3a)") '----- Increase in fall speeds of rimed ice',
     &    ' particles as functions of mean particle diameter ',
     &    ' (integrated over size distributions) -----'
      endif
      do j=MDImin,MDImax
        if (icount.eq.nlines .AND. print_diag) then
          write(6,"(/2a)") 'D(mm)    Vent1      Vent2    ',
     &       'Accrete       Mass     Vel  Dens    CL_MEAN ',
     &       'B1         B2         ',
     &       'B3         B4      B5      B6      B7      ',
     &       'B8'

          write(7,"(/3a)") '      <----------------------------------',
     &      '---------------  Rime Factor  --------------------------',
     &      '--------------------------->'
          write(7,"(a,23f5.2)") 'D(mm)',(rime_factor(k), k=1,5),
     &       (rime_factor(k), k=6,40,2)
          icount=0
        endif
        mdiam=BsDMI*float(j)       ! in m
        smom3=0.
        pratei=0.
        rime_vel=0.                 ! Note that this array is being reused!
        do i=IDImin,IDImax
          dx=diam(i)/mdiam
          if (dx .le. xmax) then    ! To prevent arithmetic underflows
            expf=exp(-dx)*DdelI
            VENTI1(J)=VENTI1(J)+vent1(i)*expf
            VENTI2(J)=VENTI2(J)+vent2(i)*expf
!!!CHEM_CP
            CL_MEAN(J)=CL_MEAN(J)+cl(i)*expf
!!!CHEM_CP
            ACCRI(J)=ACCRI(J)+diam(i)*diam(i)*vel(i)*expf
            xmass=mass(i)*expf
            do k=1,Nrime
              rime_vel(k)=rime_vel(k)+xmass*vel_rime(i,k)
            enddo
            MASSI(J)=MASSI(J)+xmass
            pratei=pratei+xmass*vel(i)
            smom3=smom3+diam(i)**3*expf
          else
            exit
          endif
        enddo
   !
   !--- Increased fall velocities functions of mean diameter (j),
   !      normalized by ice content, and rime factor (k) 
   !
        do k=1,Nrime
          ivel_rime(j,k)=rime_vel(k)/MASSI(J)
        enddo
   !
   !--- Increased fall velocities functions of ice content at 0.1 mm
   !      intervals (j_100) and rime factor (k); accumulations here
   !
        jj=j/100
        if (jj.ge.2 .AND. jj.le.9) then
          do k=1,Nrime
            VEL_RF(jj,k)=VEL_RF(jj,k)+ivel_rime(j,k)
          enddo
        endif
        bulk_dens=cbulk*MASSI(J)/smom3
        VENTI1(J)=VENTI1(J)/mdiam
        VENTI2(J)=VENTI2(J)/mdiam
        ACCRI(J)=ACCRI(J)/mdiam
        VSNOWI(J)=pratei/MASSI(J)
        MASSI(J)=MASSI(J)/mdiam

!!!CHEM_CP
        CL_MEAN(J)=CL_MEAN(J)/mdiam

        PP=0.7E5

        GAMMAS=(1.E5/PP)**(1/3)       !!!WE ASSUME CONSTANT PRESSURE (in Pa)

!!!WE ASSUME FOUR REGIONS OF RIMING  (1.          ,  1.-8.    ,  8.-40.,  >40.)
!                                    (unrimed snow, rimed snow, graupel, sleet)
!!! INCREASE IN FALL SPEEDS          (1.          ,  1.5      ,   3.   ,  3.5 )

        VSNOW_GAMMA(J,1)=GAMMAS*VSNOWI(J)
        VSNOW_GAMMA(J,2)=GAMMAS*VSNOWI(J)*1.5
        VSNOW_GAMMA(J,3)=GAMMAS*VSNOWI(J)*3.
        VSNOW_GAMMA(J,4)=GAMMAS*VSNOWI(J)*3.5

        DO M=1,4         !!loop on rime factor regions 
         DO N=1,NUM_AERO  !!loop on aerosol
!
         D_AERO(N)=R_AERO_E(N)*2.
         CC=1.+2.*FREEMP*(1.257+0.4*EXP(-0.55*D_AERO(N)/FREEMP))
     &      /D_AERO(N)
         BRD=KB*T*CC/(3*PI*VISCOS*D_AERO(N))
         TAU=RHO_AERO(N)*CC*(D_AERO(N)**2.)/(18.*VISCOS)
         RE=CL_MEAN(J)*VSNOW_GAMMA(J,M)*RHO/(2.*VISCOS)     !!!WE USE CHARACTERISTIC LENGTH!!
         SC=VISCOS/(RHO*BRD)
         V_AERO=9.8*TAU
         ST=2.*TAU*(VSNOW_GAMMA(J,M)-V_AERO)/CL_MEAN(J)
         SSTAR=(1.2+(1./12.)*LOG(1.+RE))/(1.+ LOG(1.+RE))
         CAP_EFF_SNOW(J,N,M)=  (1/SC)**(2./3.) 
         CAP_EFF_SNOW(J,N,M)=  CAP_EFF_SNOW(J,N,M)
     &                       + (1-EXP(-(1+RE**0.5)*((D_AERO(N)/2)**2
     &                       / CL_MEAN(J)**2)))
!                          
        IF(ST>SSTAR)THEN
         CAP_EFF_SNOW(J,N,M)= CAP_EFF_SNOW(J,N,M)
     &                        +((ST-SSTAR)/(ST-SSTAR+2./3.))**1.5 
        ENDIF
!
!         IF (CAP_EFF_SNOW(J,N,M)>1.) CAP_EFF_SNOW(J,N,M)=1.

         ENDDO  !!! loop on aerosol
        ENDDO  !!! loop on rime factor regions

        if (mod(j,10).eq.0 .AND. print_diag) then
          xmdiam=1.e3*mdiam
!          write(6,"(f5.3,4e11.4,f6.3,f8.3)") xmdiam,VENTI1(j),VENTI2(j),
         write(6,"(f5.3,4e11.4,f6.3,f8.3,f6.3,8g11.5)") xmdiam,VENTI1(j)
     &   ,VENTI2(j),
     &    ACCRI(j),MASSI(j),VSNOWI(j),bulk_dens,1.e3*CL_MEAN(j)
     &   ,CAP_EFF_SNOW(J,1,1),CAP_EFF_SNOW(J,2,1),CAP_EFF_SNOW(J,3,1)
     &   ,CAP_EFF_SNOW(J,4,1),CAP_EFF_SNOW(J,5,1),CAP_EFF_SNOW(J,6,1)
     &   ,CAP_EFF_SNOW(J,7,1),CAP_EFF_SNOW(J,8,1)


          write(7,"(f5.3,23f5.2)") xmdiam,(ivel_rime(j,k), k=1,5),
     &       (ivel_rime(j,k), k=6,40,2)
          icount=icount+1
        endif
      enddo
!
!--- Average increase in fall velocities rimed ice as functions of mean
!      particle diameter (j, only need 0.1 mm intervals) and rime factor (k)
!
      if (print_diag) then
        write(7,"(/2a)") '----- Increase in fall speeds of rimed ',
     &    'ice particles at reduced, 0.1-mm intervals  -----'
        write(7,"(/3a)") '        <----------------------------------',
     &    '---------------  Rime Factor  --------------------------',
     &    '--------------------------->'
        write(7,"(a,23f5.2)") 'D(mm)',(rime_factor(k), k=1,5),
     &    (rime_factor(k), k=6,40,2)
      endif
      do j=2,9
        VEL_RF(j,0)=1.
        do k=1,Nrime
          VEL_RF(j,k)=.01*VEL_RF(j,k)
        enddo
        if (print_diag) write(7,"(f3.1,2x,23f5.2)") 0.1*j,
     &    (VEL_RF(j,k), k=1,5),(VEL_RF(j,k), k=6,40,2)
      enddo
!
!-----------------------------------------------------------------------------------
!
      return
      end
!
!#######################################################################
!-------------- Creates lookup tables for rain processes ---------------
!#######################################################################
!
      subroutine rain_lookup (Print_diag)
      logical :: Print_diag
!
!--- Parameters & arrays for fall speeds of rain as a function of rain drop
!      diameter.  These quantities are integrated over exponential size
!      distributions of rain drops at 1 micron intervals (DdelR) from minimum 
!      drop sizes of .05 mm (50 microns, DminR) to maximum drop sizes of 10 mm 
!      (DmaxR). 
!
      real, parameter :: DminR=.05e-3, DmaxR=10.e-3, DdelR=1.e-6, 
     & XRmin=1.e6*DminR, XRmax=1.e6*DmaxR
      integer, parameter :: IDRmin=XRmin, IDRmax=XRmax
      real diam(IDRmin:IDRmax), vel(IDRmin:IDRmax)
!
!--- Parameters rain lookup tables, which establish the range of mean drop
!      diameters; from a minimum mean diameter of 0.05 mm (DMRmin) to a 
!      maximum mean diameter of 0.45 mm (DMRmax).  The tables store solutions
!      at 1 micron intervals (BsDMR) of mean drop diameter.  
!
      real, parameter :: DMRmin=.05e-3, DMRmax=.45e-3, BsDMR=1.e-6,
     & XMRmin=1.e6*DMRmin, XMRmax=1.e6*DMRmax
      integer, parameter :: MDRmin=XMRmin, MDRmax=XMRmax
      real mdiam, mass
!
!--- Rain lookup tables for mean rain drop diameters from DMRmin to DMRmax,
!      assuming exponential size distributions for the rain drops
      COMMON /RVENT_TABLES/ VENTR1(MDRmin:MDRmax), VENTR2(MDRmin:MDRmax)
      COMMON /RACCR_TABLES/ ACCRR(MDRmin:MDRmax)
      COMMON /RMASS_TABLES/ MASSR(MDRmin:MDRmax)
      REAL MASSR
      COMMON /RRATE_TABLES/ RRATE(MDRmin:MDRmax)
      COMMON /RVELR_TABLES/ VRAIN(MDRmin:MDRmax)
!

!!!CHEM_CP
!!!MSPADA
      REAL, PARAMETER :: D2WF=CCCC
      REAL, PARAMETER :: 
     & RHO_WATER(8)=(/1000.,1000.,1000.,1000.,1000.,1000.,1000.,1000./)
!!!MSPADA
      INTEGER, PARAMETER :: NUM_ASH=0,NUM_DUST=8,NUM_SALT=8,NUM_OM=0
     &                      ,NUM_BC=0,NUM_SO4=0                                          !!!CHANGE AS DESIRED
      INTEGER, PARAMETER :: NUM_AERO=NUM_ASH+NUM_DUST+NUM_SALT+NUM_OM
     &                              +NUM_BC+NUM_SO4

      REAL, PARAMETER :: 
     & RHO_DUST(8)=(/2500.,2500.,2500.,2500.,2650.,2650.,2650.,2650./)                  !!DUST DENSITY IN KG/M3
     &,R_DUST_V(8)=(/0.15E-6,0.25E-6,0.47E-6,0.80E-6,1.36E-6,2.29E-6
     &              ,3.93E-6,7.24E-6/)                                                  !!Volume radius of each bin in m
     &,R_DUST_E(8)=(/0.15E-6,0.25E-6,0.45E-6,0.78E-6,1.32E-6,2.24E-6
     &               ,3.80E-6,7.11E-6/)                                                  !!EFFECTIVE RADIUS OF EACH BIN IN M
!
!!!MSPADA
     &,RHO_SALT_D(8)=(/2160.,2160.,2160.,2160.,2160.,2160.,2160.,2160./)                  !!JUST FIRST GUESS FOR SEA-SALT
     &,RHO_SALT(8)=RHO_SALT_D(8)
!     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.47E-6,0.80E-6,1.36E-6,2.29E-6
!     &              ,3.93E-6,7.24E-6/)                                                  !!JUST FIRST GUESS FOR SEA-SALT
!
!!     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.46E-6,0.79E-6,1.33E-6,2.26E-6
!!     &              ,3.87E-6,7.19E-6/)                                                  !!SECOND GUESS FOR SEA-SALT (GADS)
     &,R_SALT_V_D(8)=(/0.15E-6,0.25E-6,0.47E-6,0.81E-6,1.40E-6,2.37E-6
     &              ,4.30E-6,9.23E-6/) ! bin8 extended to 15um                            !! THIRD GUESS (LS04)
     &,R_SALT_V(8)=D2WF*R_SALT_V_D(8)
!     &,R_SALT_E_D(8)=(/0.15E-6,0.25E-6,0.45E-6,0.78E-6,1.32E-6,2.24E-6
!     &              ,3.80E-6,7.11E-6/)                                                  !!JUST FIRST GUESS FOR SEA-SALT
!
!!     &,R_SALT_E_D(8)=(/0.15E-6,0.24E-6,0.44E-6,0.77E-6,1.30E-6,2.21E-6
!!     &              ,3.74E-6,7.06E-6/)                                                  !!SECOND GUESS FOR SEA-SALT (GADS)
!
     &,R_SALT_E_D(8)=(/0.14E-6,0.24E-6,0.45E-6,0.79E-6,1.36E-6,2.32E-6
     &              ,4.13E-6,8.64E-6/) ! bin 8 extended to 15um                           !! THIRD GUESS (LS04)
     &,R_SALT_E(8)=D2WF*R_SALT_E_D(8)
!!!MSPADA
!
     &,RHO_OM  (2)=(/0.,0./)  !NOT ACTIVE
     &,R_OM_V  (2)=(/0.,0./) 
     &,R_OM_E  (2)=(/0.,0./) 
!
     &,RHO_BC  (2)=(/0.,0./)  !NOT ACTIVE
     &,R_BC_V  (2)=(/0.,0./) 
     &,R_BC_E  (2)=(/0.,0./) 
!
     &,RHO_SO4 (2)=(/0.,0./)  !NOT ACTIVE
     &,R_SO4_V (2)=(/0.,0./) 
     &,R_SO4_E (2)=(/0.,0./)

!We assume average conditions 
      REAL,PARAMETER :: T=288.0,RHO=0.8,RHO0=1.194
!
!      REAL,PARAMETER :: PI=3.1416

      REAL KB,WTAIR,AVG,RGAS,WATVISCOS,VISCOS,VISCOSK 
      REAL VTHERMG,FREEMP,CC,BRD,TAU,RE,SC,ST,SSTAR
      REAL GAMMAR
      COMMON  /CAP_R_TABLES/ CAP_EFF_RAIN(MDRmin:MDRmax,1:NUM_AERO)
      REAL RHO_AERO(1:NUM_AERO),R_AERO_V(1:NUM_AERO)
      REAL R_AERO_E(1:NUM_AERO),D_AERO(1:NUM_AERO)
      REAL VRAIN_GAMMA(MDRmin:MDRmax)
      INTEGER N,M


!-----------------------------------------------------------------------
!------- Preparations for collection efficiency of aerosol---- ---------
!-----------------------------------------------------------------------

!NOW collect or read densities and radii of each aerosol type
!ASH WILL BE READ FROM FILE (ARNAU)

      IF (NUM_AERO>0)THEN
!
        IF(NUM_ASH>0)THEN
         DO N=1,MAX(1,NUM_ASH)
          RHO_AERO(N) = 0.
          R_AERO_V(N) = 0.
          R_AERO_E(N) = 0.
         ENDDO
        ENDIF !ENDIF NUM_ASH
!
        M=NUM_ASH
!
        IF(NUM_DUST>0)THEN
         DO N=1,MAX(1,NUM_DUST)
          RHO_AERO(M+N) = RHO_DUST(N)
          R_AERO_V(M+N) = R_DUST_V(N)
          R_AERO_E(M+N) = R_DUST_E(N)
         ENDDO
        ENDIF !ENDIF NUM_DUST
!
        M=M+NUM_DUST
!
        IF(NUM_SALT>0)THEN
         DO N=1,MAX(1,NUM_SALT)
          !RHO_AERO(M+N) = RHO_SALT(N)
          RHO_AERO(M+N) = D2WF**(-3)*RHO_SALT_D(N)+(1.-D2WF**(-3))*1000.
          R_AERO_V(M+N) = D2WF*R_SALT_V_D(N)
          R_AERO_E(M+N) = D2WF*R_SALT_E_D(N)
          !R_AERO_V(M+N) = R_SALT_V(N)
          !R_AERO_E(M+N) = R_SALT_E(N)
         ENDDO
        ENDIF !ENDIF NUM_SALT
!
        M=M+NUM_SALT
!
        IF(NUM_OM>0)THEN
         DO N=1,MAX(1,NUM_OM)
          RHO_AERO(M+N) = RHO_OM(N)
          R_AERO_V(M+N) = R_OM_V(N)
          R_AERO_E(M+N) = R_OM_E(N)
         ENDDO
        ENDIF !ENDIF NUM_OM
!
        M=M+NUM_OM
!
        IF(NUM_BC>0)THEN
         DO N=1,MAX(1,NUM_BC)
          RHO_AERO(M+N) = RHO_BC(N)
          R_AERO_V(M+N) = R_BC_V(N)
          R_AERO_E(M+N) = R_BC_E(N)
         ENDDO
        ENDIF !ENDIF NUM_BC
!
       M=M+NUM_BC
!
        IF(NUM_SO4>0)THEN
         DO N=1,MAX(1,NUM_SO4)
          RHO_AERO(M+N) = RHO_SO4(N)
          R_AERO_V(M+N) = R_SO4_V(N)
          R_AERO_E(M+N) = R_SO4_E(N)
         ENDDO
        ENDIF  !ENDIF NUM_SO4
!
       ENDIF !ENDIF NUM_AERO
!
      WRITE(0,*)'*************************************'
      WRITE(0,*)' NUM_AERO =',NUM_AERO
      WRITE(0,*)' NUM_TRACERS_CHEM=',NUM_TRACERS_CHEM
      WRITE(0,*)' NUM_DUST =',NUM_DUST
      WRITE(0,*)' NUM_SALT =',NUM_SALT
      WRITE(0,*)' NUM_OM =',NUM_OM
      WRITE(0,*)' NUM_BC=',NUM_BC
      WRITE(0,*)' NUM_SO4  =',NUM_SO4
      WRITE(0,*)' RHO_AERO =',RHO_AERO
      WRITE(0,*)' R_AERO_V  =',R_AERO_V
      WRITE(0,*)' R_AERO_E =',R_AERO_E
      WRITE(0,*)'*************************************'

      KB        = 1.3806504e-23                     !: boltzmann's 1.3806504e-23 J K-1  (= kg m**2 s-2 k-1)
      WTAIR     = 28.966e-3                         !: molecular weight of air (kg mol-1)
      AVG       = 6.02252e+23                       !: avogadro's number (molec. mol-1)
      RGAS      = 8.314472                          !: gas constant (= 8.314472 J K-1 mol-1 =  kg m**2 K-1 mol-1)
      WATVISCOS = 1.e-3                             !: water viscosity (kg/m/s)
      VISCOS    = 1.718e-5                          !: dynamic viscosity of air (=1.718e-5 kg m-1 s-1)
      VISCOSK   = VISCOS / RHO                      !: kinematic viscosity = viscos / rho = (m**2 s-1)
      VTHERMG   = SQRT(8.*KB*T*AVG/(3.1416*WTAIR))  !: mean thermal velocity of air molecules (m s-1)
      FREEMP    = 2.0 * VISCOSK / VTHERMG           !: mean free path of an air molecule  = 2 x viscosk /

!!!CHEM_CP
!-----------------------------------------------------------------------
!------- Fall speeds of rain as function of rain drop diameter ---------
!-----------------------------------------------------------------------
!

      do i=IDRmin,IDRmax
        diam(i)=float(i)*DdelR
        d=100.*diam(i)         ! Diameter in cm
        if (d .le. .42) then
   !
   !--- Rutledge & Hobbs (1983); vel (m/s), d (cm)
   !
          vel(i)=max(0., -.267+51.5*d-102.25*d*d+75.7*d**3)
        else if (d.gt.0.42 .and. d.le..58) then
   !
   !--- Linear interpolation of Gunn & Kinzer (1949) data
   !
          vel(i)=8.92+.25/(.58-.42)*(d-.42)
        else
          vel(i)=9.17
        endif
      enddo
      write(8, " ( //30('*'),' NOW FOR RAIN ',30('*')//
     & ' Fall speeds (V in m/s) for individual rain drops as ',
     & 'functions of drop diameter D (mm)' ) " )
      do i=1,100
        i1=(i-1)*100+IDRmin
        i2=i1+90
   !
   !--- Print out rain fall speeds only for D<=5.8 mm (.58 cm)
   !
        if (diam(i1) .gt. .58e-2) exit
        if (print_diag) then
          write(8,"(/'D(mm)->  ',10f7.3)") (1000.*diam(j), j=i1,i2,10)
          write(8,"('V(m/s)-> ',10f7.3)") (vel(j), j=i1,i2,10)
        endif
      enddo
!
!-----------------------------------------------------------------------
!------------------- Lookup tables for rain processes ------------------
!-----------------------------------------------------------------------
!
      pi=acos(-1.)
      pi2=2.*pi
      cmass=1000.*pi/6.
      if (print_diag) then
        write(8, " ( //'----- Statistics as functions of ',
     &    'mean diameter (integrated over size distributions) ',
     &    'rain drops -----'/
     &    /'Diam - Mean diameter (mm)'
     &    /'VENTR1 - 1st ventilation coefficient (m**2)'
     &    /'VENTR2 - 2nd ventilation coefficient (m**3/s**.5)'
     &    /'ACCRR - accretion moment (m**4/s)'
     &    /'RHO*QR - mass content (kg/m**3) for N0r=8e6'
     &    /'RRATE - rain rate moment (m**5/s)'
     &    /'VR - mass-weighted rain fall speed (m/s)'
     &    /' Diam      VENTR1      VENTR2       ACCRR      ',
     &    'RHO*QR       RRATE    VR         B1         B2         ',
     &    'B3         B4      B5      B6      B7      ',
     &    'B8' ) " )
      endif
      do j=MDRmin,MDRmax
        mdiam=float(j)*BsDMR
        VENTR2(J)=0.
        ACCRR(J)=0.
        MASSR(J)=0.
        RRATE(J)=0.
        do i=IDRmin,IDRmax
          expf=exp(-diam(i)/mdiam)*DdelR
          VENTR2(J)=VENTR2(J)+diam(i)**1.5*vel(i)**.5*expf
          ACCRR(J)=ACCRR(J)+diam(i)*diam(i)*vel(i)*expf
          MASSR(J)=MASSR(J)+diam(i)**3*expf
          RRATE(J)=RRATE(J)+diam(i)**3*vel(i)*expf
        enddo
   !
   !--- Derived based on ventilation, F(D)=0.78+.31*Schmidt**(1/3)*Reynold**.5,
   !      where Reynold=(V*D*rho/dyn_vis), V is velocity, D is particle diameter,
   !      rho is air density, & dyn_vis is dynamic viscosity.  Only terms 
   !      containing velocity & diameter are retained in these tables.  
   !
        VENTR1(J)=.78*pi2*mdiam**2
        VENTR2(J)=.31*pi2*VENTR2(J)
   !
        MASSR(J)=cmass*MASSR(J)
        RRATE(J)=cmass*RRATE(J)
        VRAIN(J)=RRATE(J)/MASSR(J)

!!!CHEM_CP: CALCULATE COLLECTION EFFICIENCY TABLES WITH SLINN's formulas for rain
! V_AERO      : Terminal velocity of particles (m/s) (neglect)
! WATVISCOS   : water viscosity (kg/m/s)
! RE          : Reynolds number of raindrop
! ST          : Strokes parameter of collected particle
! SSTAR       : Critical Strokes number
! TAU         : Characteristic relaxation time of particle (s)
! RHO_AERO    : Density of particles (kg/m3)
! CC          : Cunningham Slip Factor
! SC          : Schmidt number for collected particle
! BRD         : Brownian diffusivity of particle in air (m2/s)

!--- Air resistance for rain fall speed (Beard, 1985, JAS, p.470)
!
        GAMMAR=(RHO0/RHO)**.4       !!!WE ASSUME CONSTANT RHO

        VRAIN_GAMMA(J)=GAMMAR*VRAIN(J)

        DO N=1,NUM_AERO  !!loop on aerosol

        D_AERO(N)=R_AERO_E(N)*2.

        CC=1.+2.*FREEMP*(1.257+0.4*EXP(-0.55*D_AERO(N)/FREEMP))
     &     /D_AERO(N)
        BRD=KB*T*CC/(3*PI*VISCOS*D_AERO(N))
        TAU=RHO_AERO(N)*CC*(D_AERO(N)**2.)/(18.*VISCOS)
        RE=MDIAM*VRAIN_GAMMA(J)*RHO/(2.*VISCOS)
        SC=VISCOS/(RHO*BRD)
        V_AERO=9.8*TAU                     
        ST=2.*TAU*(VRAIN_GAMMA(J)-V_AERO)/MDIAM
!        ST=2.*TAU*VRAIN_GAMMA(J)/MDIAM
        SSTAR=(1.2+(1./12.)*LOG(1.+RE))/(1.+ LOG(1.+RE))
        CAP_EFF_RAIN(J,N)=(4./(RE*SC))*(1.+0.4*RE**0.5*SC**(1./3.)     !!Brownian Diffusion
     &               + 0.16*RE**0.5*SC**0.5)                           !!Brownian Diffusion
     &               + 4.*(D_AERO(N)/MDIAM)*(VISCOS/WATVISCOS          !!Directional Interception
     &               +(1.+2.*RE**0.5)*(D_AERO(N)/MDIAM))               !!Directional Interception

        IF(ST>SSTAR)THEN
        CAP_EFF_RAIN(J,N)=CAP_EFF_RAIN(J,N)
     &                   +((ST-SSTAR)/(ST-SSTAR+2./3.))**1.5   !!Inertial Impaction
        ENDIF

        IF (CAP_EFF_RAIN(J,N)>1.) CAP_EFF_RAIN(J,N)=1.

        ENDDO

!!!CHEM_CP

        if(print_diag) write(8,"(f5.3,5g12.5,f6.3,8g11.5))")1000.*mdiam,
     &    ventr1(j),ventr2(j),accrr(j),8.e6*massr(j),rrate(j),vrain(j)
     &    ,CAP_EFF_RAIN(J,1),CAP_EFF_RAIN(J,2),CAP_EFF_RAIN(J,3)
     &    ,CAP_EFF_RAIN(J,4),CAP_EFF_RAIN(J,5),CAP_EFF_RAIN(J,6)
     &    ,CAP_EFF_RAIN(J,7),CAP_EFF_RAIN(J,8)
!!!MSPADA DEBUG
        write(8,*) "porcodio ",CAP_EFF_RAIN(J,9),CAP_EFF_RAIN(J,10)
      enddo
!
!-----------------------------------------------------------------------
!
      return
      end
!
!#######################################################################
!--- Sets up lookup table for calculating initial ice crystal growth ---
!#######################################################################
!
      SUBROUTINE MY_GROWTH_RATES (DTPH)
!
!--- Below are tabulated values for the predicted mass of ice crystals
!    after 600 s of growth in water saturated conditions, based on 
!    calculations from Miller and Young (JAS, 1979).  These values are
!    crudely estimated from tabulated curves at 600 s from Fig. 6.9 of
!    Young (1993).  Values at temperatures colder than -27C were 
!    assumed to be invariant with temperature.  
!
!--- Used to normalize Miller & Young (1979) calculations of ice growth
!    over large time steps using their tabulated values at 600 s.
!    Assumes 3D growth with time**1.5 following eq. (6.3) in Young (1993).
!
      integer, parameter :: MY_T1=1, MY_T2=35
      COMMON /CMY600/ MY_GROWTH(MY_T1:MY_T2)
      REAL MY_GROWTH, MY_600(MY_T1:MY_T2)
!
      DATA MY_600 /
     & 5.5e-8, 1.4E-7, 2.8E-7, 6.E-7, 3.3E-6,     !  -1 to  -5 deg C
     & 2.E-6, 9.E-7, 8.8E-7, 8.2E-7, 9.4e-7,      !  -6 to -10 deg C
     & 1.2E-6, 1.85E-6, 5.5E-6, 1.5E-5, 1.7E-5,   ! -11 to -15 deg C
     & 1.5E-5, 1.E-5, 3.4E-6, 1.85E-6, 1.35E-6,   ! -16 to -20 deg C
     & 1.05E-6, 1.E-6, 9.5E-7, 9.0E-7, 9.5E-7,    ! -21 to -25 deg C
     & 9.5E-7, 9.E-7, 9.E-7, 9.E-7, 9.E-7,        ! -26 to -30 deg C
     & 9.E-7, 9.E-7, 9.E-7, 9.E-7, 9.E-7 /        ! -31 to -35 deg C
!
!-----------------------------------------------------------------------
!
      DT_ICE=(DTPH/600.)**1.5
      MY_GROWTH=DT_ICE*MY_600
!
!-----------------------------------------------------------------------
!
      RETURN
      END

