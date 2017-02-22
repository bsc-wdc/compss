#!/bin/bash
#set -x

ulimit -s unlimited

#-----------------------------------------------------------------------
# MAIN NMMB-DUST RUN SCRIPT - Define MN settings first
#-----------------------------------------------------------------------
INPES=06                      # Number inpes    
JNPES=10                      # Number jnpes   
WRTSK=04                      # Number write tasks  
#-----------------------------------------------------------------------
# Global-regional switch - Model domain setup global/regional
#-----------------------------------------------------------------------
DOMAIN=0                      # GLOBAL/REGIONAL (0/1)
LM=24                         # Vertical model layers
CASE=GLOB                     # Name of the case 
#----------
# If regional you need to modify manually files llgrid_chem.inc in vrbl409rrtm_bsc1.0_reg
#-----------------------------------------------------------------------
DT_INT1=180                   # Run time step (integer seconds) !180
TLM0D1=0.0                    # Center point longitudinal (E/W)
TPH0D1=0.0                    # Center point latitudinal (S/N)
WBD1=-180.0                   # Western boundary (from center point)
SBD1=-90.0                    # Southern boundary (from center point)
DLMD1=1.40625                 # Longitudinal grid resolution
DPHD1=1.0                     # Latitudinal grid resolution
PTOP1=100.                    # Pressure top of the domain (Pa)
DCAL1=0.768                   # Mineral Dust Emission Calibration Factor
NRADS1=20                     # Number of timesteps between radiation calls (short)
NRADL1=20                     # Number of timesteps between radiation calls (long)
#-----------------------------------------------------------------------
DT_INT2=60                    # regional           
TLM0D2=20.0                   # regional           
TPH0D2=35.0                   # regional           
WBD2=-51.0                    # regional          
SBD2=-35.0                    # regional          
DLMD2=0.30                    # regional            
DPHD2=0.30                    # regional            
PTOP2=5000.                    # Pressure top of the domain (Pa)
DCAL2=0.255                   # Mineral Dust Emission Calibration Factor
NRADS2=60                      # Number of timesteps between radiation calls (short)
NRADL2=60                      # Number of timesteps between radiation calls (long)
#-----------------------------------------------------------------------
# Case selection
#-----------------------------------------------------------------------
DO_FIXED=0                    # RUN FIXED (0/1) 1
DO_VRBL=0                     # RUN VRBL (0/1) 1
DO_UMO=0                      # RUN UMO (0/1) 0
DO_POST=1                     # RUN POST_CARBONO (0/1)
#-----------------------------------------------------------------------
# Select START and ENDING Times
#-----------------------------------------------------------------------
START=20140901                # First day of simulation
END=20140901                  # Last day of simulation (if just one day --> START = END)
HOUR=00                       # Choose the time of initial input data (hours)
NHOURS=24                     # Length of the forecast (hours)
NHOURS_INIT=24                # History init previous day timestep
HIST=3                        # Frequency of history output (up to 552 hours)
HIST_M=180                    # Frequency of history output (in minutes)
BOCO=6                        # Frequency of boundary condition update for REGIONAL (hours)
TYPE_GFSINIT="FNL"            # FNL or GFS
#-----------------------------------------------------------------------
# Select configuration of POSTPROC (DO_POST) 
#-----------------------------------------------------------------------
HOUR_P=00                     # Choose the time of initial output data (hours)
NHOURS_P=24                   # Length of postproc data (hours)
HIST_P=3                      # Frequency of history output (hours)
LSM=15                        # Output Layers
#-----------------------------------------------------------------------
# Select IC of chemistry for run with COUPLE_DUST_INIT=0
#-----------------------------------------------------------------------
INIT_CHEM=0                   # 0. IC from ideal conditions 2. from inca 3.from global nmmb-ctm for regional
#-----------------------------------------------------------------------
# Couple dust
#-----------------------------------------------------------------------
COUPLE_DUST=1                 # Couple dust for the next run (0/1)
COUPLE_DUST_INIT=0            # Couple dust from the beginning (0/1)
#-----------------------------------------------------------------------
# END USER MODIFICATION SECTION
# HANDS OFF FROM SETTING SECTION BELOW !!!
#-----------------------------------------------------------------------

export PATH=$PATH:/gpfs/projects/bsc19/bsc19533/NMMB-BSC/nmmb-bsc-ctm-v2.0/MODEL/exe

#-----------------------------------------------------------------------
# put the necessary paths here (CHANGE THE UMO_PATH WITH YOUR OWN)
#-----------------------------------------------------------------------
export UMO_PATH=/gpfs/projects/bsc19/bsc19533/NMMB-BSC/nmmb-bsc-ctm-v2.0
export FIX=$UMO_PATH/PREPROC/FIXED
export VRB=$UMO_PATH/PREPROC/VARIABLE
export OUTPUT=$UMO_PATH/PREPROC/output
export UMO_ROOT=$UMO_PATH/JOB
export SRCDIR=$UMO_PATH/MODEL
export UMO_OUT=$UMO_PATH/OUTPUT/CURRENT_RUN
export POST_CARBONO=$UMO_PATH/POSTPROC
export GRB=$UMO_PATH/DATA/INITIAL
export DATMOD=$UMO_PATH/DATA/STATIC
export CHEMIC=
export STE=
export OUTNMMB=$UMO_PATH/OUTPUT
export OUTGCHEM=
export PREMEGAN=
export TMP=/tmp

export FNL=$GRB
export GFS=$GRB

#-----------------------------------------------------------------
# Clean output folder
#-----------------------------------------------------------------
rm -r $UMO_OUT/*

#-----------------------------------------------------------------
# MN settings in run_rrtm.cmd
#-----------------------------------------------------------------

PROC=$(echo "scale=0 ; $INPES*$JNPES+$WRTSK" | bc)

echo "Number of processors" $PROC

if [ $DOMAIN -eq 0 ] ; then
   WBD=$WBD1
   SBD=$SBD1
   DLMD=$DLMD1
   DPHD=$DPHD1
elif [ $DOMAIN -eq 1 ] ; then
   WBD=$WBD2
   SBD=$SBD2
   DLMD=$DLMD2
   DPHD=$DPHD2
fi

IMI=$(echo "scale=1 ; -2.0*$WBD/$DLMD+1.5" | bc)
JMI=$(echo "scale=1 ; -2.0*$SBD/$DPHD+1.5" | bc)
IMI=${IMI/.*}
JMI=${JMI/.*}

if [ $DOMAIN -eq 0 ] ; then
   let IM=$IMI+2
   let JM=$JMI+2
elif [ $DOMAIN -eq 1 ] ; then
   IM=$IMI
   JM=$JMI
fi

echo " "
echo "Model grid size - IM/JM/LM: " $IMI " / " $JMI " / " $LM
echo "Extended domain - IM/JM/LM: " $IM " / " $JM " / " $LM
echo " "

let HOURCTL=$HOUR+$HIST
HOUR=`printf "%.2d" $HOUR`
HOURCTL=`printf "%.2d" $HOURCTL`

#-----------------------------------------------------------------
# Data conversion
#-----------------------------------------------------------------

DATE=$START

YEAR=`date -ud "$DATE" +%Y`
YY=`date -ud "$DATE" +%y`
MM=`date -ud "$DATE" +%m`
DD=`date -ud "$DATE" +%d`
MTH=`date -ud "$DATE" +%b`

DATENEW=$YY$MM$DD$HOUR
DATENEW2=$YY$MM$DD
COUPLE_TMP=$COUPLE_DUST
HOURNEW="t"$HOUR"z"

#-----------------------------------------------------------------
# Define and create output folder by case
#-----------------------------------------------------------------
mkdir -p $UMO_PATH/OUTPUT/$CASE/
mkdir -p $UMO_PATH/OUTPUT/$CASE/output 

cd $UMO_PATH/PREPROC/
ln -sf ../OUTPUT/$CASE/output/

#-----------------------------------------------------------------
#   Fixed process (do before main time looping)
#-----------------------------------------------------------------

if [ $DO_FIXED -eq 1 ] ; then
   echo "Enter Fixed process"
   cd $FIX
   rm -f $OUTPUT/*

############ prepare model grid setup files global ###############

   if [ $DOMAIN -eq 0 ] ; then

      rm modelgrid.inc
      rm lmimjm.inc
      sed -e  "s/TLMD/$TLM0D1/" \
          -e  "s/TPHD/$TPH0D1/" \
          -e  "s/WBDN/$WBD1/" \
          -e  "s/SBDN/$SBD1/" \
          -e  "s/DLMN/$DLMD1/" \
          -e  "s/DPHN/$DPHD1/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP1/" \
          -e  "s/KKK/$LM/"  modelgrid_rrtm.tmp > modelgrid.inc
      sed -e  "s/TLMD/$TLM0D1/" \
          -e  "s/TPHD/$TPH0D1/" \
          -e  "s/WBDN/$WBD1/" \
          -e  "s/SBDN/$SBD1/" \
          -e  "s/DLMN/$DLMD1/" \
          -e  "s/DPHN/$DPHD1/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP1/" \
          -e  "s/KKK/$LM/"  lmimjm_rrtm.tmp > lmimjm.inc

############ run global job  #####################################

      ./runfixed_rrtm.scr

############ prepare model grid setup files regional #############

   elif [ $DOMAIN -eq 1 ] ; then

      rm modelgrid.inc
      rm lmimjm.inc
      sed -e  "s/TLMD/$TLM0D2/" \
          -e  "s/TPHD/$TPH0D2/" \
          -e  "s/WBDN/$WBD2/" \
          -e  "s/SBDN/$SBD2/" \
          -e  "s/DLMN/$DLMD2/" \
          -e  "s/DPHN/$DPHD2/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP2/" \
          -e  "s/KKK/$LM/"  modelgrid_rrtm.tmp > modelgrid.inc
      sed -e  "s/TLMD/$TLM0D2/" \
          -e  "s/TPHD/$TPH0D2/" \
          -e  "s/WBDN/$WBD2/" \
          -e  "s/SBDN/$SBD2/" \
          -e  "s/DLMN/$DLMD2/" \
          -e  "s/DPHN/$DPHD2/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP2/" \
          -e  "s/KKK/$LM/"  lmimjm_rrtm.tmp > lmimjm.inc

############ submit regional job to MN ###########################

      ./runfixed_rrtm.scr

   fi

   echo "Fixed process finished"

fi

#-----------------------------------------------------------------
# Start main time loop
#-----------------------------------------------------------------
while [ $DATE -le $END ]; do

echo "DATE: " $DATE "simulation started"

#-----------------------------------------------------------------
# Define model output folder by case and date
#-----------------------------------------------------------------
FOLDER_OUTPUT_CASE=$OUTNMMB/$CASE
FOLDER_OUTPUT=$OUTNMMB/$CASE/$DATE$HOUR

#-----------------------------------------------------------------
#   Vrbl process
#-----------------------------------------------------------------

if [ $DO_VRBL -eq 1 ] ; then

   echo "Enter Vrbl process"

   cd $OUTPUT
   rm *.gfs
   rm gfs.*
   rm sst2dvar_grb_0.5
   rm fcst
   rm boco.*
   rm boco_chem.*
   rm $VRB/sstgrb
   rm llstmp
   rm llsmst
   rm llgsno
   rm llgcic
   rm llgsst
   rm llspl.000
   rm llgsst05
   rm albedo
   rm albase
   rm vegfrac
   rm z0base
   rm z0
   rm ustar
   rm sst05
   rm dzsoil
   rm tskin
   rm sst
   rm snow
   rm snowheight
   rm cice
   rm seamaskcorr
   rm landusecorr
   rm landusenewcorr
   rm topsoiltypecorr
   rm vegfraccorr
   rm z0corr
   rm z0basecorr
   rm emissivity
   rm canopywater
   rm frozenprecratio
   rm smst
   rm sh2o
   rm stmp

   BOCOS=`printf "%.2d" $BOCO`

   echo "-------------------"
   echo "execute vrbl script: prep_rrtm"
   echo "-------------------"
   
   cd $VRB

############ prepare input grid setup file #######################

   rm llgrid.inc

   sed -e  "s/LLL/$NHOURS/" \
       -e  "s/HH/$HOUR/" \
       -e  "s/UPBD/$BOCO/" \
       -e  "s/YYYYMMDD/$DATENEW2/" llgrid_rrtm_$TYPE_GFSINIT.tmp > llgrid.inc 

############ prepare global job  #################################

   if [ $DOMAIN -eq 0 ] ; then

      if [ $TYPE_GFSINIT == "FNL" ] ; then
        day=`date -d "${DATE}" +%Y%m%d`
        ln -s $FNL/fnl_${DATE}_${HOUR}_00 $OUTPUT/gfs.t${HOUR}z.pgrbf00
      else
	day_start=`date -d "${DATE}" +%y%m%d`
        echo "Converting wafs.00.0P5DEG from grib2 to grib1"
        cnvgrib -g21 $GFS/wafs.00.0P5DEG $OUTPUT/gfs.t${HOUR}z.pgrbf00
      fi

      let UPGLOB=$HOUR+3

      UPGLOB=`printf "%.2d" $UPGLOB`

      sed -e  "s/NN/$HOUR/" \
          -e  "s/EE/00/" \
          -e  "s/UP/$BOCOS/ "  prep_rrtm.tmp > prep_rrtm

      chmod 755 prep_rrtm

      ############ prepare model grid setup files global ###############

      rm modelgrid.inc
      rm lmimjm.inc
      sed -e  "s/TLMD/$TLM0D1/" \
          -e  "s/TPHD/$TPH0D1/" \
          -e  "s/WBDN/$WBD1/" \
          -e  "s/SBDN/$SBD1/" \
          -e  "s/DLMN/$DLMD1/" \
          -e  "s/DPHN/$DPHD1/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP1/" \
          -e  "s/KKK/$LM/"  modelgrid_rrtm.tmp > modelgrid.inc
      sed -e  "s/TLMD/$TLM0D1/" \
          -e  "s/TPHD/$TPH0D1/" \
          -e  "s/WBDN/$WBD1/" \
          -e  "s/SBDN/$SBD1/" \
          -e  "s/DLMN/$DLMD1/" \
          -e  "s/DPHN/$DPHD1/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP1/" \
          -e  "s/KKK/$LM/"  lmimjm_rrtm.tmp > lmimjm.inc

      ############ Running Variable Global Script  ###########################
      ./prep_rrtm

   ############ prepare regional job  ###############################

   elif [ $DOMAIN -eq 1 ] ; then

    #rm -f $OUTPUT/GRG_$DATE'_EU_AQ.nc'
    #ln -s $OUTGCHEM/GRG_$DATE'_EU_AQ.nc' $OUTPUT/GRG_$DATE'_EU_AQ.nc'

	if [ $TYPE_GFSINIT == "FNL" ] ; then
	
		for i in $(seq $HOUR $BOCO $NHOURS)
		do
	    		var=`printf "%.2d" $i`
	    		hday=`printf "%.2d" $(($var % 24))`
	    		sum_day=$(($var / 24))
	    		day=`date -d "${DATE} ${sum_day} days" +%Y%m%d`
	    		ln -s $FNL/fnl_${day}_${hday}_00 $OUTPUT/gfs.t${HOUR}z.pgrbf${var}
		done

	else

		day_start=`date -d "${DATE}" +%y%m%d`

		for i in $(seq 00 $BOCO $NHOURS)
		do
	    		var=`printf "%.2d" $i`
			echo "Converting wafs.${var}.0P5DEG from grib2 to grib1"
	    		cnvgrib -g21 $GFS/wafs.${var}.0P5DEG $OUTPUT/gfs.t${HOUR}z.pgrbf${var}
		done

	fi

      sed -e  "s/NN/$HOUR/" \
          -e  "s/EE/$NHOURS/" \
          -e  "s/UP/$BOCOS/ "  prep_rrtm.tmp > prep_rrtm

      chmod 755 prep_rrtm

      ############ prepare model grid setup files regional #############

      rm modelgrid.inc
      rm lmimjm.inc
      sed -e  "s/TLMD/$TLM0D2/" \
          -e  "s/TPHD/$TPH0D2/" \
          -e  "s/WBDN/$WBD2/" \
          -e  "s/SBDN/$SBD2/" \
          -e  "s/DLMN/$DLMD2/" \
          -e  "s/DPHN/$DPHD2/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP2/" \
          -e  "s/KKK/$LM/"  modelgrid_rrtm.tmp > modelgrid.inc
      sed -e  "s/TLMD/$TLM0D2/" \
          -e  "s/TPHD/$TPH0D2/" \
          -e  "s/WBDN/$WBD2/" \
          -e  "s/SBDN/$SBD2/" \
          -e  "s/DLMN/$DLMD2/" \
          -e  "s/DPHN/$DPHD2/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/IBDY/$IM/" \
          -e  "s/JBDY/$JM/" \
          -e  "s/PTOP/$PTOP2/" \
          -e  "s/KKK/$LM/"  lmimjm_rrtm.tmp > lmimjm.inc

      ############ Running Variable Regional Script  ###########################
      ./prep_rrtm

   fi

   #We need for postprocess
   cp lmimjm.inc $FOLDER_OUTPUT_CASE/

   echo "Vrbl process finished"
fi

#-----------------------------------------------------------------
#   UMO model run
#-----------------------------------------------------------------

if [ $DO_UMO -eq 1 ] ; then

   echo "Enter UMO model run"

   cd $UMO_OUT

   rm isop.dat
   rm lai*.dat
   rm meteo-data.dat
   rm pftp_*.dat

   cp $CHEMIC/MEGAN/out/aqmeii-reg/isop.dat .
   cp $CHEMIC/MEGAN/out/aqmeii-reg/lai*.dat .
   cp $CHEMIC/MEGAN/out/aqmeii-reg/meteo-data.dat .
   cp $CHEMIC/MEGAN/out/aqmeii-reg/pftp_*.dat .

   rm PET*txt
   rm PET*File
   rm boco.*
   rm boco_chem.*
   rm chemic-reg
   rm nmm_b_history.*
   rm main_input_filename
   rm main_input_filename2
   rm GWD.bin
   rm configure_file
   rm tr*
   rm co2_trans
   rm ETAMPNEW_AERO
   rm ETAMPNEW_DATA
   rm RRT*
   rm *.TBL

   # Clean previous runs
   rm fcstdone.* restartdone.*
   rm nmmb_rst_* nmmb_hst_*

   cd $UMO_ROOT

   #-----------------------------------------------------------------
   #   Dust coupling part 1
   #-----------------------------------------------------------------

   if [ $DATE -eq $START ] ; then
       COUPLE_DUST=0
   else
       COUPLE_DUST=$COUPLE_TMP
   fi

   if [ $COUPLE_DUST_INIT -eq 1 ] ; then
       COUPLE_DUST=$COUPLE_DUST_INIT
   fi

   echo "couple_dust" $COUPLE_DUST
   echo "domain" $DOMAIN

   if [ $COUPLE_DUST -eq 1 ] ; then
      DUST_FLAG="EEEE/true"
   else
      DUST_FLAG="EEEE/false"
   fi     
 
   if [ $DOMAIN -eq 0 ] ; then
        sed -e  "s/III/$IMI/" \
            -e  "s/JJJ/$JMI/" \
            -e  "s/KKK/$LM/" \
            -e  "s/TPHD/$TPH0D1/" \
            -e  "s/TLMD/$TLM0D1/" \
            -e  "s/WBD/$WBD1/" \
            -e  "s/SBD/$SBD1/" \
            -e  "s/INPES/$INPES/" \
            -e  "s/JNPES/$JNPES/" \
            -e  "s/WRTSK/$WRTSK/" \
            -e  "s/DTINT/$DT_INT1/" \
            -e  "s/YYYY/$YEAR/" \
            -e  "s/MM/$MM/" \
            -e  "s/DD/$DD/" \
            -e  "s/HH/$HOUR/" \
            -e  "s/LLL/$NHOURS/" \
            -e  "s/STT/$HIST_M/" \
            -e  "s/DOM/true/" \
            -e  "s/$DUST_FLAG/" \
            -e  "s/BBBB/$DCAL1/" \
            -e  "s/NRADS/$NRADS1/" \
            -e  "s/NRADL/$NRADL1/" \
            -e  "s/CCCC/$INIT_CHEM/" configfile_rrtm_chem.tmp > configfile_rrtm_chem
    elif [ $DOMAIN -eq 1 ] ; then
        sed -e  "s/III/$IMI/" \
            -e  "s/JJJ/$JMI/" \
            -e  "s/KKK/$LM/" \
            -e  "s/TPHD/$TPH0D2/" \
            -e  "s/TLMD/$TLM0D2/" \
            -e  "s/WBD/$WBD2/" \
            -e  "s/SBD/$SBD2/" \
            -e  "s/INPES/$INPES/" \
            -e  "s/JNPES/$JNPES/" \
            -e  "s/WRTSK/$WRTSK/" \
            -e  "s/DTINT/$DT_INT2/" \
            -e  "s/YYYY/$YEAR/" \
            -e  "s/MM/$MM/" \
            -e  "s/DD/$DD/" \
            -e  "s/HH/$HOUR/" \
            -e  "s/LLL/$NHOURS/" \
            -e  "s/STT/$HIST_M/" \
            -e  "s/DOM/false/" \
            -e  "s/$DUST_FLAG/" \
            -e  "s/BBBB/$DCAL2/" \
            -e  "s/NRADS/$NRADS2/" \
            -e  "s/NRADL/$NRADL2/" \
            -e  "s/CCCC/$INIT_CHEM/" configfile_rrtm_chem_reg.tmp > configfile_rrtm_chem
   fi
   
   echo ""
   echo "Executing nmmb_esmf.x UMO-NMMb-DUST-RRTM model"
   echo ""

   cd $UMO_OUT 

   echo $OUTPUT
   cp $OUTPUT/boco.* .
   cp $OUTPUT/boco_chem.* .
   cp $OUTPUT/chemic-reg .
   cp $OUTPUT/fcst input_domain_01 #main_input_filename
   cp $OUTPUT/soildust main_input_filename2
   cp $OUTPUT/GWD.bin .
   mv $UMO_ROOT/configfile_rrtm_chem configure_file

   cp $DATMOD/* .
   cp $DATMOD/nam_micro_lookup.dat ETAMPNEW_DATA
   cp $DATMOD/wrftables/* .
   cp $DATMOD/co2data/* .

   # for aerosols scavenging coeff
   cp $OUTPUT/lookup_aerosol2.dat.rh00 ETAMPNEW_AERO_RH00
   cp $OUTPUT/lookup_aerosol2.dat.rh50 ETAMPNEW_AERO_RH50
   cp $OUTPUT/lookup_aerosol2.dat.rh70 ETAMPNEW_AERO_RH70
   cp $OUTPUT/lookup_aerosol2.dat.rh80 ETAMPNEW_AERO_RH80
   cp $OUTPUT/lookup_aerosol2.dat.rh90 ETAMPNEW_AERO_RH90
   cp $OUTPUT/lookup_aerosol2.dat.rh95 ETAMPNEW_AERO_RH95
   cp $OUTPUT/lookup_aerosol2.dat.rh99 ETAMPNEW_AERO_RH99

   # for rrtm radiation
   cp $DATMOD/fix/fix_rad/global_climaeropac_global.txt aerosol.dat
   cp $DATMOD/fix/fix_rad/co2historicaldata* .
   cp $DATMOD/fix/fix_rad/solarconstantdata.txt .
   cp $DATMOD/fix/fix_rad/volcanic_aerosols_* .

   # for gocart climatology conc. and opt. properties
   cp $DATMOD/fix/fix_gocart_clim/2000* .
   cp $DATMOD/fix/fix_aeropt_luts/NCEP_AEROSOL.bin .

   # for chemistry tests
   cp /gpfs/projects/bsc32/bsc32771/MN3/NMMB/RUN/FUKU-DATA/xe133_emissions.dat .

   cp configure_file configure_file_01
   cp configure_file model_configure

   ln -sf $DATMOD/global_o3prdlos.f77 fort.28
   ln -sf $DATMOD/global_o3clim.txt fort.48

   cp $SRCDIR/NAMELISTS/solver_state.txt .
   cp $SRCDIR/NAMELISTS/atmos.configure atmos.configure
   cp $SRCDIR/NAMELISTS/ocean.configure ocean.configure

   # Coupling previous day
   cp $FOLDER_OUTPUT_CASE/history_INIT.hhh .

   date
   time mpirun $SRCDIR/exe/NEMS.x  
   date

   echo ""
   echo "Finished Executing nmmb_esmf.x UMO-NMMb-DUST-RRTM model"
   echo ""

   #-----------------------------------------------------------------
   # Dust coupling part 2
   #-----------------------------------------------------------------

   rm $UMO_OUT/history_INIT.hhh

   mkdir -p $FOLDER_OUTPUT

   if [ $COUPLE_TMP -eq 1 ] ; then
      if [ $NHOURS_INIT -lt 100 ] ; then
         if [ $NHOURS_INIT -lt 10 ] ; then
            cp $UMO_OUT/nmmb_hst_01_bin_000${NHOURS_INIT}h_00m_00.00s $FOLDER_OUTPUT_CASE/history_INIT.hhh 
         else
            cp $UMO_OUT/nmmb_hst_01_bin_00${NHOURS_INIT}h_00m_00.00s $FOLDER_OUTPUT_CASE/history_INIT.hhh
         fi
      else
         cp $UMO_OUT/nmmb_hst_01_bin_0${NHOURS_INIT}h_00m_00.00s $FOLDER_OUTPUT_CASE/history_INIT.hhh
      fi
   fi

   mv $UMO_OUT/nmmb_hst_01_bin_* $FOLDER_OUTPUT/.
   mv $UMO_OUT/nmm_rrtm.out $FOLDER_OUTPUT/.
   mv $UMO_OUT/configure_file $FOLDER_OUTPUT/.
   mv $OUTPUT/boundary_ecmwf.nc $FOLDER_OUTPUT/.

fi

if [ $DO_POST -eq 1 ] ; then

   echo "Postproc_carbono process for DAY:" $DATE

   DATE_POST=`date -d "$DATE" +"%d%b%Y"`
   XHRT=`printf "%.1d" $HIST_P`
   TDEFT=$(echo "scale=0 ; $NHOURS_P/$HIST+1" | bc)
   TDEF=`printf "%.2d" $TDEFT`

   cd $POST_CARBONO

   cp $FOLDER_OUTPUT_CASE/lmimjm.inc .

   sed -e "s/QQQ/$NHOURS_P/" \
       -e "s/SSS/$HOUR_P/" \
       -e "s/TTT/$HIST_P/" new_postall.f.tmp > new_postall.f

   sed -e "s/YYYYMMDD/$DATE/" run-postproc_auth.sh.tmp > run-postproc_auth.sh

   chmod u+x run-postproc_auth.sh

   if [ $DOMAIN -eq 0 ] ; then

   sed -e  "s/DATE/${DATE}${HOUR}/" \
       -e  "s/III/$IMI/" \
       -e  "s/WBDN/$WBD1/" \
       -e  "s/DLMN/$DLMD1/" \
       -e  "s/JJJ/$JMI/" \
       -e  "s/SBDN/$SBD1/" \
       -e  "s/DPHN/$DPHD1/" \
       -e  "s/KKK/$LSM/" \
       -e  "s/HH/$TDEF/" \
       -e  "s/INITCTL/${HOUR}Z${DATE_POST}/" \
       -e  "s/XHR/${XHRT}hr/" pout_global_pressure.ctl.tmp > pout_global_pressure_${DATE}${HOUR}.ctl

     mv pout_global_pressure_${DATE}${HOUR}.ctl $FOLDER_OUTPUT/

     ./run-postproc_auth.sh $FOLDER_OUTPUT glob ${DATE}${HOUR}

   elif [ $DOMAIN -eq 1 ] ; then

      TPH0DN=$(echo "scale=1 ; $TPH0D2+90.0" | bc)
      WBDDEF=$(echo "scale=1 ; $WBD2+$TLM0D2" | bc)
      SBDDEF=$(echo "scale=1 ; $SBD2+$TPH0D2" | bc)

      let IREG=$IMI-2
      let JREG=$JMI-2

      sed -e  "s/DATE/${DATE}${HOUR}/" \
          -e  "s/IRG/$IREG/" \
          -e  "s/JRG/$JREG/" \
          -e  "s/TLMN/$TLM0D2/" \
          -e  "s/TPHN/$TPH0DN/" \
          -e  "s/DLMN/$DLMD2/" \
          -e  "s/DPHN/$DPHD2/" \
          -e  "s/WBDN/$WBD2/" \
          -e  "s/SBDN/$SBD2/" \
          -e  "s/III/$IMI/" \
          -e  "s/JJJ/$JMI/" \
          -e  "s/WBXX/$WBDDEF/" \
          -e  "s/SBYY/$SBDDEF/" \
          -e  "s/KKK/$LSM/" \
          -e  "s/HH/$TDEF/" \
          -e  "s/INITCTL/${HOUR}Z${DATE_POST}/" \
          -e  "s/XHR/${XHRT}hr/" pout_regional_pressure.ctl.tmp > pout_regional_pressure_${DATE}${HOUR}.ctl

     mv pout_regional_pressure_${DATE}${HOUR}.ctl $FOLDER_OUTPUT/
     
     ./run-postproc_auth.sh $FOLDER_OUTPUT reg ${DATE}${HOUR}

   fi

   rm lmimjm.inc
   rm new_postall.f
   rm run-postproc_auth.sh

   echo "Finished Postproc_Carbono"
fi

echo "DATE: " $DATE "simulation finished"

#Getting next simulation day
YEAR=`date -ud "$DATE + next day" +%Y`
YY=`date -ud "$DATE + next day" +%y`
MM=`date -ud "$DATE + next day" +%m`
DD=`date -ud "$DATE + next day" +%d`
MTH=`date -ud "$DATE" +%b`
DATE=$YEAR$MM$DD
DATENEW=$YY$MM$DD$HOUR
DATENEW2=$YY$MM$DD

done

exit
