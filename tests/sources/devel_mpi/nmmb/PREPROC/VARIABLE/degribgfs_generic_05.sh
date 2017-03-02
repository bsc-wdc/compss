#!/bin/bash 

START=`pwd`
OUTDIR='../output'

# READ PARAMS
hr=t${1}z 		# t00z - GFS INIT TIME
endtime=${2}		# 00 
int=${3}		# 03
model=gfs
type2=${4}
tval=00
folder=${5}

# CLEAN PREVIOUS FILES
cd $folder
rm -f *.dump
rm -f *.gfs

cd $START
rm -f gfsconv.out
gfortran -O gfs2model_rrtm.f -o gfs2model_rrtm.x

echo "Read hours from $tval to $endtime"

while [ $tval -le $endtime ]
do
	echo "Processing Hour $tval"
	tvsave=$tval

	if [ $tval -lt 10 -a $tval != 00 ] 
	then
		tval=0${tval}
	else
		tval=$tval
 	fi

	ROOT=`pwd`

	filename=${model}.${hr}.${type2}${tval}
	GRIBFILE=${folder}/${filename}
	EXTEN='.dump'

	echo " ***************************************************"
	echo " **********  GRIB grid processor *******************"
	echo " ******* Dump data from $filename file ******"
	echo " ***************************************************"

	if [ ! -e $GRIBFILE ] 
	then
		echo "Missing input file $GRIBFILE"
       		exit 
	fi

	# GETTING DATE
	wgrib -s $GRIBFILE | head -1 | cut -d: -f3 | cut -d= -f2 > date.txt
	# GETTING DATA
	wgrib $GRIBFILE > content.txt	
	
	cat content.txt | while read line
    	do
		code=`echo ${line} | gawk -F":" '{print $1}'` 
  		variable=`echo ${line} | gawk -F":" '{print $4}'`
  		level=`echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}'`
		
		if [ $variable = "TMP" -a $level = "sfc" ]
		then
			echo $code $variable $level
      			echo " Dumping SST/TS from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SST_TS$EXTEN > /dev/null
  		fi
  		
		if [ $variable = "TMP" -a $level = "0-10" ] 
		then
			echo $code $variable $level
		      	echo " Dumping SOILT level 1 from gribfile "
		      	wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT1$EXTEN > /dev/null
		fi
		
  		if [ $variable = "TMP" -a $level = "10-40" ] 
  		then
  			echo $code $variable $level
      			echo " Dumping SOILT level 2 from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT2$EXTEN > /dev/null
  		fi
  		
  		if [ $variable = "TMP" -a $level = "40-100" ]
  		then
  			echo $code $variable $level
		      	echo " Dumping SOILT level 3 from gribfile "
		      	wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT3$EXTEN > /dev/null
		fi
		
		if [ $variable = "TMP" -a $level = "100-200" ] 
		then
			echo $code $variable $level
      			echo " Dumping SOILT level 4 from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT4$EXTEN > /dev/null
  		fi
  	
                if [ $variable = "TMP" -a $level = "10-200" ]
                then
                        echo $code $variable $level
                        echo " Dumping SOILT level 2 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT2$EXTEN > /dev/null
                        echo " Dumping SOILT level 3 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT3$EXTEN > /dev/null
                        echo " Dumping SOILT level 4 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILT4$EXTEN > /dev/null
                fi
	
  		if [ $variable = "SOILW" -a $level = "0-10" ] 
		then
			echo $code $variable $level
		      	echo " Dumping SOILW level 1 from gribfile "
		      	wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW1$EXTEN > /dev/null
		fi
		
  		if [ $variable = "SOILW" -a $level = "10-40" ] 
  		then
  			echo $code $variable $level
      			echo " Dumping SOILW level 2 from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW2$EXTEN > /dev/null
  		fi
  		
  		if [ $variable = "SOILW" -a $level = "40-100" ]
  		then
  			echo $code $variable $level
		      	echo " Dumping SOILW level 3 from gribfile "
		      	wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW3$EXTEN > /dev/null
		fi
		
		if [ $variable = "SOILW" -a $level = "100-200" ] 
		then
			echo $code $variable $level
      			echo " Dumping SOILW level 4 from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW4$EXTEN > /dev/null
  		fi

                if [ $variable = "SOILW" -a $level = "10-200" ]
                then
                        echo $code $variable $level
                        echo " Dumping SOILW level 2 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW2$EXTEN > /dev/null
                        echo " Dumping SOILW level 3 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW3$EXTEN > /dev/null
                        echo " Dumping SOILW level 4 from gribfile "
                        wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_SOILW4$EXTEN > /dev/null
                fi
  		  	
  		if [ $variable = "WEASD" -a $level = "sfc" ] 
  		then
  			echo $code $variable $level
      			echo " Dumping SNOW from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_WEASD$EXTEN > /dev/null
  		fi

  		if [ $variable = "ICEC" -a $level = "sfc" ] 
  		then
  			echo $code $variable $level
      			echo " Dumping SEAICE from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_ICEC$EXTEN > /dev/null
  		fi

  		if [ $variable = "PRMSL" -a $level = "MSL" ] 
   		then
   			echo $code $variable $level
      			echo " Dumping PRMSL from gribfile "
      			wgrib -d $code $GRIBFILE -o $OUTDIR/"$tval"_PRMSL$EXTEN > /dev/null
      		fi
  	done	

	tac content.txt | while read line
    	do
    		code=`echo ${line} | gawk -F":" '{print $1}'` 
  		variable=`echo ${line} | gawk -F":" '{print $4}'`
  		level=`echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}'`
  	        units=`echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $2}'`

  		if [ "$variable" = "UGRD" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $units $levs
					echo " Dumping Upper level fields U Level: " $levs
      					wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_UU$EXTEN > /dev/null
    				fi
    			done
  		fi
  		
  		if [ "$variable" = "VGRD" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $levs
		      			echo " Dumping Upper level fields V Level: " $levs
		      			wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_VV$EXTEN > /dev/null
		    		fi
		    	done
		fi
		
		if [ "$variable" = "TMP" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $levs
		      			echo " Dumping Upper level fields T Level: " $levs
		      			wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_TT$EXTEN > /dev/null
		    		fi
		    	done
		fi   

		if [ "$variable" = "HGT" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $levs
		      			echo " Dumping Upper level fields HGT Level: " $levs
		      			wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_HH$EXTEN > /dev/null
		    		fi
		    	done
		fi  

		if [ "$variable" = "RH" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $levs
		      			echo " Dumping Upper level fields RH Level: " $levs
		      			wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_SH$EXTEN > /dev/null
		    		fi
		    	done
		fi  	

		if [ "$variable" = "CLWMR" -a "$units" = "mb" ] 
  		then
    			for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100
    			do
    				if [ $level = $levs ]
    				then
					echo $code $variable $level $levs
		      			echo " Dumping Upper level fields CLWMR Level: " $levs
		      			wgrib -d $code $GRIBFILE -append -o $OUTDIR/"$tval"_CW$EXTEN > /dev/null
		    		fi
		    	done
		fi  



    	done

	rm content.txt

	# Convert degribbed data to unformatted format for local area model use.

	echo " "
	echo " Start degrib to formatted data conversion process for timestep " $tval
	echo " "
 
	$ROOT/gfs2model_rrtm.x $tval >> gfsconv.out

	#this might be big, let's first list, than remove it
	ls -l $OUTDIR/*$EXTEN
	rm -f $OUTDIR/*$EXTEN

	ls -1 $OUTDIR/*.gfs > flist

	tval=$(($tval + $int))

done

rm gfsconv.out
rm date.txt
rm $ROOT/gfs2model_rrtm.x

exit
