set terminal png size 1500,1500
# #CBM3 GNUPLOT

#Plots:   I3 vs IO3
#Parameters:
        #$1 = taskSleepTime
        #$2 = workers
        #$3 = deepness


set title sprintf("CBM3  - COMPSs IN vs INOUT [Sleep: %i ms, Workers: %i,  Deepness: %i]  [[[[ USING RENAMING ]]]]", taskSleepTime, workers, deepness)

####################################

set xlabel "TX Size KB"
#set xrange [0:100000]

set ylabel "Time ms"
#set yrange [0:60000]

set grid ytics lt 0 lw 1 lc rgb "#bbbbbb"
set grid xtics lt 0 lw 1 lc rgb "#bbbbbb"

###################################

set key inside
set datafile missing "?"
        #$8=time IOFILES, $9=time IOOBJ, $10=time IFILES, $11=time IOBJ
plot "../data/gnuplotInput.dat" u ($4/1000):10  title "CBM3 Files IN" w lp pt 7,\
     "../data/gnuplotInput.dat" u ($4/1000):8   title "CBM3 Files INOUT" w lp pt 7,\
     "../data/gnuplotInput.dat" u ($4/1000):11  title "CBM3 Objects IN" w lp pt 7,\
     "../data/gnuplotInput.dat" u ($4/1000):9   title "CBM3 Objects INOUT" w lp pt 7 
replot 

