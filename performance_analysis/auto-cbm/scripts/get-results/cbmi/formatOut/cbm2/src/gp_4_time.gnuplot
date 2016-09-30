set terminal png size 1500,1500
#  #CBM2 GNUPLOT

#Plots:   IOFiles vs IFiles vs IOObjects vs IObjects
#Parameters:
        #$1 = taskSleepTime
        #$2 = numTasks
        #$3 = workers
        #$4 = deepness


set title sprintf("CBM2  - COMPSs IOFiles vs IFiles vs IOObjects vs IObjects. [Sleep: %i ms, NTasks: %i, Workers: %i,  Deepness: %i]", taskSleepTime, numTasks, workers, deepness)

####################################

set xlabel "TX Size KB"

set ylabel "Time ms"
#set logscale y 10
#set yrange [0:100000000]

set grid ytics lt 0 lw 1 lc rgb "#bbbbbb"
set grid xtics lt 0 lw 1 lc rgb "#bbbbbb"

###################################

set key inside
set datafile missing "?"
        #$8=IOFILES, $9=IOOBJ, $10=IFILES, $11=IOBJ
plot "../data/gnuplotInput.dat" u ($5/1000):8   title "Files INOUT"   w lp pt 7,\
     "../data/gnuplotInput.dat" u ($5/1000):10  title "Files IN"      w lp pt 7,\
     "../data/gnuplotInput.dat" u ($5/1000):9   title "Objects INOUT" w lp pt 7,\
     "../data/gnuplotInput.dat" u ($5/1000):11  title "Objects IN"    w lp pt 7
      
replot 

