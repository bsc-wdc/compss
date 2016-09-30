set terminal png size 1500,1500
# #COMPSS - BM1 GNUPLOT

#Plots:   [ Speedup(y) ]   vs.  [ Number of workers(x) ]
        #$1 = numTasks

set title sprintf("COMPSs BM  -  Speedup vs Num Workers - %i Num tasks", numTasks)

####################################

set xlabel "Number of workers"
#set logscale x 2
set xrange [:64]

set ylabel "Speedup"
#set logscale y 10
set yrange [0:64]

###################################

set key inside
set datafile missing "?"

set grid ytics lt 0 lw 1 lc rgb "#bbbbbb"
set grid xtics lt 0 lw 1 lc rgb "#bbbbbb"

sequential (taskSleepTime) = (taskSleepTime * numTasks) / 16 # sequential=1 worker, 16 tasks per node
speedup (elapsedTime, taskSleepTime) = sequential(taskSleepTime) / elapsedTime

#Row format:  
    #1=numTasks,  2=taskDeepness, 3=taskSleepTime, 4=numWorkers,
    #  5=PAR1ms, 6=PAR2ms, 7=PAR3ms, 8=PAR4ms, 9=PAR5ms, 10=PAR6ms, 11=PAR7ms, ...
    
    # _PAR_ is set by the auto-cbm executing script

plot "../data/gnuplotInput.dat" u 4:(speedup($5,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($6,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($7,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($8,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($9,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($10,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($11,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($12,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($13,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
     "../data/gnuplotInput.dat" u 4:(speedup($14,__PAR__)) title "Speedup task sleep time __PAR__ ms" w lp pt 7,\
replot 

