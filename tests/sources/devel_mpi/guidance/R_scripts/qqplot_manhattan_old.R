##
##  Copyright 2002-2014 Barcelona Supercomputing Center (www.bsc.es)
##  Life Science Department, 
##  Computational Genomics Group (http://www.bsc.es/life-sciences/computational-genomics)
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
## Last update: $LastChangedDate: 2015-01-08 11:04:27 +0100 (Thu, 08 Jan 2015) $
## Revision Number: $Revision: 14 $
## Last revision  : $LastChangedRevision: 14 $
## Written by     : XXXXX YYYYYY.
##                  xxxxx.yyyyy@gmail.com
## Modified by    : Friman Sanchez C.
##               friman.sanchez@gmail.com
## GWImp-COMPSs web page: http://cg.bsc.es/gwimp-compss/
##

manhattan <- function(dataframe, colors=c("lightsteelblue4","lightyellow2"), ymin=0, ymax="max", limitchromosomes=1:23, suggestiveline=-log10(1e-5), genomewideline=-log10(5e-8), annotate=NULL, ...) {
   d=dataframe
   if (!("CHR" %in% names(d) & "BP" %in% names(d) & "P" %in% names(d))) stop("Make sure your data frame contains columns CHR, BP, and P")
   if (any(limitchromosomes)) d=d[d$CHR %in% limitchromosomes, ]
   d=subset(na.omit(d[order(d$CHR, d$BP), ]), (P>0 & P<=0.1)) # remove na's, sort, and keep only 0<P<=1
   d$logp = -log10(d$P)
   d$pos=NA
   ticks=NULL
   lastbase=0
   colors <- rep(colors,max(d$CHR))[1:max(d$CHR)]

   if (ymax=="max") {ymax<-ceiling(max(d$logp))}
   ymax<-as.numeric(ymax)
   if (ymax < 8) {ymax<-8}

   #print(paste("YMAX:",ymax))
   #print(paste("YMIN:",ymin))

   numchroms=length(unique(d$CHR))
   if (numchroms==1) {
      d$pos=d$BP
      ticks=floor(length(d$pos))/2+1
   } else {
      for (i in unique(d$CHR)) {
        if (i==unique(d$CHR)[1]) {
                        d[d$CHR==i, ]$pos=d[d$CHR==i, ]$BP
                } else {
                        lastbase=lastbase+tail(subset(d,CHR==i-1)$BP, 1)
                        d[d$CHR==i, ]$pos=d[d$CHR==i, ]$BP+lastbase
                }
                ticks=c(ticks, d[d$CHR==i, ]$pos[floor(length(d[d$CHR==i, ]$pos)/2)+1])
        }
   }

   if (numchroms==1) {
      with(d, plot(pos, logp, ylim=c(ymin,ymax), ylab=expression(-log[10](italic(p))), xlab=paste("Chromosome",unique(d$CHR),"position"), ...))
   }    else {
      with(d, plot(pos, logp, ylim=c(ymin,ymax), ylab=expression(-log[10](italic(p))), xlab="Chromosome", xaxt="n", type="n", ...))
      axis(1, at=ticks, lab=unique(d$CHR), ...)
      icol=1
      for (i in unique(d$CHR)) {
         with(d[d$CHR==i, ],points(pos, logp, col=colors[icol], ...))
         icol=icol+1
        }
   }

   if (!is.null(annotate)) {
      d.annotate=d[which(d$SNP %in% annotate), ]
      with(d.annotate, points(pos, logp, col="green3", ...))
   }

   if (suggestiveline) abline(h=suggestiveline, col="indianred2")
   if (genomewideline) abline(h=genomewideline, col="indianred2")
}

#######################################

#required library

library(gap)

args <- commandArgs(TRUE)

tab_file <- args[1] #tab file with CHR, BP , P
out_qqplot <- args[2] #name out qqplot qqplot_namestudy.pdf
out_manhattan <- args[3] #name out manhattan manhattan_namestudy.pdf

out_qqplot_tiff <- args[4] #Name output file of qqplot on tiff.
out_manhattan_tiff <- args[5] #name out manhattan manhattan_namestudy.pdf

out_corrected_pvals <- args[6]  #Name of output file with genomic control corrected pvalues


tab_file_data<-read.delim(tab_file)

p <- tab_file_data$P
p <- p[!is.na(p)]
n <- length(p)

x2obs <- qchisq(p, 1, lower.tail = FALSE)
x2exp <- qchisq(1:n/n, 1, lower.tail = FALSE)

#compute lambda
lambda <- median(x2obs)/median(x2exp)
#print(paste("LAMBDA: ",lambda,sep=""))

#makeqqplot
pdf(out_qqplot)
qqunif(p, col = "lightyellow4", pch=16,cex=0.65,lcol = "indianred2", sub=paste("lambda: ",round(lambda,digits=4)), main="QQ-Plot")
invisible(list(x = x2exp, y = x2obs, lambda = lambda))
dev.off()
#makeQQPlot on tiff
tiff(out_qqplot_tiff,width=5600,height=5600,units = "px", res = 800,compression="lzw")
qqunif(p, col = "lightyellow4", pch=16,cex=0.65,lcol = "indianred2", sub=paste("lambda: ",round(lambda,digits=4)), main="QQ-Plot")
invisible(list(x = x2exp, y = x2obs, lambda = lambda))
dev.off()



#cat("Q-Q Plot Assoc successfully completed!\n")

#correcting pvalues

x2obs <- qchisq(tab_file_data$P, 1, lower.tail = FALSE)

tab_file_data$P_corr <- as.numeric(1 - pchisq(x2obs/lambda,1))
tab_file_data$P_corr[tab_file_data$P_corr == 0] <- tab_file_data$P[tab_file_data$P_corr == 0]

tab_man <- data.frame(CHR=tab_file_data$CHR,BP=tab_file_data$BP,P=tab_file_data$P_corr)


title <- "Manhattan-plot"
YMIN <- 0
YMAX <- "max"

pdf(out_manhattan)
manhattan(tab_man, pch=".", main=title, colors=c("lightsteelblue4","cornflowerblue"), suggestiveline=-log10(5e-8), ymax=YMAX, ymin=YMIN)
dev.off()

#make Manhattan on tiff
tiff(out_manhattan_tiff,width=5600,height=5600,units = "px", res = 800,compression="lzw")
manhattan(tab_man, pch=".", main=title, colors=c("lightsteelblue4","cornflowerblue"), suggestiveline=-log10(5e-8), ymax=YMAX, ymin=YMIN)
dev.off()


write.table(tab_man,out_corrected_pvals,row.names=F,sep="\t",quote=F)

#cat("MANHATTAN PLOT successfully completed!\n")

