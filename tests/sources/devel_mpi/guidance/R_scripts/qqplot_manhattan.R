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

qq.plot <- function(tab,lambda=T,stat,BA=F,plot=T,mod,
                    pch.col="red",max.yaxis=10,max.xaxis=10,scale.cex=T,scale.fact=0.25,dens=T,...) {

    #Calculating LAMBDA
    chi_list <- qchisq(na.omit(tab$P),df=1,lower.tail=F);
    l.fac <- median(chi_list)/0.456;
    print(paste(c("LAMBDA = ",l.fac),collapse=""));

    #Setting format for plot
    pvals <- -log10((na.omit(tab$P)));
    tab$col <- "ivory3"
    tab$col[tab$P <= 5e-8] <- "deeppink"
    col <- tab$col[!is.na(tab$P)]
    col <- col[order(pvals)]
    pvals <- pvals[order(pvals)]
    tab$log10 <- pvals
    n <- length(pvals)

    #Parameters for density plot
    frac <- 0.01;
    alpha <- 0.05
    index <- seq(1,n)
    nsmall <- round(n*frac)
    temp <- log(seq(2,n-nsmall,100))
    indlow <- round(temp/max(temp)*(n-nsmall))
    index <- c(indlow,seq(n-nsmall,n))
    A <- qbeta(alpha/2,shape1=index,shape2=n-index+1,lower.tail=T)
    B <- qbeta(alpha/2,shape1=index,shape2=n-index+1,lower.tail=F)
    lower <- qexp(A,rate=log(10))
    upper <- qexp(B,rate=log(10))
    p.exp <- sort(-log10( c(1:length(pvals))/(length(pvals)+1) ))
    
    #Scaling Size dots
    scale.fact <- 0.25
    my.cex <- scale.fact * pvals * 0.9;
    my.cex[pvals > -log10(5e-3)] <- 0.2 * pvals[pvals > -log10(5e-3)] * 0.9;
    my.cex[pvals > -log10(5e-5)] <- 0.15 * pvals[pvals > -log10(5e-5)] * 0.9;
    my.cex[pvals > -log10(5e-8)] <- 0.1 * pvals[pvals > -log10(5e-8)] * 0.9;
    my.cex[pvals > -log10(5e-20)] <- 0.05 * pvals[pvals > -log10(5e-20)] * 0.9;
    ylab <- expression(-log[10]~italic(P)[Obs])
    xlab <- expression(-log[10]~italic(P)[Exp])
    max.xaxis <- max(p.exp) + 1
    max.yaxis <- max(pvals) + 6
    
    #Creating Plot
    plot(p.exp,pvals, xlab=xlab, ylab=ylab,
           xlim=c(0,max.xaxis), ylim=c(0,max.yaxis), type="n", xaxs="i", yaxs="i",bty="l")
    #plot the confidence band for the data
    smallexp <- p.exp[index]
    y <- c(rev(lower),upper)
    polygon(c(rev(smallexp),smallexp),y,col="grey80",border=NA)

    ## rescaled density plots in bg
    d.vals <- density(pvals)
    lines(d.vals$x, (max.yaxis/2)+((max.yaxis/2)*(d.vals$y/max(d.vals$y))),
 	   col="cadetblue4",lty="dotted",lwd=2)
    # add density axis
    axis(side=4, at=c(max.yaxis/2,max.yaxis), labels=c(0,round(max(d.vals$y),1)))
    mtext("Data density",side=4,at=max.yaxis*0.75,line=1,adj=0.5,cex=0.9)
    lines(c(0,-log10(1/length(pvals))),c(0,-log10(1/length(pvals))), col="lightslategrey", lwd=2,lty=2)
    points(p.exp[col == "deeppink"], pvals[col == "deeppink"], pch=18, cex=my.cex[col == "deeppink"], col="deeppink")
    points(p.exp[col == "ivory3"], pvals[col=="ivory3"], pch=18, cex=my.cex[col=="ivory3"], col="ivory3")

    #legend files
    mtext(substitute(paste(lambda~" = "~lfac),list(lfac=round(l.fac,digits=3))),at=c(max.xaxis*0.5,max.yaxis*0.5),line=1,adj=0.5,cex=0.9)
}


manhattan <- function(dataframe, colors=c("lightsteelblue4","lightyellow2"), 
                      ymin=0, ymax="max", limitchromosomes=1:25, 
                      suggestiveline=-log10(1e-5), 
                      genomewideline=-log10(5e-8), 
                      annotate=NULL, ...) {
   d=dataframe
   #Validate input files
   if (!("CHR" %in% names(d) & "BP" %in% names(d) & "P" %in% names(d))) stop("Make sure your data frame contains columns CHR, BP, and P")
   if (any(limitchromosomes)) d=d[d$CHR %in% limitchromosomes, ]
   d=subset(na.omit(d[order(d$CHR, d$BP), ]), (P>0 & P<=1)) # remove na's, sort, and keep only 0<P<=1
   d$logp = -log10(d$P)
   d$pos=NA
   ticks=NULL
   lastbase=0
   colors <- c(rep(colors,24)[1:24],"lightsteelblue4")
   if (ymax=="max") {ymax<-ceiling(max(d$logp))}
   	ymax<-as.numeric(ymax)
        if (ymax < 8) {ymax<-8}

        #Print Max and Min P-values
        print(paste("YMAX:",ymax))
        print(paste("YMIN:",ymin))

        numchroms = length(unique(d$CHR))

        if (numchroms==1) {
                d$pos=d$BP
                ticks=floor(length(d$pos))/2+1
        } else {
                for (i in unique(d$CHR)) {
                        if (i==as.vector(unique(d$CHR))[1]) {
                                d[d$CHR==i, ]$pos=d[d$CHR==i, ]$BP
                        } else {
                                lastbase=lastbase+tail(subset(d,CHR==i-1)$BP, 1)
                                d[d$CHR==i, ]$pos=d[d$CHR==i, ]$BP+lastbase
                        }
                        ticks=c(ticks, d[d$CHR==i, ]$pos[floor(length(d[d$CHR==i, ]$pos)/2)+1])
                }
        }
        if (numchroms==1) {
                plot(d$pos, d$logp, ylim=c(ymin,ymax), ylab=expression(-log[10](italic(p))),
                        xlab=paste("Chromosome",unique(d$CHR),"position"))
        }else {
                plot(d$pos,d$logp, ylim=c(ymin,ymax), ylab=expression(-log[10](italic(p))),
                        xlab="Chromosome", xaxt="n", type="n")
                axis(1, at=ticks, lab=unique(d$CHR))
                icol=1

                for (i in unique(d$CHR)) {
                #with(d[d$CHR==i, ],points(pos[d$P > 5e-8], logp[d$P > 5e-8], col=colors[icol], ...))
                        points(d$pos[d$CHR == i], d$logp[d$CHR == i], col=colors[icol],pch=".")
                        icol=icol+1
                }
                for (i in unique(d$CHR)) {
                        #with(d[d$CHR==i, ],points(pos[d$P > 5e-8], logp[d$P > 5e-8], col=colors[icol], ...))
                        points(d$pos[d$CHR == i & d$logp >= -log10(5e-8)], 
                        d$logp[d$CHR == i & d$logp >= -log10(5e-8)], 
                        col="lightcoral",cex=0.6,pch=16)
                }
        }


        genomewideline <- -log10(5e-8)
        abline(h=genomewideline, col="indianred2", lty=2)


}

################################################################################################################

library(gap)
library(sfsmisc)


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

    #makeqqplot
    pdf(out_qqplot)
        qq.plot(tab_file_data, lambda=F, stat="P" ,scale.cex=T);

    dev.off()
    tiff(out_qqplot_tiff,width=5600,height=5600,units = "px", res = 800,compression="lzw")
            qq.plot(tab_file_data, lambda=F, stat="P" ,scale.cex=T,mod=modal);
    dev.off()
    cat("Q-Q Plot Assoc successfully completed!\n")
    
    #make Manhattan on pdf
    tab_file_data$SNP <- paste(paste("chr",tab_file_data$CHR,sep=""),tab_file_data$BP,sep=":")
    tab_man <- data.frame(SNP=tab_file_data$SNP,
                          CHR=tab_file_data$CHR,
                          BP=tab_file_data$BP,
                          P=tab_file_data$P)

    tab_man <- tab_man[tab_man$P <= 0.05,]

    title <- "Manhattan-plot"
    YMIN <- 0
    YMAX <- "max"

    pdf(out_manhattan,width=14,height=7.5)
        manhattan(tab_man, pch=16,cex=0.70,main=title,colors=c("lightsteelblue4","ivory3"), suggestiveline=-log10(5e-8), ymax=YMAX, ymin=YMIN)
    dev.off()

    #make Manhattan on tiff
    tiff(out_manhattan_tiff,width=9600,height=5600,units = "px", res = 800,compression="lzw")
        manhattan(tab_man, pch=16,cex=0.70,main=title,colors=c("lightsteelblue4","ivory3"), suggestiveline=-log10(5e-8), ymax=YMAX, ymin=YMIN)
    dev.off()


    write.table(tab_man,out_corrected_pvals,row.names=F,sep="\t",quote=F)

    cat("MANHATTAN PLOT successfully completed!\n")
