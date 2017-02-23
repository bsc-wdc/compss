program stdhtopo
!-----------------------------------------------------------------------
!     z. janjic, aug. 2007
!-----------------------------------------------------------------------
implicit none
!-----------------------------------------------------------------------
include './include/modelgrid.inc'
!-----------------------------------------------------------------------
character(23):: &
 fname                       !

character(128):: &
 infile &                    !
,outfile                     !

integer(kind=4):: &
 i,j

real(kind=4),dimension(1:imi,1:jmi):: &
 seamask &                   !
,stdh                        !

real(kind=4),dimension(1:im,1:jm):: &
 smt &                       !
,std                         !

integer num, length, stat
character*256 input_seamask, input_stdh

if (command_argument_count().gt.0) then
  call get_command_argument(1,input_seamask,length,stat)
  call get_command_argument(2,input_stdh,length,stat)
end if

!-----------------------------------------------------------------------
 1100 format(25f5.0)
      infile='                                                         '
      outfile='                                                        '
!--read in the sea-mask-------------------------------------------------
      infile = input_seamask
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) seamask
      close(1)
!--read in the topography standard deviation data-----------------------
      infile = input_stdh
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) stdh
      close(1)
!-----------------------------------------------------------------------
      if(im.gt.imi) then ! global domain
!--pad the model fields to be able to reach all global points-----------
        call padh2(stdh   ,std)
        call padh2(seamask,smt)
!--apply progressive 1-2-1 zonal filtering as poles are approached------
        call smoothzonal(std,smt,0)
!--crop the extra row along the boundaries------------------------------
        call croph2(smt,seamask)
        call croph2(std,stdh   )
!-----------------------------------------------------------------------
      else ! regional domain
!--apply 5-point smoother along lateral boundaries only-----------------
        call smoothbc(stdh  ,seamask,10,10)
!-----------------------------------------------------------------------
      endif
!-----------------------------------------------------------------------
      print*,'STDH'
      do j=jmi,1,-10
        write(*,1100) (stdh(i,j),i=1,imi,40)
      enddo
!-----------------------------------------------------------------------
      outfile = input_stdh
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) stdh
      close(1)
!
      print*,'Height standard deviation file written to ../output/stdh'
!-----------------------------------------------------------------------
      print*,'Enjoy your modified standard deviation file!'
!-----------------------------------------------------------------------
endprogram stdhtopo
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine smooth5(hgt,smt,nsmud)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension hgt(im,jm),hgtp(im,jm),smt(im,jm)
!--------------5-point smoothing of mountains---------------------------
      if(nsmud.eq.0) return
!-----------------------------------------------------------------------
      do n=1,nsmud
!-----------------------------------------------------------------------
        do j=2,jm-1
          do i=2,im-1
            if(smt(i,j).lt.0.5) then
              hgtp(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                        +hgt(i+1,j  )+hgt(i  ,j+1) &
                       +hgt(i,j)*4.)*0.125
            else
              hgtp(i,j)=hgt(i,j)
            endif
          enddo
        enddo
!
        do j=2,jm-1
          do i=2,im-1
            hgt(i,j)=hgtp(i,j)
          enddo
        enddo
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
      endsubroutine smooth5
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine smoothtops(hgt,smt,nsmud)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension hgt(im,jm),smt(im,jm),hgtp(im,jm)
!--------------5-point smoothing of mountains---------------------------
      if(nsmud.eq.0) return
!-----------------------------------------------------------------------
      do n=1,nsmud
        ktrim=1
        do j=2,jm-1
          do i=2,im-1
            if(smt(i,j).lt.0.5) then
              hmax1=(hgt(i  ,j-1) &
                    +hgt(i-1,j  )+hgt(i+1,j  ) &
                    +hgt(i  ,j+1))*.25
              hmax2=(hgt(i-1,j-1)+hgt(i+1,j-1) &
                    +hgt(i-1,j+1)+hgt(i+1,j+1))*.25
              hmax=hmax1*.6666667+hmax2*(1.-.6666667)
              d2h=hgt(i,j)-hmax
!
              if(d2h.gt.010.) then
                hgtp(i,j)=(hmax+hgt(i,j))*0.5
                ktrim=ktrim+1
              else
                hgtp(i,j)=hgt(i,j)
              endif
            else
              hgtp(i,j)=hgt(i,j)
            endif
          enddo
        enddo
!
        do j=2,jm-1
          do i=2,im-1
            hgt(i,j)=hgtp(i,j)
          enddo
        enddo
!-----------------------------------------------------------------------
!        print*,' iter=',n,' ktrim=',ktrim
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
      endsubroutine smoothtops
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine smoothbc(hgt,smt,lines,nsmud)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      parameter (wa=0.5,w1=wa*0.25,w2=1.-wa)
!-----------------------------------------------------------------------
      dimension hgt(im,jm),smt(im,jm),hgtp(im,jm)
!--------------5-point smoothing of mountains---------------------------
      if(nsmud.eq.0) return
!-----------------------------------------------------------------------
      do ks=1,nsmud
!-----------------------------------------------------------------------
        do j=2,lines
          do i=2,im-1
            hgtp(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                      +hgt(i+1,j  )+hgt(i  ,j+1))*w1 &
                     +w2*hgt(i,j)
          enddo
        enddo
        do j=jm-lines+1,jm-1
          do i=2,im-1
            hgtp(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                      +hgt(i+1,j  )+hgt(i  ,j+1))*w1 &
                     +w2*hgt(i,j)
          enddo
        enddo
        do j=lines+1,jm-lines
          do i=2,lines
            hgtp(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                      +hgt(i+1,j  )+hgt(i  ,j+1))*w1 &
                     +w2*hgt(i,j)
          enddo
        enddo
        do j=lines+1,jm-lines
          do i=im-lines+1,im-1
            hgtp(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                      +hgt(i+1,j  )+hgt(i  ,j+1))*w1 &
                     +w2*hgt(i,j)
          enddo
        enddo
!-----------------------------------------------------------------------
        do j=2,lines
          do i=2,im-1
            hgt(i,j)=hgtp(i,j)*(1.0-smt(i,j))+hgt(i,j)*smt(i,j)
          enddo
        enddo
        do j=jm-lines+1,jm-1
          do i=2,imi-1
            hgt(i,j)=hgtp(i,j)*(1.0-smt(i,j))+hgt(i,j)*smt(i,j)
          enddo
        enddo
        do j=lines+1,jm-lines
          do i=2,lines
            hgt(i,j)=hgtp(i,j)*(1.0-smt(i,j))+hgt(i,j)*smt(i,j)
          enddo
        enddo
        do j=lines+1,jm-lines
          do i=im-lines+1,im-1
            hgt(i,j)=hgtp(i,j)*(1.0-smt(i,j))+hgt(i,j)*smt(i,j)
          enddo
        enddo
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
      endsubroutine smoothbc
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine smoothzonal(hgt,smt,n)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      parameter &
      (pi=3.141592653589793238462643383279502884197169399375105820 &
      ,pih=pi/2.,dtr=pi/180.)
      parameter (wa=0.5,w1=wa*0.5,w2=1.-wa,cfilt=1.,rwind=1./2.)
      dimension hgt(im,jm),smt(im,jm)
      dimension hgtp(im)
!-----------------------------------------------------------------------
      if(n.eq.0) return
!-----------------------------------------------------------------------
      dlm=dlmd*dtr
      dph=dphd*dtr
!
      cpf=dph/dlm
!----------------------------------------------------------------------
      tph=sbd*dtr
      do j=3,jm-2
        tph=tph+dph
        cph=cos(tph)
        rcph=(cph/cpf)
        nsmud=0
        if(rcph.lt.cfilt) then
          do k=2,imi-2,2
            x=k*dlm*0.25
            xl=min(x/rcph,pih)
            sxl=sin(xl)+sin(2.*xl)/2.*rwind
            if(rcph/sxl.gt.cfilt) then
              nsmud=0
            else
              cx=cos(x)
              nsmud=(log(sxl/(xl*(1.+rwind)))/log(cx)+0.5)
              if(mod(nsmud,2).gt.0) nsmud=nsmud+1
              nsmud=nsmud/2
              if(xl.eq.pih) go to 200
            endif
          enddo
        endif
!----------------------------------------------------------------------
 200    do ks=1,nsmud
!----------------------------------------------------------------------
          do i=2,im-1
            hgtp(i)=(hgt(i-1,j)+hgt(i+1,j))*w1+hgt(i,j)*w2
          enddo
!
          hgtp(1)=hgtp(im-2)
          ave=(hgtp(2)+hgtp(im-1))*0.5
          hgtp(2   )=ave
          hgtp(im-1)=ave
          hgtp(im  )=hgtp(3)
!
          do i=1,im
            if(smt(i,j).lt.0.5) then
              hgt(i,j)=hgtp(i)
            endif
          enddo
!----------------------------------------------------------------------
        enddo ! end of smoothing
!----------------------------------------------------------------------
      enddo ! end of j
!----------------------------------------------------------------------
      call poavh2(hgt)
      call swaph2(hgt)
      call poleh2(hgt)
!----------------------------------------------------------------------
      endsubroutine smoothzonal
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine padh2(h2,ph2)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
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
!--adding extra row of points along boundaries--------------------------
      do i=1,imi
        ph2(i+1,jm)=h2(i,jm-3)
      enddo
      do j=jm-1,2,-1
        do i=1,imi
          ph2(i+1,j)=h2(i,j-1)
        enddo
      enddo
      do i=1,imi
        ph2(i+1,1)=h2(i,2)
      enddo
      do j=1,jm
        ph2(1 ,j)=ph2(im-2,j)
        ph2(im,j)=ph2(3   ,j)
      enddo
!-----------------------------------------------------------------------
      return
      endsubroutine padh2
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine swaph2(h2)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension h2(im,jm)
!-----------------------------------------------------------------------
      do j=1,jm
        h2(1,j)=h2(im-2,j)
        ave=(h2(2,j)+h2(im-1,j))*0.5
        h2(2,j)=ave
        h2(im-1,j)=ave
        h2(im,j)=h2(3,j)
      enddo
!-----------------------------------------------------------------------
      endsubroutine swaph2
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine poleh2(h2)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension h2(im,jm)
!-----------------------------------------------------------------------
      do i=1,im
        h2(i,1)=h2(i,3)
        h2(i,jm)=h2(i,jm-2)
      enddo
!-----------------------------------------------------------------------
      endsubroutine poleh2
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine poavh2(h2)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension h2(im,jm)
!-----------------------------------------------------------------------
      raf=1./(imi-1)
      as=0.
      an=0.
      do i=2,im-2
        as=h2(i,2)+as
        an=h2(i,jm-1)+an
      enddo
      as=as*raf
      an=an*raf
      do i=1,im
        h2(i,2)=as
        h2(i,jm-1)=an
      enddo
!-----------------------------------------------------------------------
      endsubroutine poavh2
!-----------------------------------------------------------------------
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine croph2(ph2,h2)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension h2(imi,jmi),ph2(im,jm)
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          h2(i,j)=ph2(i+1,j+1)
        enddo
      enddo
!-----------------------------------------------------------------------
      endsubroutine croph2
!-----------------------------------------------------------------------
