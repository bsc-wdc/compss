program toposeamask
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

integer(kind=4),dimension(1:imi,1:jmi):: &
 landuse &                   !
,ltypebot &                  !
,ltypetop

real(kind=4),dimension(1:imi,1:jmi):: &
 height &                    !
,demmask &                   !
,seamask

real(kind=4),dimension(1:im,1:jm):: &
 hgt &                       !
,smt                         !

integer num, length, stat
character*256 input_seamaskDEM, output_seamask, input_height, &
input_landuse, input_topsoiltype, input_botsoiltype

if (command_argument_count().gt.0) then
  call get_command_argument(1,input_seamaskDEM,length,stat)
  call get_command_argument(2,output_seamask,length,stat)
  call get_command_argument(3,input_height,length,stat)
  call get_command_argument(4,input_landuse,length,stat)
  call get_command_argument(5,input_topsoiltype,length,stat)
  call get_command_argument(6,input_botsoiltype,length,stat)
end if

!-----------------------------------------------------------------------
 1100 format(25f5.0)
      infile='                                                         '
      outfile='                                                        '
!--read in the topography height----------------------------------------
      infile = input_height
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) height
      close(1)
!--read in the landuse data to define the sea-mask----------------------
      infile = input_landuse
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) landuse
      close(1)
!--read in the DEM ocean sea mask---------------------------------------
      infile = input_seamaskDEM
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) demmask
      close(1)
!--read in bottom soil type---------------------------------------------
      infile = input_botsoiltype
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) ltypebot
      close(1)
!--read in top soil type------------------------------------------------
      infile = input_topsoiltype
      open(unit=1,file=infile,status='unknown' &
          ,form='unformatted')
      read (1) ltypetop
      close(1)
!--seamask derived from DEM data----------------------------------------
      do j=1,jmi
        do i=1,imi
          seamask(i,j)=demmask(i,j)
        enddo
      enddo
!-----------------------------------------------------------------------
      if(im.gt.imi) then ! global domain
!--pad the model fields to be able to reach all global points-----------
        call padh2(height ,hgt)
        call padh2(seamask,smt)
!--remove small islands, small lakes and waterfalls---------------------
        call fix(hgt,smt)
!--crop the extra row along the boundaries------------------------------
        call croph2(hgt,height )
        call croph2(smt,seamask)
!--derive the sea mask for elevated water bodies from landuse-----------
        do j=1,jmi
          do i=1,imi
            if(abs(height(i,j)).gt.1.) then
              if(landuse(i,j).eq.16) then
                seamask(i,j)=1.
              else
                seamask(i,j)=0.
              endif
            endif
          enddo
        enddo
!--pad the model fields to be able to reach all global points-----------
        call padh2(height ,hgt)
        call padh2(seamask,smt)
!--remove small islands, small lakes and waterfalls---------------------
        call fix(hgt,smt)
!--apply 5-point smoother over land-------------------------------------
        call smooth5(hgt,smt,0)
!--apply 5-point smoother on mountain peaks only------------------------
        call smoothtops(hgt,smt,0)
!--apply progressive 1-2-1 zonal filtering as poles are approached------
        call smoothzonal(hgt,smt,0)
!--crop the extra row along the boundaries------------------------------
        call croph2(hgt,height )
        call croph2(smt,seamask)
!-----------------------------------------------------------------------
      else ! regional domain
!--remove small islands, small lakes and waterfalls---------------------
        call fix(height,seamask)
!--derive the seam mask for elevated water bodies from landuse----------
        do j=1,jmi
          do i=1,imi
            if(abs(height(i,j)).gt.1.) then
              if(landuse(i,j).eq.16) then
                seamask(i,j)=1.
              else
                seamask(i,j)=0.
              endif
            endif
          enddo
        enddo
!--remove small islands, small lakes and waterfalls---------------------
        call fix(height,seamask)
!--apply 5-point smoother over land-------------------------------------
        call smooth5(height,seamask,0)
!--apply 5-point smoother on mountain peaks only------------------------
        call smoothtops(height,seamask,0)
!--apply 5-point smoother along lateral boundaries only-----------------
        call smoothbc(height,seamask,10,10)
!-----------------------------------------------------------------------
      endif
!-----------------------------------------------------------------------
      do j=1,jmi
        do i=1,imi
          if    (seamask(i,j).eq.1.) then
            landuse (i,j)=16 ! water
            ltypebot(i,j)=14 ! water
            ltypetop(i,j)=14 ! water
          elseif(seamask(i,j).eq.0.) then
            if(landuse (i,j).eq.16) landuse (i,j)=17
            if(ltypebot(i,j).eq.14) ltypebot(i,j)=7
            if(ltypetop(i,j).eq.14) ltypetop(i,j)=7
          endif
        enddo
      enddo
!-----------------------------------------------------------------------
      print*,'Topography height'
      do j=jmi,1,-10
        write(*,1100) (height(i,j),i=1,imi,35)
      enddo
!-----------------------------------------------------------------------
      outfile = input_height
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) height
      close(1)
!
      print*,'Height file written to ../output/height'
!-----------------------------------------------------------------------
      print*,'seamask'
      do j=jmi,1,-10
        write(*,1100) (seamask(i,j),i=1,imi,35)
      enddo
!-----------------------------------------------------------------------
      outfile = output_seamask
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) seamask
      close(1)
!
      print*,'Sea mask file written to ../output/seamask'
!--write the landuse data to define the sea-mask------------------------
      outfile = input_landuse
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) landuse
      close(1)
!
      print*,'Revised landuse file written to ../output/landuse'
!--read in bottom soil type---------------------------------------------
      outfile = input_botsoiltype
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) ltypebot
      close(1)
!
      print*,'Revised bottom soil written to ../output/botsoiltype'
!--read in top soil type------------------------------------------------
      outfile = input_topsoiltype
      open(unit=1,file=outfile,status='unknown' &
          ,form='unformatted')
      write(1) ltypetop
      close(1)
!
      print*,'Revised top soil written to ../output/topsoiltype'
!-----------------------------------------------------------------------
      print*,'Enjoy your new files!'
!-----------------------------------------------------------------------
endprogram toposeamask
!-----------------------------------------------------------------------
!+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      subroutine fix(hgt,smt)
!-----------------------------------------------------------------------
      include './include/modelgrid.inc'
!-----------------------------------------------------------------------
      dimension hgt(im,jm),smt(im,jm)
!-----------------------------------------------------------------------
      parameter(kount=30)
!--remove small islands, small lakes and waterfalls---------------------
      do k=1,kount
!-----------------------------------------------------------------------
        do j=2,jm-1
          do i=2,im-1
            sms=smt(i-1,j-1)+smt(i  ,j-1)+smt(i+1,j-1) &
               +smt(i-1,j  )+smt(i+1,j  ) &
               +smt(i-1,j+1)+smt(i  ,j+1)+smt(i+1,j+1) 
            if(smt(i,j).lt.0.5.and.sms.gt.7.5) then
              smt(i,j)=1.
              hgt(i,j)=min(hgt(i  ,j-1),hgt(i-1,j  ) &
                          ,hgt(i+1,j  ),hgt(i  ,j+1))
            endif
            if(smt(i,j).gt.0.5.and.sms.lt.0.5) then
              smt(i,j)=0.
            hgt(i,j)=(hgt(i  ,j-1)+hgt(i-1,j  ) &
                     +hgt(i+1,j  )+hgt(i,j+1  ))*0.25
            endif
            if(smt(i,j).gt.0.5) then
              if(smt(i-1,j-1).gt.0.5.and.hgt(i,j).gt.2.) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j-1))
              if(smt(i  ,j-1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i  ,j-1))
              if(smt(i+1,j-1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i+1,j-1))
              if(smt(i-1,j  ).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j  ))
              if(smt(i+1,j  ).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i+1,j  ))
              if(smt(i-1,j+1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j+1))
              if(smt(i  ,j+1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j+1))
              if(smt(i+1,j+1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j+1))
              if(smt(i-1,j-1).gt.0.5) &
                hgt(i,j)=min(hgt(i  ,j  ),hgt(i-1,j-1))
            endif
          enddo
        enddo
        if(im.gt.imi) then
          call poavh2(hgt)
          call swaph2(hgt)
          call swaph2(smt)
          call poleh2(hgt)
          call poleh2(smt)
        endif
!-----------------------------------------------------------------------
      enddo
!-----------------------------------------------------------------------
      endsubroutine fix
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
      endsubroutine padh2
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
