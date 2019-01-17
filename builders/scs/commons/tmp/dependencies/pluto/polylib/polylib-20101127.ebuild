# ebuild created by slashdevslashzerr0@gmail.com 2010-Nov-28
# based on polylib-9999.ebuild, found in Gentoo portage tree 

#EGIT_BOOTSTRAP="eautoreconf && cd cln && eautoreconf"
inherit autotools eutils

DESCRIPTION="Polytope manipulation library"
HOMEPAGE="http://icps.u-strasbg.fr/polylib/"
SRC_URI=$HOMEPAGE/${P}.tar.lzma
# there is no such file on the page. The file created by packing git contents

LICENSE="GPL-2"
SLOT="0"
KEYWORDS="amd64"
# don't know if it works for other arches
IUSE="doc"

src_unpack()
 {
  unpack "$A"
  cd "$S" || die "failed to go to $S"
  # fix stupid and dangerous implementation of matrice/vector input
  epatch "${FILESDIR}"/$PN-1024.patch || die "1024.patch failed"
  #./autogen.sh && cd cln && ./autogen.sh || die "autogen.sh failed"
  eautoreconf && cd cln && eautoreconf || die "autoreconf failed"
  use test && einfo "test will be performed"
 }

src_test()
 {
  make check || die "test failed"
  # good return code does not guarantee success. Check log file 
  #  to be sure
  einfo ; einfo "test ok" ; einfo
 }

src_install() 
 {
  emake DESTDIR="${D}" install || die

  # prepend 'polylib-' to executables
  cd "${D}/usr/bin"
  local e=$(ls)
  for i in $e ; do
   mv $i ${PN}-e || die "failed to rename $i"
  done
  
  use doc && 
   {
    cd "$S/doc" || die "no such dir $S/doc"
    # don't repack parampoly-doc.ps.gz
    for i in doc.pdf.* ; do
     unpack ./$i || die "failed to unpack $i"
    done
    mv doc.pdf ${PN}-doc.pdf
    dodoc ${PN}-doc.pdf Changes || die "Failed to install doc.pdf"
    insinto /usr/share/doc/${PF}
    doins parampoly-doc.ps.gz
   }
 }
