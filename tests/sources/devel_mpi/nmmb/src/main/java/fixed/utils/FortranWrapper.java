package fixed.utils;

public class FortranWrapper {

    public static final String FC = "ifort";
    public static final String FFLAGS = "-mcmodel=large -shared-intel -convert big_endian -traceback -assume byterecl -O3 -fp-model precise -fp-stack-check";
    public static final String OFLAG = "-o";

    public static final String SUFFIX_SRC = ".f90";
    public static final String SUFFIX_EXE = ".x";

    public static final String BOTSOILTYPE = "botsoiltype";
    public static final String GFDLCO2 = "gfdlco2";
    public static final String DEEPTEMPERATURE = "deeptemperature";
    public static final String ENVELOPE = "envelope";
    public static final String LANDUSE = "landuse";
    public static final String LANDUSENEW = "landusenew";
    public static final String SMMOUNT = "smmount";
    public static final String ROUGHNESS = "roughness";
    public static final String STDH = "stdh";
    public static final String STDHTOPO = "stdhtopo";
    public static final String SNOWALBEDO = "snowalbedo";
    public static final String TOPO = "topo";
    public static final String TOPOSEAMASK = "toposeamask";
    public static final String TOPSOILTYPE = "topsoiltype";
    public static final String VCGENERATOR = "vcgenerator";

    public static final String[] FORTRAN_FILES = new String[] { BOTSOILTYPE, GFDLCO2, DEEPTEMPERATURE, ENVELOPE, LANDUSE, LANDUSENEW,
            SMMOUNT, ROUGHNESS, STDH, STDHTOPO, SNOWALBEDO, TOPO, TOPOSEAMASK, TOPSOILTYPE, VCGENERATOR };

}
