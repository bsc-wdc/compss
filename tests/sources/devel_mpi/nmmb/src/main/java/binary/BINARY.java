package binary;

public class BINARY {

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** COMPILE STEP *****************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */
    public static Integer fortranCompiler(String mcFlag, String sharedFlag, String covertPrefix, String convertValue, String tracebackFlag,
            String assumePrefix, String assumeValue, String optFlag, String fpmodelPrefix, String fpmodelValue, String stackFlag,
            String oFlag, String executable, String source) {

        return null;
    }

    public static Integer fortranCompileObject(String mcFlag, String sharedFlag, String covertPrefix, String convertValue,
            String tracebackFlag, String assumePrefix, String assumeValue, String optFlag, String fpmodelPrefix, String fpmodelValue,
            String stackFlag, String cFlag, String source, String oFlag, String object, String moduleFlag, String moduleDir) {

        return null;
    }

    public static Integer fortranCompileWithObject(String mcFlag, String sharedFlag, String covertPrefix, String convertValue,
            String tracebackFlag, String assumePrefix, String assumeValue, String optFlag, String fpmodelPrefix, String fpmodelValue,
            String stackFlag, String oFlag, String executable, String source, String object) {

        return null;
    }

    public static Integer fortranCompilerWithW3(String mcFlag, String sharedFlag, String covertPrefix, String convertValue,
            String tracebackFlag, String assumePrefix, String assumeValue, String optFlag, String fpmodelPrefix, String fpmodelValue,
            String stackFlag, String oFlag, String executable, String source, String w3libDir, String bacioLibDir, String w3Lib,
            String bacioLib) {

        return null;
    }

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** FIXED STEP *******************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    public static Integer smmount(String topoDir, String seamaskDEM, String heightDEM) {
        return null;
    }

    public static Integer landuse(String landuseDir, String landuse, String kount_landuse) {
        return null;
    }

    public static Integer landusenew(String gtopDir, String landusenew, String kount_landusenew) {
        return null;
    }

    public static Integer topo(String topoDir, String heightmean) {
        return null;
    }

    public static Integer stdh(String seamaskDEM, String heightmean, String topoDir, String stdh) {
        return null;
    }

    public static Integer envelope(String heightmean, String stdh, String height) {
        return null;
    }

    public static Integer topsoiltype(String seamaskDEM, String soiltypeDir, String topsoiltype) {
        return null;
    }

    public static Integer botsoiltype(String seamaskDEM, String soiltypePath, String botsoiltype) {
        return null;
    }

    public static Integer toposeamask(String seamaskDEM, String seamask, String height, String landuse, String topsoiltype,
            String botsoiltype) {

        return null;
    }

    public static Integer stdhtopo(String seamask, String stdh) {
        return null;
    }

    public static Integer deeptemperature(String seamask, String soiltempPath, String deeptemperature) {
        return null;
    }

    public static Integer snowalbedo(String maxsnowalbDir, String snowalbedo) {
        return null;
    }

    public static Integer vcgenerator(String dsg) {
        return null;
    }

    public static Integer roughness(String roughnessDir, String roughness) {
        return null;
    }

    public static Integer gfdlco2(String dsg, String co2_data_dir, String co2_trans) {
        return null;
    }

    public static Integer run_aerosol(String lookup_aerosol2_rh00, String lookup_aerosol2_rh50, String lookup_aerosol2_rh70,
            String lookup_aerosol2_rh80, String lookup_aerosol2_rh90, String lookup_aerosol2_rh95, String lookup_aerosol2_rh99) {

        return null;
    }

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** VARIABLE STEP ****************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    public static Integer degribgfs_generic_05(String cW, String iCEC, String sH, String sOILT2, String sOILT4, String sOILW2,
            String sOILW4, String tT, String vV, String hH, String pRMSL, String sOILT1, String sOILT3, String sOILW1, String sOILW3,
            String sST_TS, String uU, String wEASD) {

        return null;

    }

    public static Integer gfs2model_rrtm(String cW, String iCEC, String sH, String sOILT2, String sOILT4, String sOILW2, String sOILW4,
            String tT, String vV, String hH, String pRMSL, String sOILT1, String sOILT3, String sOILW1, String sOILW3, String sST_TS,
            String uU, String wEASD, String gFS_file) {

        return null;
    }

    public static Integer inc_rrtm(String gFS_file, String Deco) {
        return null;
    }

    public static Integer cnv_rrtm(String gFS_file, String llspl000, String outtmp, String outmst, String outsst, String outsno,
            String outcic, String deco) {

        return null;
    }

    public static Integer degribsst(String llgsst05, String sstfileinPath) {
        return null;
    }

    public static Integer albedo(String llspl000, String seamask, String albedo, String albedobase, String albedomnth) {
        return null;

    }

    public static Integer albedorrtm(String llspl000, String seamask, String albedorrtm, String albedorrtm1degDir) {
        return null;
    }

    public static Integer vegfrac(String llspl000, String seamask, String vegfrac, String vegfracmnth) {
        return null;
    }

    public static Integer z0vegfrac(String seamask, String landuse, String topsoiltype, String height, String stdh, String vegfrac,
            String z0base, String z0, String ustar) {

        return null;
    }

    public static Integer allprep(String llspl000, String llgsst05, String sst05, String height, String seamask, String stdh,
            String deeptemperature, String snowalbedo, String z0, String z0base, String landuse, String landusenew, String topsoiltype,
            String vegfrac, String albedorrtm, String llgsst, String lgsno, String llgcic, String llsmst, String llstmp,
            String albedorrtmcorr, String dzsoil, String tskin, String sst, String snow, String snowheight, String cice, String seamaskcorr,
            String landusecorr, String landusenewcorr, String topsoiltypecorr, String vegfraccorr, String z0corr, String z0basecorr,
            String emissivity, String canopywater, String frozenprecratio, String smst, String sh2o, String stmp, String dsg, String fcst,
            String albedo, String ustar, String fcstDir, String bocoPrefix, String llsplPrefix) {

        return null;
    }

    public static Integer readpaulsource(String seamask, String source, String sourceNETCDF) {
        return null;

    }

    public static Integer dust_start(String llspl000, String soildust, String snow, String topsoiltypecorr, String landusecorr,
            String landusenewcorr, String kount_landuse, String kount_landusenew, String vegfrac, String height, String seamask,
            String source, String z0corr, String roughness) {

        return null;
    }

}
