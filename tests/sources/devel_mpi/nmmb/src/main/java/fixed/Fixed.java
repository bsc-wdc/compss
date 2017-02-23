package fixed;

import java.io.File;

import binary.BINARY;

import fixed.utils.FortranWrapper;


public class Fixed {

    // File system paths
    public static final String INSTALL_DIR = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/";

    public static final String FIXED_DIR = INSTALL_DIR + "PREPROC/FIXED/";
    public static final String LOOKUP_TABLES_DIR = FIXED_DIR + "lookup_tables" + File.separator;
    public static final String GEODATA_DIR = FIXED_DIR + "geodata" + File.separator;
    public static final String GTOPO_DIR = FIXED_DIR + "GTOPO30" + File.separator;

    private static final String OUTPUTS_DIR = FIXED_DIR + ".." + File.separator + "output" + File.separator;

    // For info messages
    private static final int LINE_SIZE = 60;
    private static final int PRE_CHARS_HEADER_LINE = 11;
    private static final int PRE_CHARS_MSG_LINE = 5;


    private static void printHeaderMsg(String msg) {
        // Separator line
        System.out.println("");

        // Message line
        System.out.print("========= ");
        System.out.print(msg);
        System.out.print(" ");
        for (int i = PRE_CHARS_HEADER_LINE + msg.length(); i < LINE_SIZE; ++i) {
            System.out.print("=");
        }
        System.out.println("");
    }

    private static void printInfoMsg(String msg) {
        // Separator line
        System.out.println("");

        // Message line
        System.out.print("--- ");
        System.out.print(msg);
        System.out.print(" ");
        for (int i = PRE_CHARS_MSG_LINE + msg.length(); i < LINE_SIZE; ++i) {
            System.out.print("-");
        }
        System.out.println("");
    }

    /**
     * Performs the FIXED steps
     * 
     */
    public static void doFixed() {
        /* Build the fortran executables *************************************************/
        printInfoMsg("Building fixed executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_F90_SRC;
            BINARY.fortranCompiler(FortranWrapper.FFLAGS, FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            BINARY.fortranCompiler(FortranWrapper.FFLAGS, FortranWrapper.OFLAG, executable, src);
        }
        printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        printHeaderMsg("BEGIN");

        printInfoMsg("Generate DEM height and sea mask files");
        String topoDir = GEODATA_DIR + "topo1kmDEM" + File.separator;
        String seamaskDEM = OUTPUTS_DIR + "seamaskDEM";
        String heightDEM = OUTPUTS_DIR + "heightDEM";
        BINARY.smmount(topoDir, seamaskDEM, heightDEM);

        printInfoMsg("Generate landuse file");
        String landuse = OUTPUTS_DIR + "landuse";
        String kount_landuse = OUTPUTS_DIR + "kount_landuse";
        BINARY.landuse(GTOPO_DIR, landuse, kount_landuse);

        printInfoMsg("Generate landusenew file");
        String gtopDir = GEODATA_DIR + "landuse_30s" + File.separator;
        String landusenew = OUTPUTS_DIR + "landusenew";
        String kount_landusenew = OUTPUTS_DIR + "kount_landusenew";
        BINARY.landusenew(gtopDir, landusenew, kount_landusenew);

        printInfoMsg("Generate mountains");
        String topo30sDir = GEODATA_DIR + "topo_30s" + File.separator;
        String heightmean = OUTPUTS_DIR + "heightmean";
        BINARY.topo(topo30sDir, heightmean);

        printInfoMsg("Generate standard deviation of topography height");
        String stdh = OUTPUTS_DIR + "stdh";
        BINARY.stdh(heightmean, seamaskDEM, topo30sDir, stdh);

        printInfoMsg("Generate envelope mountains");
        String height = OUTPUTS_DIR + "height";
        BINARY.envelope(heightmean, stdh, height);

        printInfoMsg("Generate top soil type file");
        String soiltypeDir = GEODATA_DIR + "soiltype_top_30s" + File.separator;
        String topsoiltype = OUTPUTS_DIR + "topsoiltype";
        BINARY.topsoiltype(seamaskDEM, soiltypeDir, topsoiltype);

        printInfoMsg("Generate bottom soil type file");
        String soiltypePath = GEODATA_DIR + "soiltype_bot_30s" + File.separator;
        String botsoiltype = OUTPUTS_DIR + "botsoiltype";
        BINARY.botsoiltype(seamaskDEM, soiltypePath, botsoiltype);

        printInfoMsg("Generate sea mask and reprocess mountains");
        String seamask = OUTPUTS_DIR + "seamask";
        BINARY.toposeamask(seamaskDEM, seamask, height, landuse, topsoiltype, botsoiltype);

        printInfoMsg("Reprocess standard deviation of topography height");
        BINARY.stdhtopo(seamask, stdh);

        printInfoMsg("Generate deep soil temperature");
        String soiltempPath = GEODATA_DIR + "soiltemp_1deg" + File.separator;
        String deeptemperature = OUTPUTS_DIR + "deeptemperature";
        BINARY.deeptemperature(seamask, soiltempPath, deeptemperature);

        printInfoMsg("Generate maximum snow albedo");
        String maxsnowalbDir = GEODATA_DIR + "maxsnowalb" + File.separator;
        String snowalbedo = OUTPUTS_DIR + "snowalbedo";
        BINARY.snowalbedo(maxsnowalbDir, snowalbedo);

        printInfoMsg("Generate vertical coordinate");
        String dsg = OUTPUTS_DIR + "dsg";
        BINARY.vcgenerator(dsg);

        printInfoMsg("Generate highres roughness length for africa and asia");
        String roughnessDir = GEODATA_DIR + "roughness_025s" + File.separator;
        String roughness = OUTPUTS_DIR + "roughness";
        BINARY.roughness(roughnessDir, roughness);

        printInfoMsg("Generate co2 files");
        String co2_data_dir = GEODATA_DIR + "co2data" + File.separator;
        String co2_trans = OUTPUTS_DIR + "co2_trans";
        BINARY.gfdlco2(dsg, co2_data_dir, co2_trans);

        printInfoMsg("Generate lookup tables for aerosol scavenging collection efficiencies");
        String lookup_aerosol2_rh00 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh00";
        String lookup_aerosol2_rh50 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh50";
        String lookup_aerosol2_rh70 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh70";
        String lookup_aerosol2_rh80 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh80";
        String lookup_aerosol2_rh90 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh90";
        String lookup_aerosol2_rh95 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh95";
        String lookup_aerosol2_rh99 = OUTPUTS_DIR + "lookup_aerosol2.dat.rh99";
        BINARY.run_aerosol(lookup_aerosol2_rh00, lookup_aerosol2_rh50, lookup_aerosol2_rh70, lookup_aerosol2_rh80, lookup_aerosol2_rh90,
                lookup_aerosol2_rh95, lookup_aerosol2_rh99);

        /* Clean Up binaries ************************************************************/
        printInfoMsg("Clean up executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }

        /* End binary calls *************************************************************/
        printHeaderMsg("END");
    }

    /**
     * Performs the FIXED steps
     * 
     * @param args
     */
    public static void main(String[] args) {
        doFixed();
    }

}
