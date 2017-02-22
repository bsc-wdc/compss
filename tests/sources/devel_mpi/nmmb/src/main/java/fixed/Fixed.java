package fixed;

import java.io.File;

import binary.BINARY;

import fixed.utils.FortranWrapper;


public class Fixed {

    private static final int LINE_SIZE = 60;
    private static final int PRE_CHARS_HEADER_LINE = 11;
    private static final int PRE_CHARS_MSG_LINE = 5;

    public static final String FIXED_DIR = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/";
    public static final String LOOKUP_TABLES_DIR = FIXED_DIR + "lookup_tables" + File.separator;
    private static final String OUTPUTS_DIR = FIXED_DIR + ".." + File.separator + "output" + File.separator;


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

    public static void main(String[] args) {
        /* Build the fortran executables *************************************************/
        printInfoMsg("Building fixed executables");
        for (String fortranFile : FortranWrapper.FORTRAN_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_SRC;
            BINARY.fortranCompiler(FortranWrapper.FFLAGS, FortranWrapper.OFLAG, executable, src);
        }
        printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        printHeaderMsg("BEGIN");

        printInfoMsg("Generate DEM height and sea mask files");
        String seamaskDEM = OUTPUTS_DIR + "seamaskDEM";
        String heightDEM = OUTPUTS_DIR + "heightDEM";
        BINARY.smmount(seamaskDEM, heightDEM);

        printInfoMsg("Generate landuse file");
        String landuse = OUTPUTS_DIR + "landuse";
        String kount_landuse = OUTPUTS_DIR + "kount_landuse";
        BINARY.landuse(landuse, kount_landuse);

        printInfoMsg("Generate landusenew file");
        String landusenew = OUTPUTS_DIR + "landusenew";
        String kount_landusenew = OUTPUTS_DIR + "kount_landusenew";
        BINARY.landusenew(landusenew, kount_landusenew);

        printInfoMsg("Generate mountains");
        String heightmean = OUTPUTS_DIR + "heightmean";
        BINARY.topo(heightmean);

        printInfoMsg("Generate standard deviation of topography height");
        String stdh = OUTPUTS_DIR + "stdh";
        BINARY.stdh(heightmean, seamaskDEM, stdh);

        printInfoMsg("Generate envelope mountains");
        String height = OUTPUTS_DIR + "height";
        BINARY.envelope(heightmean, stdh, height);

        printInfoMsg("Generate top soil type file");
        String topsoiltype = OUTPUTS_DIR + "topsoiltype";
        BINARY.topsoiltype(seamaskDEM, topsoiltype);

        printInfoMsg("Generate bottom soil type file");
        String botsoiltype = OUTPUTS_DIR + "botsoiltype";
        BINARY.botsoiltype(seamaskDEM, botsoiltype);

        printInfoMsg("Generate sea mask and reprocess mountains");
        String seamask = OUTPUTS_DIR + "seamask";
        BINARY.toposeamask(seamaskDEM, seamask, height, landuse, topsoiltype, botsoiltype);

        printInfoMsg("Reprocess standard deviation of topography height");
        BINARY.stdhtopo(seamask, stdh);

        printInfoMsg("Generate deep soil temperature");
        String deeptemperature = OUTPUTS_DIR + "deeptemperature";
        BINARY.deeptemperature(seamask, deeptemperature);

        printInfoMsg("Generate maximum snow albedo");
        String snowalbedo = OUTPUTS_DIR + "snowalbedo";
        BINARY.snowalbedo(snowalbedo);

        printInfoMsg("Generate vertical coordinate");
        String dsg = OUTPUTS_DIR + "dsg";
        BINARY.vcgenerator(dsg);

        printInfoMsg("Generate highres roughness length for africa and asia");
        String roughness = OUTPUTS_DIR + "roughness";
        BINARY.roughness(roughness);

        printInfoMsg("Generate co2 files");
        String co2_trans = OUTPUTS_DIR + "co2_trans";
        BINARY.gfdlco2(dsg, co2_trans);

        printInfoMsg("Generate lookup tables for aerosol scavenging collection efficiencies");
        String lookup_aerosol2 = OUTPUTS_DIR + "lookup_aerosol2.dat";
        BINARY.run_aerosol(lookup_aerosol2);

        /* Clean Up binaries ************************************************************/
        printInfoMsg("Clean up executables");
        for (String fortranFile : FortranWrapper.FORTRAN_FILES) {
            String executable = FIXED_DIR + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }

        /* End binary calls *************************************************************/
        printHeaderMsg("END");
    }

}
