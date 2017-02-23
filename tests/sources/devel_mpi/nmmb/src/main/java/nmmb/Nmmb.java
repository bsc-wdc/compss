package nmmb;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;

import fixed.Fixed;
import nmmb.utils.FileManagement;


public class Nmmb {

    /*
     * MAIN NMMB-DUST WORKFLOW
     */

    // -----------------------------------------------------------------------
    // Define MN settings first
    private static final int INPES = 06; // Number inpes
    private static final int JNPES = 10; // Number jnpes
    private static final int WRTSK = 04; // Number write tasks

    // -----------------------------------------------------------------------
    // Global-regional switch - Model domain setup global/regional
    private static final int DOMAIN = 0; // GLOBAL/REGIONAL (0/1)
    private static final int LM = 24; // Vertical model layers
    private static final String CASE = "GLOB"; // Name of the case

    // -----------------------------------------------------------------------
    // If regional you need to modify manually files llgrid_chem.inc in vrbl409rrtm_bsc1.0_reg
    private static final int DT_INT1 = 180; // Run time step (integer seconds) !180
    private static final double TLM0D1 = 0.0; // Center point longitudinal (E/W)
    private static final double TPH0D1 = 0.0; // Center point latitudinal (S/N)
    private static final double WBD1 = -180.0; // Western boundary (from center point)
    private static final double SBD1 = -90.0; // Southern boundary (from center point)
    private static final double DLMD1 = 1.40625; // Longitudinal grid resolution
    private static final double DPHD1 = 1.0; // Latitudinal grid resolution
    private static final double PTOP1 = 100.; // Pressure top of the domain (Pa)
    private static final double DCAL1 = 0.768; // Mineral Dust Emission Calibration Factor
    private static final int NRADS1 = 20; // Number of timesteps between radiation calls (short)
    private static final int NRADL1 = 20; // Number of timesteps between radiation calls (long)
    // -----------------------------------------------------------------------
    private static final int DT_INT2 = 60; // regional
    private static final double TLM0D2 = 20.0; // regional
    private static final double TPH0D2 = 35.0; // regional
    private static final double WBD2 = -51.0; // regional
    private static final double SBD2 = -35.0; // regional
    private static final double DLMD2 = 0.30; // regional
    private static final double DPHD2 = 0.30; // regional
    private static final double PTOP2 = 5000.; // Pressure top of the domain (Pa)
    private static final double DCAL2 = 0.255; // Mineral Dust Emission Calibration Factor
    private static final int NRADS2 = 60; // Number of timesteps between radiation calls (short)
    private static final int NRADL2 = 60; // Number of timesteps between radiation calls (long)

    // -----------------------------------------------------------------------
    // Case selection
    private static final int DO_FIXED = 0; // RUN FIXED (0/1) 1
    private static final int DO_VRBL = 0; // RUN VRBL (0/1) 1
    private static final int DO_UMO = 0; // RUN UMO (0/1) 0
    private static final int DO_POST = 1; // RUN POST_CARBONO (0/1)

    // -----------------------------------------------------------------------
    // Select START and ENDING Times
    private static final String START = "20140901"; // First day of simulation
    private static final String END = "20140901"; // Last day of simulation (if just one day --> START = END)
    private static final int HOUR = 00; // Choose the time of initial input data (hours)
    private static final int NHOURS = 24; // Length of the forecast (hours)
    private static final int NHOURS_INIT = 24; // History init previous day timestep
    private static final int HIST = 3; // Frequency of history output (up to 552 hours)
    private static final int HIST_M = 180; // Frequency of history output (in minutes)
    private static final int BOCO = 6; // Frequency of boundary condition update for REGIONAL (hours)
    private static final String TYPE_GFSINIT = "FNL"; // FNL or GFS

    // -----------------------------------------------------------------------
    // Select configuration of POSTPROC (DO_POST)
    private static final int HOUR_P = 00; // Choose the time of initial output data (hours)
    private static final int NHOURS_P = 24; // Length of postproc data (hours)
    private static final int HIST_P = 3; // Frequency of history output (hours)
    private static final int LSM = 15; // Output Layers

    // -----------------------------------------------------------------------
    // Select IC of chemistry for run with COUPLE_DUST_INIT=0
    private static final int INIT_CHEM = 0; // 0. IC from ideal conditions
                                            // 2. from inca
                                            // 3.from global nmmb-ctm for regional

    // -----------------------------------------------------------------------
    // Couple dust
    private static final int COUPLE_DUST = 1; // Couple dust for the next run (0/1)
    private static final int COUPLE_DUST_INIT = 0; // Couple dust from the beginning (0/1)

    /*
     * ************************************************************************************************************
     * ************************************************************************************************************ END
     * USER MODIFICATION SECTION HANDS OFF FROM SETTING SECTION BELOW !!!
     * ************************************************************************************************************
     * ************************************************************************************************************
     */

    // -----------------------------------------------------------------------
    // Static execution variables
    private static final int PROC;
    private static final double WBD;
    private static final double SBD;
    private static final double DLMD;
    private static final double DPHD;
    private static final int IMI;
    private static final int JMI;
    private static final int IM;
    private static final int JM;
    private static final int HOUR_CTL;
    private static final String HOUR_STR;
    private static final String HOUR_CTL_STR;

    private static final SimpleDateFormat STR_TO_DATE = new SimpleDateFormat("yyyy/MM/dd");
    private static final Date START_DATE;
    private static final Date END_DATE;

    // -----------------------------------------------------------------------
    // Retrieve information from environment
    private static final String OUTNMMB = System.getenv("OUTNMMB");
    private static final String UMO_OUT = System.getenv("UMO_OUT");

    // -----------------------------------------------------------------------
    // Execution variables
    private static Date CURRENT_DATE;
    private static String FOLDER_OUTPUT_CASE;
    private static String FOLDER_OUTPUT;

    // Set the static execution variables
    static {
        System.out.println("[SETTINGS] Start...");

        // Compute total processes
        PROC = INPES * JNPES * WRTSK;
        System.out.println("[SETTINGS] Number of processors " + PROC);

        // Set variables for workflow
        WBD = (DOMAIN == 0) ? WBD1 : WBD2;
        SBD = (DOMAIN == 0) ? SBD1 : SBD2;
        DLMD = (DOMAIN == 0) ? DLMD1 : DLMD2;
        DPHD = (DOMAIN == 0) ? DPHD1 : DPHD2;

        IMI = (int) (-2.0 * WBD / DLMD + 1.5);
        JMI = (int) (-2.0 * SBD / DPHD + 1.5);

        IM = (DOMAIN == 0) ? IMI + 2 : IMI;
        JM = (DOMAIN == 0) ? JMI + 2 : JMI;

        System.out.println("");
        System.out.println("[MN_SETTINGS] Model grid size - IM / JM / LM: " + IMI + " / " + JMI + " / " + LM);
        System.out.println("[MN_SETTINGS] Extended domain - IM / JM / LM: " + IM + " / " + JM + " / " + LM);
        System.out.println("");

        // Set time variables
        HOUR_CTL = HOUR + HIST;
        HOUR_STR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);
        HOUR_CTL_STR = (HOUR_CTL < 10) ? "0" + String.valueOf(HOUR_CTL) : String.valueOf(HOUR_CTL);

        // Set date variables
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = STR_TO_DATE.parse(START);
            endDate = STR_TO_DATE.parse(END);
        } catch (ParseException pe) {
            System.err.println("[ERROR] Cannot parse start/end dates. Aborting...");
            pe.printStackTrace();
            System.exit(1);
        } finally {
            START_DATE = startDate;
            CURRENT_DATE = startDate;
            END_DATE = endDate;
        }

        System.out.println("[SETTINGS] Done");
    }


    /**
     * MAIN NMMB WORKFLOW
     * 
     * @param args
     */
    public static void main(String[] args) {
        prepareExecution();

        // Fixed process (do before main time looping)
        if (DO_FIXED == 1) {
            doFixed();
        }

        // Start main time loop
        while (!CURRENT_DATE.after(END_DATE)) {
            System.out.println("[MAIN_LOOP] " + STR_TO_DATE.format(CURRENT_DATE) + " simulation started");

            // Define model output folder by case and date
            FOLDER_OUTPUT_CASE = OUTNMMB + File.separator + CASE;
            FOLDER_OUTPUT = OUTNMMB + File.separator + CASE + File.separator + STR_TO_DATE.format(CURRENT_DATE) + HOUR;

            // Vrbl process
            if (DO_VRBL == 1) {
                doVariable();
            }

            // UMO model run
            if (DO_UMO == 1) {
                doUMOModel();
            }

            // Post process
            if (DO_POST == 1) {
                doPost();
            }

            System.out.println("[MAIN_LOOP] " + STR_TO_DATE.format(CURRENT_DATE) + " simulation finished");

            // Getting next simulation day
            CURRENT_DATE = Date.from(CURRENT_DATE.toInstant().plusSeconds(1 * 24 * 60 * 60));
        }

    }

    private static void prepareExecution() {
        // Clean output folder
        System.out.println("[PREPARE_EXECUTION] Clean output folder");
        FileManagement.deleteAll(new File(UMO_OUT));

        // Define and create output folder by case
        System.out.println("[PREPARE_EXECUTION] Create output folder");
        String outputCasePath = UMO_OUT + File.separator + "OUTPUT" + CASE + "output";
        new File(outputCasePath).mkdirs();

        // Symlink for preprocess
        System.out.println("[PREPARE_EXECUTION] Symlink for PREPROC output folder");
        Path existingFilePath = Paths.get(outputCasePath);
        Path symLinkPath = Paths.get(UMO_OUT + File.separator + "PREPROC" + File.separator + "output");
        try {
            Files.createSymbolicLink(symLinkPath, existingFilePath);
        } catch (IOException ioe) {
            System.err.println("[ERROR] Cannot create output symlink. Aborting...");
            ioe.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("[PREPARE_EXECUTION] Done");
    }

    private static void doFixed() {
        System.out.println("[DO_FIXED] Enter fixed process");
        
        // Clean some files
        rm modelgrid.inc
        rm lmimjm.inc
        
        // Prepare files
        if (DOMAIN == 0) {
            
        } else {
            
        }
        
        // Do fixed
        Fixed.doFixed();
        
        System.out.println("[DO_FIXED] Fixed process finished");
    }

    private static void doVariable() {
        System.out.println("[DO_VARIABLE] Enter variable process");
        System.out.println("[DO_VARIABLE] Variable process finished");
    }

    private static void doUMOModel() {
        System.out.println("[DO_UMO_MODEL] Enter UMO Model process");
        System.out.println("[DO_UMO_MODEL] UMO Model process finished");
    }

    private static void doPost() {
        System.out.println("[DO_POST] Enter post process");
        System.out.println("[DO_POST] Post process finished");
    }
}
