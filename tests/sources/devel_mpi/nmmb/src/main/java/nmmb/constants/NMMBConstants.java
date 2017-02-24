package nmmb.constants;

public class NMMBConstants {

    // -----------------------------------------------------------------------
    // MN settings
    public static final String INPES_NAME = "INPES";
    public static final String JNPES_NAME = "JNPES";
    public static final String WRTSK_NAME = "WRTSK";

    // -----------------------------------------------------------------------
    // Global-regional switch - Model domain setup global/regional
    public static final String DOMAIN_NAME = "DOMAIN";
    public static final String LM_NAME = "LM";
    public static final String CASE_NAME = "CASE";

    // -----------------------------------------------------------------------
    // If regional you need to modify manually files llgrid_chem.inc in vrbl409rrtm_bsc1.0_reg
    public static final String DT_INT1_NAME = "DT_INT1";
    public static final String TLM0D1_NAME = "TLM0D1";
    public static final String TPH0D1_NAME = "TPH0D1";
    public static final String WBD1_NAME = "WBD1";
    public static final String SBD1_NAME = "SBD1";
    public static final String DLMD1_NAME = "DLMD1";
    public static final String DPHD1_NAME = "DPHD1";
    public static final String PTOP1_NAME = "PTOP1";
    public static final String DCAL1_NAME = "DCAL1";
    public static final String NRADS1_NAME = "NRADS1";
    public static final String NRADL1_NAME = "NRADL1";
    // -----------------------------------------------------------------------
    public static final String DT_INT2_NAME = "DT_INT2";
    public static final String TLM0D2_NAME = "TLM0D2";
    public static final String TPH0D2_NAME = "TPH0D2";
    public static final String WBD2_NAME = "WBD2";
    public static final String SBD2_NAME = "SBD2";
    public static final String DLMD2_NAME = "DLMD2";
    public static final String DPHD2_NAME = "DPHD2";
    public static final String PTOP2_NAME = "PTOP2";
    public static final String DCAL2_NAME = "DCAL2";
    public static final String NRADS2_NAME = "NRADS2";
    public static final String NRADL2_NAME = "NRADL2";

    // -----------------------------------------------------------------------
    // Case selection
    public static final String DO_FIXED_NAME = "DO_FIXED";
    public static final String DO_VRBL_NAME = "DO_VRBL";
    public static final String DO_UMO_NAME = "DO_UMO";
    public static final String DO_POST_NAME = "DO_POST";

    // -----------------------------------------------------------------------
    // Select START and ENDING Times
    public static final String START_DATE_NAME = "START";
    public static final String END_DATE_NAME = "END";
    public static final String HOUR_NAME = "HOUR";
    public static final String NHOURS_NAME = "NHOURS";
    public static final String NHOURS_INIT_NAME = "NHOURS";
    public static final String HIST_NAME = "HIST";
    public static final String HIST_M_NAME = "HIST_M";
    public static final String BOCO_NAME = "BOCO";
    public static final String TYPE_GFSINIT_NAME = "TYPE_GFSINIT";

    // -----------------------------------------------------------------------
    // Select configuration of POSTPROC (DO_POST)
    public static final String HOUR_P_NAME = "HOUR_P";
    public static final String NHOURS_P_NAME = "NHOURS_P";
    public static final String HIST_P_NAME = "HIST_P";
    public static final String LSM_NAME = "LSM";

    // -----------------------------------------------------------------------
    // Select IC of chemistry for run with COUPLE_DUST_INIT=0
    public static final String INIT_CHEM_NAME = "INIT_CHEM";

    // -----------------------------------------------------------------------
    // Couple dust
    public static final String COUPLE_DUST_NAME = "COUPLE_DUST";
    public static final String COUPLE_DUST_INIT_NAME = "COUPLE_DUST";

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // Environment variable names
    public static final String ENV_NAME_UMO_PATH = "UMO_PATH";
    public static final String ENV_NAME_FIX = "FIX";
    public static final String ENV_NAME_OUTPUT = "OUTPUT";
    public static final String ENV_NAME_UMO_OUT = "UMO_OUT";
    public static final String ENV_NAME_OUTNMMB = "OUTNMMB";

}
