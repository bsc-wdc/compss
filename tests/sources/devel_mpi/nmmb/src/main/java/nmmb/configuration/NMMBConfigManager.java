package nmmb.configuration;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * Loads the NMMB configuration
 * 
 */
public class NMMBConfigManager {

    private final PropertiesConfiguration config;


    /**
     * Loads the runtime configuration found in path @pathToConfigFile
     * 
     * @param pathToConfigFile
     * @throws ConfigurationException
     */
    public NMMBConfigManager(String pathToConfigFile) throws ConfigurationException {
        config = new PropertiesConfiguration(pathToConfigFile);
    }

    /**
     * Returns the CLEAN_BINARIES value
     * 
     * @return
     */
    public boolean getCleanBinaries() {
        return config.getBoolean(NMMBConstants.CLEAN_BINARIES_NAME);
    }

    /**
     * Returns the COMPILE_BINARIES value
     * 
     * @return
     */
    public boolean getCompileBinaries() {
        return config.getBoolean(NMMBConstants.COMPILE_BINARIES_NAME);
    }

    /**
     * Returns the INPES value
     * 
     * @return
     */
    public int getINPES() {
        return config.getInt(NMMBConstants.INPES_NAME);
    }

    /**
     * Returns the JNPES value
     * 
     * @return
     */
    public int getJNPES() {
        return config.getInt(NMMBConstants.JNPES_NAME);
    }

    /**
     * Returns the WRTSK value
     * 
     * @return
     */
    public int getWRTSK() {
        return config.getInt(NMMBConstants.WRTSK_NAME);
    }

    /**
     * Returns the DOMAIN value
     * 
     * @return
     */
    public boolean getDomain() {
        return config.getBoolean(NMMBConstants.DOMAIN_NAME);
    }

    /**
     * Returns the LM value
     * 
     * @return
     */
    public int getLM() {
        return config.getInt(NMMBConstants.LM_NAME);
    }

    /**
     * Returns the CASE name value
     * 
     * @return
     */
    public String getCase() {
        return config.getString(NMMBConstants.CASE_NAME);
    }

    /**
     * Returns the DT_INT1 value
     * 
     * @return
     */
    public int getDT_INT1() {
        return config.getInt(NMMBConstants.DT_INT1_NAME);
    }

    /**
     * Returns the DT_INT2 value
     * 
     * @return
     */
    public int getDT_INT2() {
        return config.getInt(NMMBConstants.DT_INT2_NAME);
    }

    /**
     * Returns the TLM0D1 value
     * 
     * @return
     */
    public double getTLM0D1() {
        return config.getDouble(NMMBConstants.TLM0D1_NAME);
    }

    /**
     * Returns the TLM0D2 value
     * 
     * @return
     */
    public double getTLM0D2() {
        return config.getDouble(NMMBConstants.TLM0D2_NAME);
    }

    /**
     * Returns the TPH0D1 value
     * 
     * @return
     */
    public double getTPH0D1() {
        return config.getDouble(NMMBConstants.TPH0D1_NAME);
    }

    /**
     * Returns the TPH0D2 value
     * 
     * @return
     */
    public double getTPH0D2() {
        return config.getDouble(NMMBConstants.TPH0D2_NAME);
    }

    /**
     * Returns the WBD1 value
     * 
     * @return
     */
    public double getWBD1() {
        return config.getDouble(NMMBConstants.WBD1_NAME);
    }

    /**
     * Returns the WBD2 value
     * 
     * @return
     */
    public double getWBD2() {
        return config.getDouble(NMMBConstants.WBD2_NAME);
    }

    /**
     * Returns the SBD1 value
     * 
     * @return
     */
    public double getSBD1() {
        return config.getDouble(NMMBConstants.SBD1_NAME);
    }

    /**
     * Returns the SBD2 value
     * 
     * @return
     */
    public double getSBD2() {
        return config.getDouble(NMMBConstants.SBD2_NAME);
    }

    /**
     * Returns the DLMD1 value
     * 
     * @return
     */
    public double getDLMD1() {
        return config.getDouble(NMMBConstants.DLMD1_NAME);
    }

    /**
     * Returns the DLMD2 value
     * 
     * @return
     */
    public double getDLMD2() {
        return config.getDouble(NMMBConstants.DLMD2_NAME);
    }

    /**
     * Returns the DPHD1 value
     * 
     * @return
     */
    public double getDPHD1() {
        return config.getDouble(NMMBConstants.DPHD1_NAME);
    }

    /**
     * Returns the DPHD2 value
     * 
     * @return
     */
    public double getDPHD2() {
        return config.getDouble(NMMBConstants.DPHD2_NAME);
    }

    /**
     * Returns the PTOP1 value
     * 
     * @return
     */
    public double getPTOP1() {
        return config.getDouble(NMMBConstants.PTOP1_NAME);
    }

    /**
     * Returns the PTOP2 value
     * 
     * @return
     */
    public double getPTOP2() {
        return config.getDouble(NMMBConstants.PTOP2_NAME);
    }

    /**
     * Returns the DCAL1 value
     * 
     * @return
     */
    public double getDCAL1() {
        return config.getDouble(NMMBConstants.DCAL1_NAME);
    }

    /**
     * Returns the DCAL2 value
     * 
     * @return
     */
    public double getDCAL2() {
        return config.getDouble(NMMBConstants.DCAL2_NAME);
    }

    /**
     * Returns the NRADS1 value
     * 
     * @return
     */
    public int getNRADS1() {
        return config.getInt(NMMBConstants.NRADS1_NAME);
    }

    /**
     * Returns the NRADS2 value
     * 
     * @return
     */
    public int getNRADS2() {
        return config.getInt(NMMBConstants.NRADS2_NAME);
    }

    /**
     * Returns the NRADL1 value
     * 
     * @return
     */
    public int getNRADL1() {
        return config.getInt(NMMBConstants.NRADL1_NAME);
    }

    /**
     * Returns the NRADL2 value
     * 
     * @return
     */
    public int getNRADL2() {
        return config.getInt(NMMBConstants.NRADL2_NAME);
    }

    /**
     * Returns the DO_FIXED value
     * 
     * @return
     */
    public boolean getFixed() {
        return config.getBoolean(NMMBConstants.DO_FIXED_NAME);
    }

    /**
     * Returns the DO_VARIABLE value
     * 
     * @return
     */
    public boolean getVariable() {
        return config.getBoolean(NMMBConstants.DO_VRBL_NAME);
    }

    /**
     * Returns the DO_UMO value
     * 
     * @return
     */
    public boolean getUmoModel() {
        return config.getBoolean(NMMBConstants.DO_UMO_NAME);
    }

    /**
     * Returns the DO_POST value
     * 
     * @return
     */
    public boolean getPost() {
        return config.getBoolean(NMMBConstants.DO_POST_NAME);
    }

    /**
     * Returns the START_DATE value
     * 
     * @return
     */
    public String getStartDate() {
        return config.getString(NMMBConstants.START_DATE_NAME);
    }

    /**
     * Returns the END_DATE value
     * 
     * @return
     */
    public String getEndDate() {
        return config.getString(NMMBConstants.END_DATE_NAME);
    }

    /**
     * Returns the INIT_CHEM value
     * 
     * @return
     */
    public int getInitChem() {
        return config.getInt(NMMBConstants.INIT_CHEM_NAME);
    }

    /**
     * Returns the COUPLE_DUST value
     * 
     * @return
     */
    public boolean getCoupleDust() {
        return config.getBoolean(NMMBConstants.COUPLE_DUST_NAME);
    }

    /**
     * Returns the COUPLE_DUST_INIT value
     * 
     * @return
     */
    public boolean getCoupleDustInit() {
        return config.getBoolean(NMMBConstants.COUPLE_DUST_INIT_NAME);
    }

    /**
     * Returns the HOUR value
     * 
     * @return
     */
    public int getHour() {
        return config.getInt(NMMBConstants.HOUR_NAME);
    }

    /**
     * Returns the NHOURS value
     * 
     * @return
     */
    public int getNHours() {
        return config.getInt(NMMBConstants.NHOURS_NAME);
    }

    /**
     * Returns the NHOURS_INIT value
     * 
     * @return
     */
    public int getNHoursInit() {
        return config.getInt(NMMBConstants.NHOURS_INIT_NAME);
    }

    /**
     * Returns the HIST value
     * 
     * @return
     */
    public int getHist() {
        return config.getInt(NMMBConstants.HIST_NAME);
    }

    /**
     * Returns the BOCO value
     * 
     * @return
     */
    public int getBoco() {
        return config.getInt(NMMBConstants.BOCO_NAME);
    }

    /**
     * Returns the GSFINIT value
     * 
     * @return
     */
    public String getTypeGFSInit() {
        return config.getString(NMMBConstants.TYPE_GFSINIT_NAME);
    }

    /**
     * Returns the HOUR_P value
     * 
     * @return
     */
    public int getHourP() {
        return config.getInt(NMMBConstants.HOUR_P_NAME);
    }

    /**
     * Returns the NHOURS_P value
     * 
     * @return
     */
    public int getNHoursP() {
        return config.getInt(NMMBConstants.NHOURS_P_NAME);
    }

    /**
     * Returns the HIST_P value
     * 
     * @return
     */
    public int getHistP() {
        return config.getInt(NMMBConstants.HIST_P_NAME);
    }

    /**
     * Returns the LSM value
     * 
     * @return
     */
    public int getLSM() {
        return config.getInt(NMMBConstants.LSM_NAME);
    }

}
