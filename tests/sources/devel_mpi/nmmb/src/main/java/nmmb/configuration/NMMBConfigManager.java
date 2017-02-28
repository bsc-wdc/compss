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

    public int getINPES() {
        return config.getInt(NMMBConstants.INPES_NAME);
    }

    public int getJNPES() {
        return config.getInt(NMMBConstants.JNPES_NAME);
    }

    public int getWRTSK() {
        return config.getInt(NMMBConstants.WRTSK_NAME);
    }

    public boolean getDomain() {
        return config.getBoolean(NMMBConstants.DOMAIN_NAME);
    }

    public int getLM() {
        return config.getInt(NMMBConstants.LM_NAME);
    }

    public String getCase() {
        return config.getString(NMMBConstants.CASE_NAME);
    }

    public int getDT_INT1() {
        return config.getInt(NMMBConstants.DT_INT1_NAME);
    }

    public int getDT_INT2() {
        return config.getInt(NMMBConstants.DT_INT2_NAME);
    }

    public double getTLM0D1() {
        return config.getDouble(NMMBConstants.TLM0D1_NAME);
    }

    public double getTLM0D2() {
        return config.getDouble(NMMBConstants.TLM0D2_NAME);
    }

    public double getTPH0D1() {
        return config.getDouble(NMMBConstants.TPH0D1_NAME);
    }

    public double getTPH0D2() {
        return config.getDouble(NMMBConstants.TPH0D2_NAME);
    }

    public double getWBD1() {
        return config.getDouble(NMMBConstants.WBD1_NAME);
    }

    public double getWBD2() {
        return config.getDouble(NMMBConstants.WBD2_NAME);
    }

    public double getSBD1() {
        return config.getDouble(NMMBConstants.SBD1_NAME);
    }

    public double getSBD2() {
        return config.getDouble(NMMBConstants.SBD2_NAME);
    }

    public double getDLMD1() {
        return config.getDouble(NMMBConstants.DLMD1_NAME);
    }

    public double getDLMD2() {
        return config.getDouble(NMMBConstants.DLMD2_NAME);
    }

    public double getDPHD1() {
        return config.getDouble(NMMBConstants.DPHD1_NAME);
    }

    public double getDPHD2() {
        return config.getDouble(NMMBConstants.DPHD2_NAME);
    }

    public double getPTOP1() {
        return config.getDouble(NMMBConstants.PTOP1_NAME);
    }

    public double getPTOP2() {
        return config.getDouble(NMMBConstants.PTOP2_NAME);
    }

    public double getDCAL1() {
        return config.getDouble(NMMBConstants.DCAL1_NAME);
    }

    public double getDCAL2() {
        return config.getDouble(NMMBConstants.DCAL2_NAME);
    }

    public int getNRADS1() {
        return config.getInt(NMMBConstants.NRADS1_NAME);
    }

    public int getNRADS2() {
        return config.getInt(NMMBConstants.NRADS2_NAME);
    }

    public int getNRADL1() {
        return config.getInt(NMMBConstants.NRADL1_NAME);
    }

    public int getNRADL2() {
        return config.getInt(NMMBConstants.NRADL2_NAME);
    }

    public boolean getFixed() {
        return config.getBoolean(NMMBConstants.DO_FIXED_NAME);
    }

    public boolean getVariable() {
        return config.getBoolean(NMMBConstants.DO_VRBL_NAME);
    }

    public boolean getUmoModel() {
        return config.getBoolean(NMMBConstants.DO_UMO_NAME);
    }

    public boolean getPost() {
        return config.getBoolean(NMMBConstants.DO_POST_NAME);
    }

    public String getStartDate() {
        return config.getString(NMMBConstants.START_DATE_NAME);
    }

    public String getEndDate() {
        return config.getString(NMMBConstants.END_DATE_NAME);
    }

    public int getInitChem() {
        return config.getInt(NMMBConstants.INIT_CHEM_NAME);
    }

}
