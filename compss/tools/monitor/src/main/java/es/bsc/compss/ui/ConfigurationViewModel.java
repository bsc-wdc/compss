package es.bsc.compss.ui;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.BindUtils;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.zul.ListModelList;

import es.bsc.compss.commons.Loggers;


public class ConfigurationViewModel {

    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_CONFIGURATION);
    private List<ConfigParam> configurations;


    // Define Refresh time class
    private class refreshTime extends ConfigParam {

        public refreshTime(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            logger.debug("Refresh time update.");

            int ms = Integer.valueOf(this.getValue()) * 1000;
            if ((ms > 0) && (ms < 60000)) {
                logger.debug("   New refresh time = " + ms + " ms");
                Properties.setRefreshTime(ms);
            } else {
                logger.debug("   Refresh time out of bounds: " + ms + " ms");
                this.setValue(String.valueOf(Properties.getRefreshTime() / 1_000));
            }
        }
    }

    // Define Sort Applications class
    private class sortApplications extends ConfigParam {

        public sortApplications(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            logger.debug("Sort Applications update.");
            boolean newValue = Boolean.valueOf(this.getValue());
            logger.debug("   New sort application value = " + newValue);
            Properties.setSortApplications(newValue);
        }
    }

    // Define load Graph x-scale class
    private class loadGraphXScale extends ConfigParam {

        public loadGraphXScale(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            logger.debug("Load Graph x-Scale update");
            int newValue = Integer.valueOf(this.getValue());
            if (newValue >= 1) {
                logger.debug("   New load Graph x-Scale update = " + newValue);
                Properties.setxScaleForLoadGraph(newValue);
            } else {
                logger.debug("   The load graph value isn't correct. Reverting value.");
                this.setValue(String.valueOf(Properties.getxScaleForLoadGraph()));
            }
        }
    }


    @Init
    public void init() {
        logger.debug("Loading configurable parameters...");
        configurations = new LinkedList<>();

        // Add Refresh Time
        refreshTime rt = new refreshTime("Refresh Time (s)", String.valueOf(Properties.getRefreshTime() / 1_000), false);
        configurations.add(rt);
        // Add Sort Applications
        sortApplications sa = new sortApplications("Sort applications (true/false)", String.valueOf(Properties.isSortApplications()), false);
        configurations.add(sa);
        // Add LoadGraph X-Scale
        loadGraphXScale lgxs = new loadGraphXScale("Load Graph's X-Scale factor (int >= 1)", String.valueOf(Properties.getxScaleForLoadGraph()),
                false);
        configurations.add(lgxs);

        logger.debug("Configurable parameters loaded");
    }

    public List<ConfigParam> getConfigurations() {
        return new ListModelList<ConfigParam>(this.configurations);
    }

    @Command
    public void changeEditableStatus(@BindingParam("ConfigParam") ConfigParam cp) {
        cp.setEditingStatus(!cp.getEditingStatus());
        refreshRowTemplate(cp);
    }

    @Command
    public void confirm(@BindingParam("ConfigParam") ConfigParam cp) {
        cp.update();
        changeEditableStatus(cp);
        refreshRowTemplate(cp);
    }

    public void refreshRowTemplate(ConfigParam cp) {
        BindUtils.postNotifyChange(null, null, cp, "editingStatus");
    }

}
