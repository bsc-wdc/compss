/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.ui;

import es.bsc.compss.commons.Loggers;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.BindUtils;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.zul.ListModelList;


public class ConfigurationViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_CONFIGURATION);
    private List<ConfigParam> configurations;


    // Define Refresh time class
    private class RefreshTime extends ConfigParam {

        public RefreshTime(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            LOGGER.debug("Refresh time update.");

            int ms = Integer.valueOf(this.getValue()) * 1000;
            if ((ms > 0) && (ms < 60000)) {
                LOGGER.debug("   New refresh time = " + ms + " ms");
                Properties.setRefreshTime(ms);
            } else {
                LOGGER.debug("   Refresh time out of bounds: " + ms + " ms");
                this.setValue(String.valueOf(Properties.getRefreshTime() / 1_000));
            }
        }
    }

    // Define Sort Applications class
    private class SortApplications extends ConfigParam {

        public SortApplications(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            LOGGER.debug("Sort Applications update.");
            boolean newValue = Boolean.valueOf(this.getValue());
            LOGGER.debug("   New sort application value = " + newValue);
            Properties.setSortApplications(newValue);
        }
    }

    // Define load Graph x-scale class
    private class LoadGraphXScale extends ConfigParam {

        public LoadGraphXScale(String name, String value, boolean editing) {
            super(name, value, editing);
        }

        public void update() {
            // Actions after UI update
            LOGGER.debug("Load Graph x-Scale update");
            int newValue = Integer.valueOf(this.getValue());
            if (newValue >= 1) {
                LOGGER.debug("   New load Graph x-Scale update = " + newValue);
                Properties.setxScaleForLoadGraph(newValue);
            } else {
                LOGGER.debug("   The load graph value isn't correct. Reverting value.");
                this.setValue(String.valueOf(Properties.getxScaleForLoadGraph()));
            }
        }
    }


    /**
     * Initializes the default configuration parameters displayed in the UI.
     */
    @Init
    public void init() {
        LOGGER.debug("Loading configurable parameters...");
        this.configurations = new LinkedList<>();

        // Add Refresh Time
        RefreshTime rt =
            new RefreshTime("Refresh Time (s)", String.valueOf(Properties.getRefreshTime() / 1_000), false);
        this.configurations.add(rt);
        // Add Sort Applications
        SortApplications sa = new SortApplications("Sort applications (true/false)",
            String.valueOf(Properties.isSortApplications()), false);
        this.configurations.add(sa);
        // Add LoadGraph X-Scale
        LoadGraphXScale lgxs = new LoadGraphXScale("Load Graph's X-Scale factor (int >= 1)",
            String.valueOf(Properties.getxScaleForLoadGraph()), false);
        this.configurations.add(lgxs);

        LOGGER.debug("Configurable parameters loaded");
    }

    public List<ConfigParam> getConfigurations() {
        return new ListModelList<ConfigParam>(this.configurations);
    }

    @Command
    public void changeEditableStatus(@BindingParam("ConfigParam") ConfigParam cp) {
        cp.setEditingStatus(!cp.getEditingStatus());
        refreshRowTemplate(cp);
    }

    /**
     * Update the value of a configuration parameter after user edition.
     * 
     * @param cp New configuration parameter information.
     */
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
