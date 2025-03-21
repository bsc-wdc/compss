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
package es.bsc.compss.gat.master.configuration;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.gat.master.GATAdaptor;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;

import java.util.Map.Entry;

import org.gridlab.gat.GATContext;


public class GATConfiguration extends MethodConfiguration {

    private GATContext context;
    private boolean usingGlobus = false;
    private boolean userNeeded = false;
    private String queue = "";


    /**
     * Creates a new GATConfiguration instance with the associated adaptor name and broker adaptor.
     * 
     * @param adaptorName Adaptor name.
     * @param brokerAdaptorName Broker adaptor name.
     */
    public GATConfiguration(String adaptorName, String brokerAdaptorName) {
        super(adaptorName);

        initContext(brokerAdaptorName, System.getProperty(COMPSsConstants.GAT_FILE_ADAPTOR));

        for (Entry<String, String> entry : super.getAdditionalProperties().entrySet()) {
            String propName = entry.getKey();
            String propValue = entry.getValue();
            if (propName.startsWith("[context=job]")) {
                propName = propName.substring(13);
                this.addContextPreference(propName, propValue);
            } else if (propName.startsWith("[context=file]")) {
                propName = propName.substring(14);
                this.addContextPreference(propName, propValue);
                GATAdaptor.addTransferContextPreferences(propName.substring(14), propValue);
            }
        }
    }

    /**
     * Copies the given GATConfiguration.
     * 
     * @param clone The GATConfiguration to copy.
     */
    public GATConfiguration(GATConfiguration clone) {
        super(clone);
        this.context = clone.context; // TODO: check if context should be cloned or this assignation is OK
        this.usingGlobus = clone.usingGlobus;
        this.userNeeded = clone.userNeeded;
        this.queue = clone.queue;
    }

    @Override
    public MethodConfiguration copy() {
        return new GATConfiguration(this);
    }

    private void initContext(String brokerAdaptor, String fileAdaptor) {
        this.context = new GATContext();
        this.context.addPreference("ResourceBroker.adaptor.name", brokerAdaptor);
        this.context.addPreference("File.adaptor.name", fileAdaptor + ", srcToLocalToDestCopy, local");

        this.usingGlobus = brokerAdaptor.equalsIgnoreCase("globus");
        this.userNeeded = brokerAdaptor.regionMatches(true, 0, "ssh", 0, 3);
    }

    /**
     * Returns the GAT context.
     * 
     * @return The GAT context.
     */
    public GATContext getContext() {
        return this.context;
    }

    /**
     * Sets a new GAT context.
     * 
     * @param context The new GAT context.
     */
    public void setContext(GATContext context) {
        this.context = context;
    }

    /**
     * Adds a preference to the current context.
     * 
     * @param key Preference key.
     * @param value Preference value.
     */
    public void addContextPreference(String key, String value) {
        this.context.addPreference(key, value);
    }

    /**
     * Returns whether the configuration requires globus or not.
     * 
     * @return {@literal true} if the configuration requires globus, {@literal false} otherwise.
     */
    public boolean isUsingGlobus() {
        return this.usingGlobus;
    }

    /**
     * Sets a new value for the globus property.
     * 
     * @param usingGlobus New globus property.
     */
    public void setUsingGlobus(boolean usingGlobus) {
        this.usingGlobus = usingGlobus;
    }

    /**
     * Returns whether the configuration requires a user to login the worker node or not.
     * 
     * @return {@literal true} if the configuration requires a user to login the worker node, {@literal false}
     *         otherwise.
     */
    public boolean isUserNeeded() {
        return this.userNeeded;
    }

    /**
     * Sets a new value for the user property.
     * 
     * @param userNeeded New user property.
     */
    public void setUserNeeded(boolean userNeeded) {
        this.userNeeded = userNeeded;
    }

    /**
     * Returns the queue system value.
     * 
     * @return The queue system value.
     */
    public String getQueue() {
        return this.queue;
    }

    /**
     * Sets a new queue system value.
     * 
     * @param queue New queue system value.
     */
    public void setQueue(String queue) {
        this.queue = queue;
    }

}
