/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import org.gridlab.gat.GATContext;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.gat.master.GATAdaptor;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;


public class GATConfiguration extends MethodConfiguration {

    private GATContext context;
    private boolean usingGlobus = false;
    private boolean userNeeded = false;
    private String queue = "";


    public GATConfiguration(String adaptorName, String brokerAdaptorName) {
        super(adaptorName);

        initContext(brokerAdaptorName, System.getProperty(COMPSsConstants.GAT_FILE_ADAPTOR));

        for (java.util.Map.Entry<String, String> entry : super.getAdditionalProperties().entrySet()) {
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

    public GATConfiguration(GATConfiguration clone){
        super(clone);
        context = clone.context; // TODO: check if context should be cloned or this assignation is OK
        usingGlobus = clone.usingGlobus;
        userNeeded = clone.userNeeded;
        queue = clone.queue;
    }

    @Override
    public MethodConfiguration copy(){
        return new GATConfiguration(this);
    }


    private void initContext(String brokerAdaptor, String fileAdaptor) {
        this.context = new GATContext();
        this.context.addPreference("ResourceBroker.adaptor.name", brokerAdaptor);
        this.context.addPreference("File.adaptor.name", fileAdaptor + ", srcToLocalToDestCopy, local");

        this.usingGlobus = brokerAdaptor.equalsIgnoreCase("globus");
        this.userNeeded = brokerAdaptor.regionMatches(true, 0, "ssh", 0, 3);
    }

    public GATContext getContext() {
        return context;
    }

    public void setContext(GATContext context) {
        this.context = context;
    }

    public void addContextPreference(String key, String value) {
        this.context.addPreference(key, value);
    }

    public boolean isUsingGlobus() {
        return usingGlobus;
    }

    public void setUsingGlobus(boolean usingGlobus) {
        this.usingGlobus = usingGlobus;
    }

    public boolean isUserNeeded() {
        return userNeeded;
    }

    public void setUserNeeded(boolean userNeeded) {
        this.userNeeded = userNeeded;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

}
