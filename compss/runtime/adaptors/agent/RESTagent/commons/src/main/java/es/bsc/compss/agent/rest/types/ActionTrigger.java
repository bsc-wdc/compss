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
package es.bsc.compss.agent.rest.types;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;


/**
 * Class describing an action to do after a request is done.
 */
@XmlRootElement(name = "action")
public class ActionTrigger implements RESTAgentRequestListener {

    private String action;

    List<String> forwardTo;


    public ActionTrigger() {

    }

    @XmlElement(name = "actionName")
    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @XmlElementWrapper(name = "forwardTo")
    @XmlElement(name = "agent")
    public List<String> getForwardTo() {
        return this.forwardTo;
    }

    public void setForwardTo(List<String> forwardTo) {
        this.forwardTo = forwardTo;
    }

    @Override
    public String toString() {
        return action;
    }

    @Override
    public void requestCompleted(RESTAgentRequestHandler handler) {
        if ("stop".equalsIgnoreCase(action)) {
            handler.powerOff(forwardTo);
        }
    }
}
