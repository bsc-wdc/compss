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
package es.bsc.compss.types.tracing.paraver;

import es.bsc.compss.types.tracing.CPU;
import es.bsc.compss.types.tracing.SystemComposition;


public class PRVNode extends SystemComposition<CPU> {

    private int nodeId;


    public PRVNode(int nodeId, String label) {
        super(label);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    /**
     * debug method.
     *
     * @param pad pad
     * @return message
     */
    public String print(String pad) {
        StringBuilder s = new StringBuilder(pad + this.getLabel() + "\n");
        for (CPU component : this.getSubComponents()) {
            s.append(component.print(pad + "\t"));
        }
        return s.toString();
    }

    public void applyOffset(int nodeOffset) {
        nodeId = this.nodeId + nodeOffset;
    }
}
