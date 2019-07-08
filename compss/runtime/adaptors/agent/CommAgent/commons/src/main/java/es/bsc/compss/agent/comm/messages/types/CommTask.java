/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.agent.comm.messages.types;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.job.JobHistory;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


/**
 * This class is a container describing a task that is submitted to a CommAgent.
 *
 */
public class CommTask extends NIOTask {

    private String cei;

    public CommTask() {
    }

    public CommTask(COMPSsConstants.Lang lang, boolean workerDebug, AbstractMethodImplementation impl, String cei,
            LinkedList<NIOParam> arguments, NIOParam target, LinkedList<NIOParam> results,
            List<String> slaveWorkersNodeNames,
            int taskId, int jobId, JobHistory hist, int transferGroupId) {
        super(lang, workerDebug, impl,
                arguments, target, results,
                slaveWorkersNodeNames,
                taskId, jobId, hist, transferGroupId);
        this.cei = cei;
    }

    public String getCei() {
        return cei;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        boolean ceiDefined = in.readBoolean();
        if (ceiDefined) {
            cei = in.readUTF();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        boolean ceiDefined = cei != null && !cei.isEmpty();
        out.writeBoolean(ceiDefined);
        if (ceiDefined) {
            out.writeUTF(cei);
        }
    }

}
