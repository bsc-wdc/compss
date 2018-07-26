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
package es.bsc.compss.gat.worker.implementations;

import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.StorageInvoker;
import es.bsc.compss.types.execution.InvocationContext;
import java.io.File;


public class ExternalMethodDefinition extends JavaMethodDefinition {

    public ExternalMethodDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx);
    }

    @Override
    public Invoker getInvoker(InvocationContext context, File sandBoxDir) throws JobExecutionException {
        return new StorageInvoker(context, this, sandBoxDir, null);
    }

}
