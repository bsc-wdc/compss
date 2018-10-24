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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.AccessParams.BindingObjectAccessParams;
import es.bsc.compss.types.request.exceptions.ShutdownException;

public class FinishBindingObjectAccessRequest extends APRequest {

	BindingObjectAccessParams boAP;
	
	public FinishBindingObjectAccessRequest(BindingObjectAccessParams boAP) {
		this.boAP = boAP;
	}

	@Override
	public APRequestType getRequestType() {
		return APRequestType.FINISH_ACCESS_FILE;
	}

	@Override
	public void process(AccessProcessor ap, TaskAnalyser ta,
			DataInfoProvider dip, TaskDispatcher td) throws ShutdownException {
		dip.finishBindingObjectAccess(boAP.getMode(), boAP.getCode());
	}

}
