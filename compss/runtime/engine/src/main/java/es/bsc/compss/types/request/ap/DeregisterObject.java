package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
//import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.ObjectInfo;
import es.bsc.compss.types.request.exceptions.ShutdownException;

public class DeregisterObject extends APRequest {

	int hash_code;
		
	public DeregisterObject(Object o) {
		
		hash_code = o.hashCode();
		
	}
	
	@Override
	public APRequestType getRequestType() {
	
		return APRequestType.DEREGISTER_OBJECT;
	
	}

	@Override
	public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
			throws ShutdownException {
		
		ObjectInfo objectInfo = dip.deleteData(hash_code);

		if (objectInfo == null) {
			LOGGER.info("The object with code: " + String.valueOf(hash_code) + " is not used by any task");
			
			return;
			//I think it's not possible to enter here, the problem we had was that
			//they were not deleted, but I think it's mandatory to log out what happens
		}
		else {
			LOGGER.info("Data of : " + String.valueOf(hash_code) + " deleted");
		}	
		//At this point all the ObjectInfo versions (renamings) are 
		//out of the DataInfoProvider data structures
		
		ta.deleteData(objectInfo);
		
	}

}
