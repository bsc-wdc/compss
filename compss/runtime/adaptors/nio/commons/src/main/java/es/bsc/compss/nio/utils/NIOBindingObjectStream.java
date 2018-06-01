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
package es.bsc.compss.nio.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.stage.Transfer;
import es.bsc.compss.log.Loggers;

public class NIOBindingObjectStream{
    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String DBG_PREFIX = "[NIOBindingObjectStream] ";
	NIOConnection c;
	NIOBindingObjectTransferListener ncl;
	
	public NIOBindingObjectStream (NIOConnection c, NIOBindingObjectTransferListener ncl){
		this.c = c;
		this.ncl = ncl;
	}
    
    public void push(ByteBuffer b) throws IOException{
    	if (ncl == null){
    		if (b!=null){
    			if (b.hasArray()){
    				byte[] bArray = b.array();
    				LOGGER.debug( DBG_PREFIX + "Sending buffer array "+ bArray.length );
    				c.sendDataArray(b.array());
    			}else{
    				byte[] bArray = new byte[b.limit()];
    				b.get(bArray);
    				LOGGER.debug(DBG_PREFIX + "Sending array " + bArray.length);
    				c.sendDataArray(bArray);
    			}
    		}
    	}
	}
	    
	public byte[] pull() throws Exception{
	    LOGGER.debug(DBG_PREFIX +"Pulling byte array");
		ncl.addOperation();
		c.receiveDataArray();
		ncl.enable();
		LOGGER.debug(DBG_PREFIX +"Waiting to receive the data array");
		ncl.aquire();
		Transfer t = ncl.getTransfer();
		if (t.isArray()){
			byte[] bArray = ncl.getTransfer().getArray();
			LOGGER.debug(DBG_PREFIX + "Returning array of " + bArray.length);
			return bArray;
		}else{ 
			LOGGER.debug(DBG_PREFIX + "Error is not an Array");
			throw new Exception("Transfer is not an array");
		}
	}
		
	    


}
