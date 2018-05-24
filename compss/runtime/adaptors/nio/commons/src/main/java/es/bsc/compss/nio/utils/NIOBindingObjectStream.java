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
