package es.bsc.compss.nio.utils;

import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.exceptions.CommException.ErrorType;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.compss.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NIOBindingObjectReceiver implements Runnable {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    
    NIOBindingObjectStream nbos;
    String id;
    int type;
    NIOConnection c;
    
    public NIOBindingObjectReceiver(NIOConnection c, String id, int type, NIOBindingObjectStream nbos) {
        this.nbos = nbos;
        this.id = id;
        this.type = type;
        this.c = c;
    }

    public void run() {
      int res = NIOBindingDataManager.receiveNativeObject(id, type, nbos);
      if (res != 0){
          LOGGER.error("Error ("+res+") receiving native object "+id);
          c.error(new CommException(ErrorType.NIO, "Error ("+res+") receiving native object."));
      }else{
          NIOBindingDataManager.objectReceived(c);
      }
      c.finishConnection();
      
    }
}