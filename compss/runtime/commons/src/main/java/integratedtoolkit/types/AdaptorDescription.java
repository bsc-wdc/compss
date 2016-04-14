package integratedtoolkit.types;

import java.util.TreeMap;


public class AdaptorDescription  {

    public final static String NIOAdaptor = "integratedtoolkit.nio.master.NIOAdaptor";
    public final static String GATAdaptor = "integratedtoolkit.gat.master.GATAdaptor";
    public final static String WSAdaptor = "integratedtoolkit.ws.master.WSAdaptor";

    public final static String MIN_PORT = "MinPort";
    public final static String MAX_PORT = "MaxPort";
    public final static String BROKER_ADAPTOR = "BrokerAdaptor";
    public final static String DEFAULT_BROKER_ADAPTOR = "sshtrilead";
    private int minPort = 0;
    private int maxPort = 0;
    private String brokerAdaptor = DEFAULT_BROKER_ADAPTOR;
    private final String name; 

    public AdaptorDescription(String name) {
        this.name = name;
    }

    public AdaptorDescription(String name, int minPort, int maxPort, String brokerAdaptor) {
        this(name);
        if (brokerAdaptor!=null&&!brokerAdaptor.isEmpty()){
        	this.brokerAdaptor = brokerAdaptor;
        }
        this.minPort = minPort;
        this.maxPort = maxPort;
    }

    public String getName(){
        return this.name;
    }     

    public int[] getPortRange(){
        return new int[]{minPort, maxPort};
    }
    
    public void setPortRange(int minPort, int maxPort){
        this.minPort = minPort;
        this.maxPort = maxPort;
    }
    
    public boolean portRangeIsValid(){
        return (minPort > 0 && maxPort >= minPort);
    }
    
    public void checkAdaptorProperties() throws Exception{
    	if (!this.name.equals(GATAdaptor) && !portRangeIsValid()){
            String message = "No valid port range provided. [project.xml and resources.xml need to define overlapping ranges (if project is set)]";
            System.out.println(message);
            throw new Exception(message);
        }
    }
    
    public static TreeMap<String, AdaptorDescription> merge(TreeMap<String, AdaptorDescription> projectAdaptorsDesc, TreeMap<String, AdaptorDescription> resourceAdaptorsDesc) throws Exception{
        if (projectAdaptorsDesc.size() == 0 && resourceAdaptorsDesc.size() > 0) {
            return resourceAdaptorsDesc;
        }
        TreeMap<String, AdaptorDescription> aDesc = new TreeMap<String, AdaptorDescription>();
        
        String[] possibleAdaptors = new String[]{NIOAdaptor, GATAdaptor};
        AdaptorDescription projectAdaptor;
        AdaptorDescription resourceAdaptor;
        int[] pPorts, rPorts;
        
        for( String adaptorName : possibleAdaptors){
            projectAdaptor = projectAdaptorsDesc.get(adaptorName);
            resourceAdaptor = resourceAdaptorsDesc.get(adaptorName);

            if (resourceAdaptor != null) {
            	
 
            	String rBrokerAdaptor = resourceAdaptor.getBrokerAdaptor();
            	rPorts = resourceAdaptor.getPortRange();
                if (projectAdaptor != null) {
                	String pBrokerAdaptor = projectAdaptor.getBrokerAdaptor();
                	if (pBrokerAdaptor==null || pBrokerAdaptor.isEmpty()){
                		pBrokerAdaptor = rBrokerAdaptor;
                	}
                    pPorts = projectAdaptor.getPortRange();
                    if (resourceAdaptor.portRangeIsValid()) {
                    	int minPort = rPorts[0];
                    	int maxPort = rPorts[1];

                    	if (pPorts[0] > -1){
                    		minPort = (rPorts[0] > pPorts[0]) ? rPorts[0] : pPorts[0];
                    	}
                    	if (pPorts[1] > -1){
                    		maxPort = (rPorts[1] < pPorts[1]) ? rPorts[1] : pPorts[1];
                    	}
                    	aDesc.put(adaptorName, new AdaptorDescription(adaptorName, minPort, maxPort, pBrokerAdaptor));
                    }else if (adaptorName.equals(GATAdaptor)){
                    	aDesc.put(adaptorName, new AdaptorDescription(adaptorName, rPorts[0], rPorts[1], rBrokerAdaptor));
                    }
                } else {
                	if (adaptorName.equals(GATAdaptor)){
                		aDesc.put(adaptorName, new AdaptorDescription(adaptorName, rPorts[0] , rPorts[1], rBrokerAdaptor));
                	}else if (resourceAdaptor.portRangeIsValid()) {
                		aDesc.put(adaptorName, new AdaptorDescription(adaptorName, rPorts[0], rPorts[1], rBrokerAdaptor));
                	}
                }
            }
            
        }
        return aDesc;
    }

	public String getBrokerAdaptor() {
		return brokerAdaptor;
	}
	public void setBrokerAdaptor(String brokerAdaptor) {
		if (brokerAdaptor!=null&&!brokerAdaptor.isEmpty()){
        	this.brokerAdaptor = brokerAdaptor;
        }
	}
}

