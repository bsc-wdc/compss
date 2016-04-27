package integratedtoolkit.connectors.rocci;

import java.util.ArrayList;
import java.util.HashMap;

import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class ROCCI extends AbstractSSHConnector {
	
    private static final String ROCCI_CLIENT_VERSION = "4.2.5";
    
    private static final Integer RETRY_TIME = 5; 				// Seconds
    private static final long DEFAULT_TIME_SLOT = FIVE_MIN;		// Minutes

    private RocciClient client;
	private ArrayList<String> cmd_string;
    private String attributes = "Not Defined";
    
	private static Integer MAX_VM_CREATION_TIME = 10; 	// Minutes
    private static Integer MAX_ALLOWED_ERRORS = 3;		// Number of maximum errors
    private long timeSlot = DEFAULT_TIME_SLOT;
    
    
    public ROCCI(String providerName, HashMap<String, String> props) {
		super(providerName, props);
		
		logger.info("Initializing rOCCI connector with version" + ROCCI_CLIENT_VERSION);
        cmd_string = new ArrayList<String>();
        //user@hostname:~$ occi -v
        // 4.2.5
        /*Usage: occi [OPTIONS]

                     Options:
                         -e, --endpoint URI               OCCI server URI, defaults to "http://localhost:3000"
                         -n, --auth METHOD                Authentication method, only: [x509|basic|digest|none], defaults to "none"
                         -k, --timeout SEC                Default timeout for all HTTP connections, in seconds
                         -u, --username USER              Username for basic or digest authentication, defaults to "anonymous"
                         -p, --password PASSWORD          Password for basic, digest and x509 authentication
                         -c, --ca-path PATH               Path to CA certificates directory, defaults to "/etc/grid-security/certificates"
                         -f, --ca-file PATH               Path to CA certificates in a file
                         -s, --skip-ca-check              Skip server certificate verification [NOT recommended]
                         -F, --filter CATEGORY            Category type identifier to filter categories from model, must be used together with the -m option
                         -x, --user-cred FILE             Path to user's x509 credentials, defaults to "/home/user/.globus/usercred.pem"
                         -X, --voms                       Using VOMS credentials; modifies behavior of the X509 authN module
                         -y, --media-type MEDIA_TYPE      Media type for client <-> server communication, only: [application/occi+json|text/plain,text/occi|text/plain|text/occi], defaults to "text/plain,text/occi"
                         -r, --resource RESOURCE          Term, identifier or URI of a resource to be queried, required
                         -t, --attribute ATTR             An "attribute='value'" pair, mandatory attrs for creating new resource instances: [occi.core.title]
                         -T, --context CTX_VAR            A "context_variable='value'" pair for new 'compute' resource instances, only: [public_key, user_data]
                         -a, --action ACTION              Action to be performed on a resource instance, required
                         -M, --mixin IDENTIFIER           Identifier of a mixin, formatted as SCHEME#TERM or SHORT_SCHEME#TERM
                         -j, --link URI                   URI of an instance to be linked with the given resource, applicable only for action 'link'
                         -g, --trigger-action ACTION      Action to be triggered on the resource, formatted as SCHEME#TERM or TERM
                         -l, --log-to OUTPUT              Log to the specified device, only: [stdout|stderr], defaults to 'stderr'
                         -o, --output-format FORMAT       Output format, only: [json|plain|json_pretty|json_extended|json_extended_pretty], defaults to "plain"
                         -b, --log-level LEVEL            Set the specified logging level, only: [debug|error|fatal|info|unknown|warn]
                         -z, --examples                   Show usage examples
                         -m, --dump-model                 Contact the endpoint and dump its model
                         -d, --debug                      Enable debugging messages
                         -h, --help                       Show this message
                         -v, --version                    Show version
        */
        
        // ROCCI client parameters setup
        if (props.get("Server") != null) {
            cmd_string.add("--endpoint " + props.get("Server"));
        }

        if (props.get("auth") != null) {
            cmd_string.add("--auth " + props.get("auth"));
        }
        
        if (props.get("timeout") != null) {
            cmd_string.add("--timeout " + props.get("timeout"));
        }

        if (props.get("username") != null) {
            cmd_string.add("--username " + props.get("username"));
        }

        if (props.get("password") != null) {
            cmd_string.add("--password " + props.get("password"));
        }

        if (props.get("ca-path") != null) {
            cmd_string.add("--ca-path " + props.get("ca-path"));
        }

        if (props.get("ca-file") != null) {
            cmd_string.add("--ca-file " + props.get("ca-file"));
        }

        if (props.get("skip-ca-check") != null) {
            cmd_string.add("--skip-ca-check " + props.get("skip-ca-check"));
        }

        if (props.get("filter") != null) {
            cmd_string.add("--filter " + props.get("filter"));
        }

        if (props.get("user-cred") != null) {
            cmd_string.add("--user-cred " + props.get("user-cred"));
        }

        if (props.get("voms") != null) {
            cmd_string.add("--voms");
        }

        if (props.get("media-type") != null) {
            cmd_string.add("--media-type " + props.get("media-type"));
        }

        if (props.get("resource") != null)
           cmd_string.add("--resource "+props.get("resource"));

        if (props.get("attributes") != null)
           cmd_string.add("--attributes "+props.get("attributes"));
        
        if (props.get("context") != null) {
            cmd_string.add("--context " + props.get("context"));
        }

        if (props.get("action") != null)
        	   cmd_string.add("--action "+props.get("action"));
        
        if (props.get("mixin") != null)
        	   cmd_string.add("--mixin "+props.get("mixin"));
        
        if (props.get("link") != null) {
            cmd_string.add("--link " + props.get("link"));
        }

        if (props.get("trigger-action") != null) {
            cmd_string.add("--trigger-action " + props.get("trigger-action"));
        }

        if (props.get("log-to") != null) {
            cmd_string.add("--log-to " + props.get("log-to"));
        }

        /*if (props.get("output-format") != null) {
            cmd_string.add("--output-format " + props.get("output-format"));
        }*/
        cmd_string.add("--output-format json_extended_pretty");

        if (props.get("dump-model") != null) {
            cmd_string.add("--dump-model");
        }

        if (props.get("debug") != null) {
            cmd_string.add("--debug");
        }

        if (props.get("verbose") != null) {
            cmd_string.add("--verbose");
        }

        // ROCCI connector parameters setup
        if (props.get("max-vm-creation-time") != null) {
            MAX_VM_CREATION_TIME = Integer.parseInt(props.get("max-vm-creation-time"));
        }

        if (props.get("max-connection-errors") != null) {
            MAX_ALLOWED_ERRORS = Integer.parseInt(props.get("max-connection-errors"));
        }

        if (props.get("owner") != null && props.get("jobname") != null) {
            attributes = props.get("owner") + "-" + props.get("jobname");
        }  
        String time = props.get("time-slot");
        if (time != null) {
            timeSlot = Integer.parseInt(time)*1000;
        }else{
        	timeSlot = DEFAULT_TIME_SLOT;
        }

        client = new RocciClient(cmd_string, attributes, logger);
	}

	@Override
	public Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException {
		logger.info("Creating a VM with rOCCI connector.");
		try {
        	String instanceCode = rd.getType();
        	String vmId = client.create_compute(rd.getImage().getImageName(), instanceCode);
        	if (debug){
        		logger.debug("VM "+ vmId + " created");
        	}
        	return vmId;
        } catch (Exception e) {
            logger.error("Error creating a VM", e);
            throw new ConnectorException(e);
        }
	}

	@Override
	public void destroy(Object vm) throws ConnectorException {
		String vmId = (String) vm;
		logger.info(" Destroy VM "+vmId+" with rOCCI connector");
        client.delete_compute(vmId);
	}

	@Override
	public CloudMethodResourceDescription waitUntilCreation(Object vm, CloudMethodResourceDescription requested) throws ConnectorException {
		String vmId = vm.toString();
		logger.info("Waiting until VM "+ vmId +" is created");
		CloudMethodResourceDescription granted = new CloudMethodResourceDescription();
        Integer polls = 0;
        int errors = 0;
       
        String status = null;

        status = client.get_resource_status(vmId);

        try {
			Thread.sleep(RETRY_TIME * 1000);
		} catch (InterruptedException e1) {
			logger.warn("Sleep Interrumped", e1);
		}
       
        //Possible status: waiting, active and inactive
	    while (!status.equals("active")) {
            try {
                polls++;
                Thread.sleep(RETRY_TIME  * 1000);
                if (RETRY_TIME  * polls >= MAX_VM_CREATION_TIME * 60) {
                	logger.error("Maximum VM waiting for creation time reached.");
                	throw new ConnectorException("Maximum VM creation time reached.");
                }
                status = client.get_resource_status(vmId);
                errors = 0;
            } catch (Exception e) {
                errors++;
                if (errors == MAX_ALLOWED_ERRORS) {
                    logger.error("ERROR_MSG = [\n\tError = " + e.getMessage() + "\n]");
                    throw new ConnectorException("Error getting the status of the request");
                }
            }
        }
        String IP = client.get_resource_address(vmId);
        
        granted.copy(requested);
        granted.setName(IP);
        granted.setOperatingSystemType("Linux");
        granted.setValue(getMachineCostPerTimeSlot(granted));
        
		return granted;
	}

	@Override
	public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
		return rd.getValue();
	}

	@Override
	public long getTimeSlot() {
		return timeSlot;
	}
	
	@Override
	protected void close(){
	  //Nothing to do
	}

}
