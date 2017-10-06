package es.bsc.compss.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONException;


public class JSONStateManager {

    // private static final String JSON_DATA = "{}";
    // private static final String JSON_DATA =
    // "{\"resources\":{},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":590,\"minTime\":590}}}";
    // private static final String JSON_DATA =
    // "{\"resources\":{\"COMPSsWorker02\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    // private static final String JSON_DATA =
    // "{\"resources\":{},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"extraInfo\":35,\"maxTime\":620,\"executions\":2,\"avgTime\":590,\"minTime\":590}}}";
    // private static final String JSON_DATA =
    // "{\"resources\":{\"COMPSsWorker01\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    // private static final String JSON_DATA =
    // "{\"resources\":{\"COMPSsWorker01\":{\"implementations\":{\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    // private static final String JSON_DATA = "{}";/*
    private static final String JSON_DATA = "{" + "\"cloud\":{" + "\"BSC\":{"
            + "\"small\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}},"
            + "\"big\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":62,\"executions\":2,\"avgTime\":60,\"minTime\":59},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":62,\"executions\":2,\"avgTime\":60,\"minTime\":59}}},"
            + "}" + "}," + "\"resources\":{"
            + "\"COMPSsWorker02\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}},"
            + "\"COMPSsWorker01\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}"
            + "},"
            + "\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":4,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":4,\"avgTime\":600,\"minTime\":590}}}";
    // */

    private final JSONObject jsonRepresentation;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    
    
    public JSONStateManager(){
    	String objectStr = "{}";
    	String inputProfile = System.getProperty(COMPSsConstants.INPUT_PROFILE);
    	if (inputProfile!=null && !inputProfile.isEmpty()){
    		LOGGER.debug("Input profile detected. Reading from file "+ inputProfile);
    		objectStr = manageInputProfile(inputProfile);
    	}
  
    	jsonRepresentation = new JSONObject(objectStr);
    	init();
    }
    
    private String manageInputProfile(String inputProfile) {
    	File f = new File (inputProfile);
    	try {
            byte[] bytes = Files.readAllBytes(f.toPath());
            return new String(bytes,"UTF-8");
        } catch (FileNotFoundException e) {
            ErrorManager.warn("Error loading profile. File "+ f.getAbsolutePath() + " not found. Using default values");
            return "{}";
        } catch (IOException e) {
        	ErrorManager.warn("Error loading profile. Exception reading "+ f.getAbsolutePath() + ". Using default values", e);
        	return "{}";
        }
		
	}

	private void init() {
        JSONObject resources = getJSONForResources();
        if (resources == null) {
            addResources();
        }
        JSONObject implementations = getJSONForImplementations();
        if (implementations == null) {
            addImplementations();
        }
        JSONObject cloud = getJSONForCloud();
        if (cloud == null) {
            addCloud();
        }
    }

    private JSONObject addResources() {
        JSONObject resources = new JSONObject();
        jsonRepresentation.put("resources", resources);
        return resources;
    }

    private JSONObject addImplementations() {
        JSONObject implementations = new JSONObject();
        jsonRepresentation.put("implementations", implementations);
        return implementations;
    }

    private JSONObject addCloud() {
        JSONObject cloud = new JSONObject();
        jsonRepresentation.put("cloud", cloud);
        return cloud;
    }

    public JSONObject getJSONForImplementations() {
        try {
            return jsonRepresentation.getJSONObject("implementations");
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public JSONObject getJSONForImplementation(int coreId, int implId) {
        try {
            JSONObject implsJSON = jsonRepresentation.getJSONObject("implementations");
            String signature = CoreManager.getSignature(coreId, implId);
            return implsJSON.getJSONObject(signature);
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public <T extends WorkerResourceDescription> JSONObject getJSONForResources() {
        try {
            return jsonRepresentation.getJSONObject("resources");
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public <T extends WorkerResourceDescription> JSONObject getJSONForResource(Worker<T> resource) {
        try {
            JSONObject resourcesJSON = getJSONForResources();
            if (resourcesJSON != null) {
                return resourcesJSON.getJSONObject(resource.getName());
            }
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public JSONObject getJSONForCloud() {
        try {
            return jsonRepresentation.getJSONObject("cloud");
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public JSONObject getJSONForCloudProvider(CloudProvider cp) {
        try {
            JSONObject cloudJSON = getJSONForCloud();
            if (cloudJSON != null) {
                return cloudJSON.getJSONObject(cp.getName());
            }
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public JSONObject getJSONForCloudInstanceTypeDescription(CloudProvider cp, CloudInstanceTypeDescription citd) {
        try {
            JSONObject cpJSON = getJSONForCloudProvider(cp);
            if (cpJSON != null) {
                return cpJSON.getJSONObject(citd.getName());
            }
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public JSONObject getJSONForImplementation(CloudProvider cp, CloudInstanceTypeDescription citd, int coreId, int implId) {
        try {
            JSONObject citdJSON = getJSONForCloudInstanceTypeDescription(cp, citd);
            if (citdJSON != null) {
                String signature = CoreManager.getSignature(coreId, implId);
                return citdJSON.getJSONObject(signature);
            }
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    public void addResourceJSON(ResourceScheduler<? extends WorkerResourceDescription> rs) {
        // Increasing Implementation stats
        int coreCount = CoreManager.getCoreCount();
        for (int coreId = 0; coreId < coreCount; coreId++) {
            for (int implId = 0; implId < CoreManager.getNumberCoreImplementations(coreId); implId++) {
                JSONObject implJSON = getJSONForImplementation(coreId, implId);
                Profile p = rs.getProfile(coreId, implId);
                if (implJSON == null) {
                    addImplementationJSON(coreId, implId, p);
                } else {
                    accumulateImplementationJSON(coreId, implId, p);
                }
            }
        }
        // Attaching new resource to the resources
        JSONObject resources = getJSONForResources();
        resources.put(rs.getName(), rs.toJSONObject());
    }

    public JSONObject updateResourceJSON(ResourceScheduler<? extends WorkerResourceDescription> rs) {
        JSONObject oldResource = getJSONForResource(rs.getResource());
        return rs.updateJSON(oldResource);
    }

    public void addImplementationJSON(int coreId, int implId, Profile profile) {
        String signature = CoreManager.getSignature(coreId, implId);
        JSONObject implsJSON = this.getJSONForImplementations();
        implsJSON.put(signature, profile.toJSONObject());
    }

    public void accumulateImplementationJSON(int coreId, int implId, Profile profile) {
        JSONObject oldImplJSON = this.getJSONForImplementation(coreId, implId);
        profile.accumulateJSON(oldImplJSON);
    }

    public String getString() {
        return jsonRepresentation.toString();
    }
    
    public void write(){
    	String outputProfile = System.getProperty(COMPSsConstants.OUTPUT_PROFILE);
    	if (outputProfile!=null && !outputProfile.isEmpty()){
    		LOGGER.debug("Output profile detected. Writting to file "+ outputProfile);
    		writeOutputProfile(outputProfile);
    	}
    }
    
    public void writeOutputProfile(String filename){
    	BufferedWriter writer = null;
    	try {
    	    writer = new BufferedWriter( new FileWriter(filename));
    	    writer.write(jsonRepresentation.toString());
    	} catch ( IOException e){
    		ErrorManager.warn("Error loading profile. Exception reading "+ filename + ".",e);
    	} finally {
    	    try {
    	        if ( writer != null)
    	        writer.close( );
    	    } catch ( IOException e){
    	    	//Nothing to do
    	    }
    	}
    }
}
