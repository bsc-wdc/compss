package integratedtoolkit.util;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.description.CloudInstanceTypeDescription;
import org.json.JSONObject;
import org.json.JSONException;

public class JSONStateManager {

    //private static final String JSON_DATA = "{}";
    //private static final String JSON_DATA = "{\"resources\":{},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":590,\"minTime\":590}}}";
    //private static final String JSON_DATA = "{\"resources\":{\"COMPSsWorker02\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    //private static final String JSON_DATA = "{\"resources\":{},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"extraInfo\":35,\"maxTime\":620,\"executions\":2,\"avgTime\":590,\"minTime\":590}}}";
    //private static final String JSON_DATA = "{\"resources\":{\"COMPSsWorker01\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    //private static final String JSON_DATA = "{\"resources\":{\"COMPSsWorker01\":{\"implementations\":{\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}},\"implementations\":{\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}";
    //private static final String JSON_DATA = "{}";/*
    private static final String JSON_DATA = "{"
            + "\"cloud\":{"
            + "\"BSC\":{"
            + "\"small\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}},"
            + "\"big\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":62,\"executions\":2,\"avgTime\":60,\"minTime\":59},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":62,\"executions\":2,\"avgTime\":60,\"minTime\":59}}},"
            + "}"
            + "},"
            + "\"resources\":{"
            + "\"COMPSsWorker02\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}},"
            + "\"COMPSsWorker01\":{\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":2,\"avgTime\":600,\"minTime\":590}}}"
            + "},"
            + "\"implementations\":{\"increment(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":4,\"avgTime\":600,\"minTime\":590},\"reduce(FILE_T)simple.SimpleImpl\":{\"maxTime\":620,\"executions\":4,\"avgTime\":600,\"minTime\":590}}}";
    //*/

    private final JSONObject jsonRepresentation;

    public JSONStateManager() {
        jsonRepresentation = new JSONObject(JSON_DATA);
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
        //Increasing Implementation stats
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
        //Attaching new resource to the resources
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
}
