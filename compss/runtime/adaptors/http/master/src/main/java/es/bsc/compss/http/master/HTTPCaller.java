/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.http.master;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.implementations.HTTPImplementation;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class HTTPCaller extends RequestDispatcher<HTTPJob> {

    private static final String SUBMIT_ERROR = "Error calling HTTP Service";
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String URL_PARAMETER_OPEN_TOKEN = "\\{\\{";
    private static final String URL_PARAMETER_CLOSE_TOKEN = "\\}\\}";


    public HTTPCaller(RequestQueue<HTTPJob> queue) {
        super(queue);
    }

    @Override
    public void processRequests() {
        while (true) {
            HTTPJob job = queue.dequeue();
            if (job == null) {
                break;
            }
            try {
                final TaskDescription taskDescription = job.getTaskParams();
                final Map<String, String> namedParameters = constructMapOfNamedParameters(taskDescription);

                HTTPImplementation httpImplementation = (HTTPImplementation) job.getImplementation();

                LOGGER.debug("Executing HTTP Request...");

                HTTPInstance httpInstance = job.getResourceNode();

                final String baseUrl = httpInstance.getConfig().getBaseUrl();
                Response httpResponse = performHttpRequest(baseUrl, namedParameters, httpImplementation);

                // todo: beautify this and maybe check the empty string
                String updates = httpImplementation.getUpdates();
                formatResponse(httpResponse, httpImplementation.getProduces());
                updateResponse(httpResponse, updates, namedParameters);

                processResponse(job, httpResponse);

            } catch (Exception e) {
                JsonObject ret = new JsonObject();
                HTTPImplementation httpImplementation = (HTTPImplementation) job.getImplementation();
                ret.addProperty("$return_0", httpImplementation.getDefaultReturn());
                job.failed(JobEndStatus.EXECUTION_FAILED, ret);
                LOGGER.error(SUBMIT_ERROR, e);
            }
        }
    }

    private Map<String, String> constructMapOfNamedParameters(TaskDescription<Parameter> taskDescription)
        throws CannotLoadException {

        Map<String, String> namedParameters = new HashMap<>();

        for (Parameter par : taskDescription.getParameters()) {
            final Direction parameterDirection = par.getDirection();

            if (parameterDirection == Direction.IN || parameterDirection == Direction.INOUT
                || parameterDirection == Direction.IN_DELETE) {
                switch (par.getType()) {
                    case FILE_T:
                        DependencyParameter fileParam = (DependencyParameter) par;
                        if (par.getContentType() != null && (par.getContentType().toUpperCase().equals("FILE")
                            || par.getContentType().toUpperCase().equals("FILE_T"))) {
                            String content = readFile(fileParam.getDataTarget());
                            namedParameters.put(par.getName(), content);
                        } else {
                            String content = extractJsonString(fileParam.getDataTarget());
                            if (content != null) {
                                namedParameters.put(par.getName(), content);
                            } else {
                                LOGGER.warn("UNSUPPORTED JSON PARAMETER IN HTTP TASK: " + fileParam.getName());
                            }
                        }
                        break;
                    case OBJECT_T:
                    case PSCO_T:
                    case EXTERNAL_PSCO_T:
                        DependencyParameter dependencyParameter = (DependencyParameter) par;
                        final Object objectValue = getObjectValue(dependencyParameter);
                        addParameterToMapOfParameters(namedParameters, par, objectValue);
                        break;
                    case STREAM_T:
                    case EXTERNAL_STREAM_T:
                        LOGGER.error("Error: HTTP CAN'T USE STREAMS AS PARAMETERS!");
                        // Skip
                        break;
                    case BINDING_OBJECT_T:
                        LOGGER.error("Error: HTTP CAN'T USE BINDING OBJECTS AS PARAMETERS!");
                        // Skip
                        break;
                    case COLLECTION_T:
                    case DICT_COLLECTION_T:
                        LOGGER.warn("Error: HTTP CAN'T USE DICTIONARY COLLECTION OBJECTS AS PARAMETERS! SKIPPING");
                        continue;
                    default:
                        // Basic or String
                        BasicTypeParameter basicTypeParameter = (BasicTypeParameter) par;
                        addParameterToMapOfParameters(namedParameters, par, basicTypeParameter.getValue());
                }
            } else if (parameterDirection == Direction.OUT) {
                LOGGER.debug("Out parameter of HTTPCaller: " + par);
            }
        }
        return namedParameters;
    }

    // todo: move it somewhere else
    private static String readFile(String fileName) {
        File f = new File(fileName);
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents;
    }

    private String extractJsonString(String fileName) {
        String jsonContent = readFile(fileName).substring(4);
        JsonElement jsonElement = JsonParser.parseString(jsonContent);
        if (jsonElement.isJsonPrimitive()) {
            return jsonElement.getAsJsonPrimitive().toString();
        } else {
            if (jsonElement.isJsonObject()) {
                return jsonElement.toString();
            } else {
                if (jsonElement.isJsonArray()) {
                    return jsonElement.getAsJsonArray().toString();
                }
            }
        }
        return null;
    }

    private Object getObjectValue(DependencyParameter dp) throws CannotLoadException {
        final DataAccessId dataAccessId = dp.getDataAccessId();
        DataInstanceId dataInstanceId = ((DataAccessId.ReadingDataAccessId) dataAccessId).getReadDataInstance();
        String renaming = dataInstanceId.getRenaming();
        LogicalData logicalData = Comm.getData(renaming);

        if (!logicalData.isInMemory()) {
            logicalData.loadFromStorage();
        }
        return logicalData.getValue();
    }

    private void addParameterToMapOfParameters(Map<String, String> namedParameters, Parameter par, Object o) {
        String key = par.getName();
        if (key != null && !key.isEmpty()) {
            final String s = convertObjectToString(o);
            final String value = String.valueOf(s);
            namedParameters.put(key, value);
        }
    }

    private String convertObjectToString(Object o) {
        if (o instanceof Integer) {
            return Integer.toString((Integer) o);
        }
        if (o instanceof Float) {
            return String.valueOf(o);
        }
        return (String) o;
    }

    private Response performHttpRequest(String baseUrl, final Map<String, String> namedParameters,
        final HTTPImplementation httpImplementation) throws IOException {

        final String fullUrl = baseUrl + httpImplementation.getResource();

        final String requestType = httpImplementation.getRequest();
        final String parsedUrl = URLReplacer.replaceUrlParameters(fullUrl, namedParameters, URL_PARAMETER_OPEN_TOKEN,
            URL_PARAMETER_CLOSE_TOKEN);

        // nm:
        // todo: (do not) read file content
        String payload = httpImplementation.getPayload();
        String payloadType = httpImplementation.getPayloadType();

        payload =
            URLReplacer.formatPayload(payload, namedParameters, URL_PARAMETER_OPEN_TOKEN, URL_PARAMETER_CLOSE_TOKEN);

        return HTTPController.performRequestAndGetResponse(requestType, parsedUrl, payload, payloadType);
    }

    private void formatResponse(Response response, String produces) {

        if (produces == null || produces.equals(Constants.UNASSIGNED) || produces.equals("null")
            || produces.equals("#")) {
            JsonElement respBodyElem = JsonParser.parseString(response.getResponseBody().toString());
            JsonObject newBody = new JsonObject();
            newBody.add("$return_0", respBodyElem);
            response.setResponseBody(newBody);
            return;
        }

        JsonElement element = JsonParser.parseString(produces);
        JsonObject producesJSONObj = element.getAsJsonObject();
        Map<Object, String> paths = new HashMap();

        extractPaths(producesJSONObj, paths, "");

        JsonElement bodyJsonElement = JsonParser.parseString(response.getResponseBody().toString());
        JsonObject bodyJsonObject = bodyJsonElement.getAsJsonObject();

        JsonObject newBody = new JsonObject();

        for (Object key : paths.keySet()) {
            String keyString = paths.get(key).replaceFirst(",", "");
            newBody.add((String) key, extractValueFromJsonResponse(bodyJsonObject, keyString));
        }

        response.setResponseBody(newBody);
    }

    private JsonElement extractValueFromJsonResponse(JsonObject json, String keyString) {
        String[] keys = keyString.split(",");
        for (int i = 0; i < keys.length - 1; i++) {
            json = json.getAsJsonObject(keys[0]);
        }
        return json.get(keys[keys.length - 1]);
    }

    private void extractPaths(JsonObject produces, Map<Object, String> map, String previousPath) {
        for (Object key : produces.keySet()) {
            String keyStr = (String) key;
            Object value = produces.get(keyStr);
            String path = "";

            if (value instanceof JsonObject) {
                path = previousPath + "," + keyStr;
                extractPaths((JsonObject) value, map, path);
            } else {
                String retKey = produces.getAsJsonPrimitive(keyStr).getAsString();
                map.put(formatKey(retKey), previousPath + "," + keyStr);
            }
        }
    }

    private String formatKey(String key) {
        return key.replaceAll(URL_PARAMETER_OPEN_TOKEN, "\\$").replaceAll(URL_PARAMETER_CLOSE_TOKEN, "");
    }

    private String[] extractKeys(String produces) {
        // todo: beautify this
        String[] keys = produces.replaceAll("#", "").split(",");
        for (int i = 0; i < keys.length; i++) {
            String tmp = keys[i].trim();
            tmp = tmp.replaceAll(URL_PARAMETER_OPEN_TOKEN, "");
            tmp = tmp.replaceAll(URL_PARAMETER_CLOSE_TOKEN, "");
            keys[i] = tmp;
        }
        return keys;
    }

    private void updateResponse(Response response, String updates, Map<String, String> namedParameters) {

        if (updates == null || updates.equals(Constants.UNASSIGNED) || updates.equals("null") || updates.equals("#")) {
            return;
        }

        // todo: add try catch
        String tbu = updates.split("=")[0];

        String tbuName = tbu.split("\\.")[0].trim();
        tbuName = tbuName.replaceAll(URL_PARAMETER_OPEN_TOKEN, "").replaceAll(URL_PARAMETER_CLOSE_TOKEN, "");

        String tbuKey = tbu.split("\\.")[1].trim();

        JsonElement outParamEl = JsonParser.parseString(namedParameters.get(tbuName));
        JsonObject outParam = outParamEl.getAsJsonObject();
        if (outParam.has(tbuKey)) {
            outParam.remove(tbuKey);
        }
        String subsParName = updates.split("=")[1].trim();
        JsonObject resBody = (JsonObject) response.getResponseBody();
        JsonElement newValue = resBody.get(formatKey(subsParName));

        outParam.add(tbuKey, newValue);

        resBody.remove(tbuName);
        resBody.add(tbuName, outParam);
        response.setResponseBody(resBody);
    }

    private void processResponse(HTTPJob job, final Response response) {
        int httpResponseCode = response.getResponseCode();

        if (httpResponseCode >= 200 && httpResponseCode < 300) {
            LOGGER.debug("Correct HTTP response with response code " + httpResponseCode);
            job.completed((JsonObject) response.getResponseBody());
        } else {
            LOGGER.debug("Job failing due to wrong HTTP response with response code " + httpResponseCode);
            job.failed(JobEndStatus.EXECUTION_FAILED);
        }
    }
}
