package es.bsc.compss.http.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.implementations.HTTPImplementation;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class HTTPCaller extends RequestDispatcher<HTTPJob> {

    private static final String SUBMIT_ERROR = "Error calling HTTP Service";
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String URL_PARAMETER_OPEN_TOKEN = "\\{";
    private static final String URL_PARAMETER_CLOSE_TOKEN = "\\}";


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

                Response httpResponse = performHttpRequest(namedParameters, httpImplementation);
                processResponse(job, httpResponse);

            } catch (Exception e) {
                final JobListener jobListener = job.getListener();
                jobListener.jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);

                LOGGER.error(SUBMIT_ERROR, e);
            }
        }
    }

    private void processResponse(HTTPJob job, final Response response) {
        int httpResponseCode = response.getResponseCode();

        final JobListener jobListener = job.getListener();

        if (httpResponseCode >= 200 && httpResponseCode < 300) {
            LOGGER.debug("Correct HTTP response with response code " + httpResponseCode);

            final String responseBody = response.getResponseBody();
            job.setReturnValue(responseBody);

            jobListener.jobCompleted(job);
        } else {
            LOGGER.debug("Job failing due to wrong HTTP response with response code " + httpResponseCode);

            jobListener.jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
        }
    }

    private Response performHttpRequest(final Map<String, String> namedParameters,
        final HTTPImplementation httpImplementation) throws IOException {

        final String baseUrl = httpImplementation.getBaseUrl();

        final String methodType = httpImplementation.getMethodType();
        final String parsedUrl = URLReplacer.replaceUrlParameters(baseUrl, namedParameters, URL_PARAMETER_OPEN_TOKEN,
            URL_PARAMETER_CLOSE_TOKEN);

        return HTTPController.performRequestAndGetResponse(methodType, parsedUrl);
    }

    private Map<String, String> constructMapOfNamedParameters(TaskDescription taskDescription)
        throws CannotLoadException {

        Map<String, String> namedParameters = new HashMap<>();

        for (Parameter par : taskDescription.getParameters()) {
            final Direction parameterDirection = par.getDirection();

            if (parameterDirection == Direction.IN || parameterDirection == Direction.IN_DELETE) {
                switch (par.getType()) {
                    case OBJECT_T:
                    case PSCO_T:
                    case EXTERNAL_PSCO_T:
                    case FILE_T:
                        // todo: is it the case only for pycompss?
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

    private Object getObjectValue(DependencyParameter dp) throws CannotLoadException {
        final DataAccessId dataAccessId = dp.getDataAccessId();
        final DataInstanceId dataInstanceId = ((RAccessId) dataAccessId).getReadDataInstance();
        String renaming = dataInstanceId.getRenaming();

        LogicalData logicalData = Comm.getData(renaming);

        if (!logicalData.isInMemory()) {
            logicalData.loadFromStorage();
        }
        return logicalData.getValue();
    }
}
