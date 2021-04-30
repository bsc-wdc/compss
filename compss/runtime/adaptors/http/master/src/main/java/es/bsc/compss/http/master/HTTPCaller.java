package es.bsc.compss.http.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.implementations.HTTPImplementation;
import es.bsc.compss.types.job.JobEndStatus;
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
    public static final String URL_PARAMETER_OPEN_TOKEN = "\\{";
    public static final String URL_PARAMETER_CLOSE_TOKEN = "\\}";


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
                Map<String, String> namedParameters = new HashMap<>();

                final TaskDescription taskDescription = job.getTaskParams();
                processTaskParameters(taskDescription, namedParameters);

                HTTPImplementation httpImplementation = (HTTPImplementation) job.getImplementation();

                LOGGER.debug("Executing HTTP request...");

                Response httpResponse = performHttpRequest(namedParameters, httpImplementation);
                processResponse(job, httpResponse);

            } catch (Exception e) {
                job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
                LOGGER.error(SUBMIT_ERROR, e);
            }
        }
    }

    private void processResponse(HTTPJob job, final Response response) {
        int httpResponseCode = response.getResponseCode();

        if (httpResponseCode >= 200 && httpResponseCode < 300) {
            LOGGER.debug("Correct HTTP response with response code " + httpResponseCode);

            job.setReturnValue(response.getResponseBody());
            job.getListener().jobCompleted(job);
        } else {
            LOGGER.debug("Wrong HTTP response with response code " + httpResponseCode);
            LOGGER.debug("Job failing due to wrong HTTP response");

            job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
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

    private void processTaskParameters(TaskDescription taskDescription, Map<String, String> namedParameters)
        throws CannotLoadException {

        for (Parameter par : taskDescription.getParameters()) {
            final Direction parameterDirection = par.getDirection();

            if (parameterDirection == Direction.IN || parameterDirection == Direction.IN_DELETE) {
                switch (par.getType()) {
                    case OBJECT_T:
                    case PSCO_T:
                    case EXTERNAL_PSCO_T:
                        DependencyParameter dependencyParameter = (DependencyParameter) par;
                        addParameterToMapOfParameters(namedParameters, par, getObjectValue(dependencyParameter));
                        break;

                    case FILE_T:
                        LOGGER.error("Error: HTTP CAN'T USE BINDING FILES AS PARAMETERS!");
                        // Skip
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
    }

    private void addParameterToMapOfParameters(Map<String, String> namedParameters, Parameter par, Object o) {
        String paramName = par.getName();
        if (paramName != null && !paramName.isEmpty()) {
            final String value = convertToString(o);

            namedParameters.put(paramName, String.valueOf(value));
        }
    }

    private String convertToString(Object o) {
        if (o instanceof Integer) {
            return Integer.toString((Integer) o);
        }
        if (o instanceof Float) {
            return String.valueOf(o);
        }
        return (String) o;
    }

    private Object getObjectValue(DependencyParameter dp) throws CannotLoadException {
        String renaming = ((RAccessId) dp.getDataAccessId()).getReadDataInstance().getRenaming();

        LogicalData logicalData = Comm.getData(renaming);
        if (!logicalData.isInMemory()) {
            logicalData.loadFromStorage();
        }
        return logicalData.getValue();
    }
}
