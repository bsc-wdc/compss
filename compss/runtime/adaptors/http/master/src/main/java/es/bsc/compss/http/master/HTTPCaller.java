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
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class HTTPCaller extends RequestDispatcher<HTTPJob> {

    private static final String SUBMIT_ERROR = "Error calling HTTP Service";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


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
                Map<String, Object> namedParameters = new HashMap<>();

                final TaskDescription taskParams = job.getTaskParams();
                processTaskParameters(taskParams, namedParameters);

                HTTPImplementation httpImplementation = (HTTPImplementation) job.getImplementation();

                LOGGER.debug("Executing HTTP request...");

                // String url = replaceUrlParameters(httpImplementation.getBaseUrl(), namedParameters);
                // TODO Aldo: replace parameters in URL

                final String methodType = httpImplementation.getMethodType();
                final String baseUrl = httpImplementation.getBaseUrl();

                Response response = HTTPController.performRequest(methodType, baseUrl);

                int responseCode = response.getResponseCode();
                if (responseCode >= 200 && responseCode < 300) {
                    // int[] mockResultInt = new int[]{1, 2, 4, 9};

                    job.setReturnValue(response.getResponseBody());
                    job.getListener().jobCompleted(job);
                } else {
                    job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
                }

            } catch (Exception e) {
                job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
                LOGGER.error(SUBMIT_ERROR, e);
            }
        }
    }

    private void processTaskParameters(TaskDescription taskParams, Map<String, Object> namedParameters)
        throws CannotLoadException {

        for (Parameter par : taskParams.getParameters()) {
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

    private void addParameterToMapOfParameters(Map<String, Object> namedParameters, Parameter par, Object o) {
        String paramName = par.getName();
        if (paramName != null && !paramName.isEmpty()) {
            namedParameters.put(paramName, o);
        }
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
