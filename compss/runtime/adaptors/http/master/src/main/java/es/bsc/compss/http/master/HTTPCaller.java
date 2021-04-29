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
import es.bsc.compss.worker.COMPSsException;

import java.util.ArrayList;
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
                TaskDescription taskParams = job.getTaskParams();
                Map<String, Object> namedParameters = new HashMap<>();

                for (Parameter par : taskParams.getParameters()) {
                    final Direction parameterDirection = par.getDirection();

                    if (parameterDirection == Direction.IN || parameterDirection == Direction.IN_DELETE) {
                        switch (par.getType()) {
                            case OBJECT_T:
                            case PSCO_T:
                            case EXTERNAL_PSCO_T:
                                DependencyParameter dependencyParameter = (DependencyParameter) par;
                                addParameterToMapOfParameters(namedParameters, par,
                                    getObjectValue(dependencyParameter));
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

                /*
                 * HTTPImplementation service = (HTTPImplementation) job.getImplementation(); service.getBaseUrl();
                 * service.getMethodType(); //hashMap d'adalt // TODO Aldo work here, retornar de moment String
                 * 
                 * Response response = HTTPController.performRequest(); if (response.responseCode >= 200 &&
                 * response.responseCode < 300) { //ok } else { //potser si ha definit altres status code com a ok
                 * //fail }
                 */

                // use namedParameters

                LOGGER.debug("Executing HTTP request...");

                // TODO Aldo: change with real implementation

                int[] mockResultInt = new int[] { 1,
                    2,
                    4,
                    9 };

                Object[] result = new Object[] { mockResultInt };

                if (result.length > 0) {
                    job.setReturnValue(result[0]);
                }

                job.getListener().jobCompleted(job);
            } catch (Exception e) {
                if (e instanceof COMPSsException) {
                    job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, (COMPSsException) e);
                } else {
                    job.getListener().jobFailed(job, JobEndStatus.EXECUTION_FAILED, null);
                }
                LOGGER.error(SUBMIT_ERROR, e);
                return;
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
