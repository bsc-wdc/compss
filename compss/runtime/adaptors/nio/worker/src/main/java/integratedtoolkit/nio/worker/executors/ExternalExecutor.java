package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.ThreadPrintStream;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.util.StreamGobbler;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import storage.StorageException;
import storage.StorageItf;


public abstract class ExternalExecutor extends Executor {
    

    @Override
    String createSandBox() throws Exception {
        File wdirFile = new File(NIOWorker.workingDir + File.separator + "sand_" + UUID.randomUUID().hashCode());
        if (wdirFile.mkdir()) {
            return wdirFile.getAbsolutePath();
        } else {
            throw new Exception("Sandbox not created");
        }
    }

    @Override
    void executeTask(String sandBox, NIOTask nt, NIOWorker nw) throws Exception {
        Map<String, String> env = getEnvironment(nt);
        ArrayList<String> args = getLaunchCommand(nt);
        addArguments(args, nt, nw);
        String strArgs = getArgumentsAsString(args);
        addEnvironment(env, nt, nw);
        ArrayList<String> command = new ArrayList<String>();
        command.add("/bin/bash");
        command.add("-e");
        command.add("-c");
        command.add(strArgs);
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("EXECUTOR COMMAND: ");
            for (String c : command) {
                sb.append(c).append(" ");
            }
            logger.debug(sb.toString());
        }

        executeExternal(nt.getJobId(), command, env, sandBox, nt, nw);
        
    }

    private String getArgumentsAsString(ArrayList<String> args) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String c : args) {
            if (!first) {
                sb.append(" ");
            } else {
                first = false;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    @Override
    void removeSandBox(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        removeSandBox(wdirFile);
    }

    private void removeSandBox(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    removeSandBox(child);
                }
            }
            Files.delete(f.toPath());

        }
    }

    public abstract Map<String, String> getEnvironment(NIOTask nt);

    public abstract ArrayList<String> getLaunchCommand(NIOTask nt);

    private static void addArguments(ArrayList<String> lArgs, NIOTask nt,  NIOWorker nw) throws JobExecutionException, SerializedObjectException {
        lArgs.add(Boolean.toString(nt.workerDebug));
        lArgs.add(nt.getClassName());
        lArgs.add(nt.getMethodName());
        lArgs.add(Boolean.toString(nt.isHasTarget()));
        lArgs.add(Integer.toString(nt.getNumParams()));
        for (NIOParam np : nt.getParams()) {
            ITExecution.ParamType type = np.getType();
            lArgs.add(Integer.toString(type.ordinal()));
            switch (type) {
            case FILE_T:
            	lArgs.add(np.getValue().toString());
            	break;
        	case SCO_T:
        	case PSCO_T:
        		String renaming = np.getValue().toString();
        		Object o = nw.getObject(renaming);
        		PSCOId pscoId = null;
        		if (o instanceof PSCOId) {
        			pscoId = (PSCOId) o;					
					if (!np.isWriteFinalValue()) {    							
						if (!pscoId.getBackends().contains(nw.getHostName())){    								
							if (tracing) {
								NIOTracer.emitEvent(Tracer.Event.STORAGE_NEWREPLICA.getId(), Tracer.Event.STORAGE_NEWREPLICA.getType());
							}								
							try {
								// Replicate PSCO
								StorageItf.newReplica(pscoId.getId(), nw.getHostName());
							} catch (StorageException e) {
								throw new JobExecutionException("Error New Replica: parameter with id " + pscoId.getId() +  ", " + e.getMessage());
							} finally {
								if (tracing) {
									NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWREPLICA.getType());
								}									
							}
						}    						    						
					} else {
						if (tracing) {
							NIOTracer.emitEvent(Tracer.Event.STORAGE_NEWVERSION.getId(), Tracer.Event.STORAGE_NEWVERSION.getType());
						}
						try {
							// New PSCO Version
							String newId = StorageItf.newVersion(pscoId.getId(), nw.getHostName());
							// Modify the PSCO Identifier
							pscoId.setId(newId);
						} catch (StorageException e) {
							throw new JobExecutionException("Error New Version: parameter with id " + pscoId.getId() +  ", " + e.getMessage());    						
						} finally {
							if (tracing) {
								NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_NEWVERSION.getType());
							}
						}
					}
	           		lArgs.add(pscoId.getId());
	                lArgs.add(np.isWriteFinalValue() ? "W" : "R");                        		
        		} else {
        			throw new JobExecutionException("Parameter " + o + " must be a PSCO");
        		}
        		break;            	
            case OBJECT_T:
                lArgs.add(np.getValue().toString());
                lArgs.add(np.isWriteFinalValue() ? "W" : "R");                
                break;
            case STRING_T:
                String value = np.getValue().toString();
                String[] vals = value.split(" ");
                int numSubStrings = vals.length;
                lArgs.add(Integer.toString(numSubStrings));
                for (String v : vals) {
                    lArgs.add(v);
                }
                break;
            default:
                lArgs.add(np.getValue().toString());
            }
        }
    }

    private void addEnvironment(Map<String, String> env, NIOTask nt, NIOWorker nw) {
        env.put("IT_WORKING_DIR", nw.getWorkingDir());
        env.put("IT_APP_DIR", nt.appDir);
    }

    private void executeExternal(int jobId, ArrayList<String> command, Map<String, String> env, String sandbox, NIOTask nt, NIOWorker nw) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(sandbox));
        pb.environment().putAll(env);
        pb.environment().remove("LD_PRELOAD");
        //pb.redirectOutput(new File("/dev/null"));
        Process execProc = null;
        
        // emit start task trace
        int taskType = nt.getTaskType() +1;  // +1 Because Task iD can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (tracing) {
            logger.debug("Emitting HW lcs");
            NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
            NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
        }

        try {
            logger.debug("Starting process ...");
            execProc = pb.start();
            try {
                execProc.getOutputStream().close();
            } catch (IOException e) {
                // Stream closed
            }
            /*BufferedReader reader = new BufferedReader(new InputStreamReader(execProc.getInputStream()));
             String line;
             while ((line = reader.readLine()) != null) {
             System.out.println(line);
             }
             reader = new BufferedReader(new InputStreamReader(execProc.getErrorStream()));
             while ((line = reader.readLine()) != null) {
             System.err.println(line);
             }*/
            logger.debug("Starting stdout/stderr gobblers ...");
            PrintStream out = ((ThreadPrintStream) System.out).getStream();
            PrintStream err = ((ThreadPrintStream) System.err).getStream();
            StreamGobbler outputGobbler = new StreamGobbler(execProc.getInputStream(), out);
            StreamGobbler errorGobbler = new StreamGobbler(execProc.getErrorStream(), err);
            outputGobbler.start();
            errorGobbler.start();
            int exitValue = execProc.waitFor();

            if (tracing){
                logger.debug("Emitting end HW lcs");
            	NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
            }
            logger.debug("Task finished. Waiting for gobblers to end...");
            outputGobbler.join();
            errorGobbler.join();
            if (exitValue != 0) {
                throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
            } else {
                logger.debug("Job" + jobId + " has finished with exit value 0");
            }
        } catch (IOException e) {
            System.err.println("Exception starting process  " + jobId);
            throw e;
        } catch (InterruptedException e) {
            System.err.println("Process interrupted " + jobId);
            throw e;
        } finally {
            if (execProc != null) {
                if (execProc.getInputStream() != null) {
                    try {
                        execProc.getInputStream().close();
                    } catch (IOException e) {

                    }
                }
                if (execProc.getErrorStream() != null) {
                    try {
                        execProc.getErrorStream().close();
                    } catch (IOException e) {

                    }
                }
            }
        }
    }
}
