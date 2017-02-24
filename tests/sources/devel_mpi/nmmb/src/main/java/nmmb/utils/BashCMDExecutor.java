package nmmb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nmmb.exceptions.CommandException;
import nmmb.loggers.LoggerNames;


public class BashCMDExecutor {

    private static final Logger LOGGER = LogManager.getLogger(LoggerNames.BASH_CMD);

    private static final String ERROR_OUTPUTREADER = "[ERROR] Cannot retrieve command output";
    private static final String ERROR_ERRORREADER = "[ERROR] Cannot retrieve command error";
    private static final String ERROR_PROC_EXEC = "[ERROR] Exception executing command";

    private final String command;
    private final List<String> arguments;


    public BashCMDExecutor(String command) {
        this.command = command;
        this.arguments = new LinkedList<>();
    }

    public void addArgument(String arg) {
        this.arguments.add(arg);
    }

    public void addFlagAndValue(String flag, String value) {
        this.arguments.add(flag);
        this.arguments.add(value);
    }

    public int execute() throws CommandException {
        LOGGER.info("[CMD EXECUTION WRAPPER] Executing command: " + this.toString());

        // Prepare command execution
        String[] cmd = new String[this.arguments.size() + 1];
        int i = 0;
        cmd[i++] = this.command;
        for (String arg : this.arguments) {
            cmd[i++] = arg;
        }
        ProcessBuilder builder = new ProcessBuilder(cmd);

        // Launch command
        Process process = null;
        int exitValue = -1;
        try {
            process = builder.start();

            LOGGER.debug("[CMD EXECUTION WRAPPER] Waiting for CMD completion");
            exitValue = process.waitFor();
        } catch (Exception e) {
            throw new CommandException(ERROR_PROC_EXEC, e);
        } finally {
            // Log binary execution
            logBinaryExecution(process, exitValue);
        }

        LOGGER.info("[CMD EXECUTION WRAPPER] End command execution");

        // Return process exit value
        return exitValue;
    }

    private static void logBinaryExecution(Process process, int exitValue) throws CommandException {

        // Print all process execution information
        LOGGER.debug("[CMD EXECUTION WRAPPER] ------------------------------------");
        LOGGER.debug("[CMD EXECUTION WRAPPER] CMD EXIT VALUE: " + exitValue);

        LOGGER.debug("[CMD EXECUTION WRAPPER] ------------------------------------");
        LOGGER.debug("[CMD EXECUTION WRAPPER] CMD OUTPUT:");
        if (process != null) {
            try (BufferedReader outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = null;
                while ((line = outputReader.readLine()) != null) {
                    LOGGER.debug(line);
                }
            } catch (IOException ioe) {
                throw new CommandException(ERROR_OUTPUTREADER, ioe);
            }
        }

        LOGGER.error("[CMD EXECUTION WRAPPER] ------------------------------------");
        LOGGER.error("[CMD EXECUTION WRAPPER] CMD ERROR:");
        if (process != null) {
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line = null;
                while ((line = errorReader.readLine()) != null) {
                    LOGGER.error(line);
                }
            } catch (IOException ioe) {
                throw new CommandException(ERROR_ERRORREADER, ioe);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.command);
        for (String arg : this.arguments) {
            sb.append(" ").append(arg);
        }

        return sb.toString();
    }

}
