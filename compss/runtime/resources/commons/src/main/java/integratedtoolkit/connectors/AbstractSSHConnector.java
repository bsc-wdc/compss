package integratedtoolkit.connectors;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

import integratedtoolkit.connectors.utils.KeyManager;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.ApplicationPackage;
import integratedtoolkit.types.CloudImageDescription;


public abstract class AbstractSSHConnector extends AbstractConnector {
    
    // Properties' names
    private static final String VM_USER = "vm-user";
    private static final String VM_PASS = "vm-password";
    private static final String VM_KEYPAIR_NAME = "vm-keypair-name";
    private static final String VM_KEYPAIR_LOCATION = "vm-keypair-location";

    // Retry properties
    private static final Integer KNOWN_HOSTS_MAX_ALLOWED_ERRORS = 20;
    private static final Integer MAX_ALLOWED_ERRORS = 5;
    private static final Integer RETRY_TIME = 10; // Seconds
    private static final int SERVER_TIMEOUT = 30_000; // Milliseconds

    // Default properties values
    private static final String DEFAULT_DEFAULT_USER = System.getProperty("user.name");
    private static final String DEFAULT_KEYPAIR_NAME = "id_rsa";
    private static final String DEFAULT_KEYPAIR_LOCATION = System.getProperty("user.home") + File.separator + ".ssh";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CONNECTORS);
    
    // Error messages
    private static final String ERROR_NO_KEYPAIR = "Error: There is no key pair to configure. Please create one with the ssh-keygen tool";
    private static final String ERROR_CONFIGURING_ACCESS = "Error configuring access for ";
    private static final String ERROR_PREPARING_MACHINE = "Error: Failed to prepare Machine ";
    private static final String ERROR_TRANSFER_PACKAGES = "Error: Cannot transfer packages to Machine";
    private static final String ERROR_KNOWN_HOSTS = "Error getting id for known hosts";
    private static final String ERROR_KEYSCAN = "Error executing key-scan command: ";
    private static final String ERROR_ADD_MASTER_KEY = "Error adding key in master's known hosts";
    private static final String ERROR_LOCAL_KNOWN_HOSTS = "Error setting key in local known_hosts: ";
    private static final String ERROR_CONFIG_KEYS = "Error configuring keys for ";
    private static final String ERROR_COMMAND_EXEC = "Error: Failed to execute command ";
    private static final String ERROR_EXCEPTION_EXEC_COMMAND = "Exception running command on ";
    private static final String ERROR_SESSION_CREATION = "Error creating session to ";
    private static final String WARN_INPUTSTREAM_CLOSE = "Warn: InputStream for remote command cannot be closed";
    private static final String WARN_DEFAULT_KEYPAIR = "Warn: Neither password nor key-pair specified. Trying with default key-pair";

    // Attributes
    private String defaultUser;
    private String defaultPassword;
    private final String keyPairName;
    private final String keyPairLocation;


    public AbstractSSHConnector(String providerName, Map<String, String> props) {
        super(providerName, props);

        String propUser = props.get(VM_USER);
        defaultUser = (propUser != null) ? propUser : DEFAULT_DEFAULT_USER;

        String propKeypairName = props.get(VM_KEYPAIR_NAME);
        keyPairName = (propKeypairName != null) ? propKeypairName : DEFAULT_KEYPAIR_NAME;

        String propKeypairLocation = props.get(VM_KEYPAIR_LOCATION);
        keyPairLocation = (propKeypairLocation != null) ? propKeypairLocation : DEFAULT_KEYPAIR_LOCATION;
        
        defaultPassword = props.get(VM_PASS);
    }

    /**
     * @return the defaultUser
     */
    public String getDefaultUser() {
        return defaultUser;
    }

    /**
     * @param defaultUser
     *            the defaultUser to set
     */
    protected void setDefaultUser(String defaultUser) {
        this.defaultUser = defaultUser;
    }

    /**
     * @return the keyPairName
     */
    public String getKeyPairName() {
        return keyPairName;
    }

    /**
     * @return the keyPairLocation
     */
    public String getKeyPairLocation() {
        return keyPairLocation;
    }

    @Override
    public void configureAccess(String workerIP, String user, String password) throws ConnectorException {
        LOGGER.debug("Configuring access for user " + user + " in " + workerIP);

        // Add machine to master known hosts
        putInKnownHosts(workerIP);

        // Configure remote machine
        try {
            // Set session properties with specific password or keypair
            String passwordOrKeyPair = null;
            boolean setPassword = false;
            if (user == null) {
                user = defaultUser;
            }
            if (password != null || defaultPassword != null) {
                setPassword = true;
                if (password == null){
                	passwordOrKeyPair = defaultPassword;
                }else{
                	passwordOrKeyPair = password;
                }
                LOGGER.debug("First access done by password");
            } else if (keyPairName != null) {
                setPassword = false;
                passwordOrKeyPair = keyPairLocation + File.separator + keyPairName;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("First access done by key pair: " + passwordOrKeyPair);
                }
            }

            // Configure keypair from master
            String keypair = KeyManager.getKeyPair();
            if (keypair == null) {
                throw new ConnectorException(ERROR_NO_KEYPAIR);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Configuring key pair: " + keypair);
            }
            configureKeys(workerIP, user, setPassword, passwordOrKeyPair, KeyManager.getPublicKey(keypair),
                    KeyManager.getPrivateKey(keypair), KeyManager.getKeyType());

        } catch (ConnectorException | IOException e) {
            LOGGER.error(ERROR_CONFIGURING_ACCESS + workerIP, e);
            throw new ConnectorException(ERROR_CONFIGURING_ACCESS + workerIP, e);
        }
    }

    @Override
    public void prepareMachine(String ip, CloudImageDescription cid) throws ConnectorException {
        LOGGER.debug("Preparing new machine " + ip);

        List<ApplicationPackage> packages = cid.getPackages();
        if (!packages.isEmpty()) {
            String user = cid.getConfig().getUser();
            Session session = null;
            try {
                session = getSession(ip, user, false, null);

                transferPackages(session, packages);

                extractPackages(ip, user, false, null, packages);

                addPackagesToClasspath(ip, user, false, null, packages);

            } catch (Exception e) {
                LOGGER.error(ERROR_PREPARING_MACHINE + ip, e);
                throw new ConnectorException(ERROR_PREPARING_MACHINE + ip, e);
            } finally {
                if (session != null && session.isConnected()) {
                    LOGGER.debug("Disconnecting session");
                    session.disconnect();
                }
            }
        }

    }

    private void transferPackages(Session session, List<ApplicationPackage> packages) throws ConnectorException {
        ChannelSftp client = null;
        try {
            client = (ChannelSftp) session.openChannel("sftp");
            client.connect(SERVER_TIMEOUT);
            for (ApplicationPackage p : packages) {
                String[] path = p.getSource().split(File.separator);
                String name = path[path.length - 1];
                String target = p.getTarget() + File.separator + name;

                // Transfer packages
                client.put(p.getSource(), target, new PackageTransferProgressMonitor());
                client.chmod(Integer.parseInt("700", 8), target);
            }
        } catch (SftpException | JSchException e) {
            throw new ConnectorException(ERROR_TRANSFER_PACKAGES, e);
        } finally {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        }
    }

    private void extractPackages(String workerIP, String user, boolean setPassword, String passwordOrKeyPair,
            List<ApplicationPackage> packages) throws ConnectorException {

        for (ApplicationPackage p : packages) {
            String[] path = p.getSource().split(File.separator);
            String name = path[path.length - 1];
            String target = p.getTarget() + File.separator + name;

            // Extracting Worker package
            String command = "tar xzf " + target + " -C " + p.getTarget() + " && rm " + target;

            executeTask(workerIP, user, setPassword, passwordOrKeyPair, command);
        }
    }

    private void addPackagesToClasspath(String workerIP, String user, boolean setPassword, String passwordOrKeyPair,
            List<ApplicationPackage> packages) throws ConnectorException {

        for (ApplicationPackage p : packages) {
            String target = p.getTarget();

            // Adding classpath in bashrc
            String command = "echo \"\nfor i in " + target +
                    "/*.jar ; do\n\texport CLASSPATH=\\$CLASSPATH:\\$i\ndone\" >> ~/.bashrc";

            executeTask(workerIP, user, setPassword, passwordOrKeyPair, command);
        }
    }

    private void putInKnownHosts(String workerIP) throws ConnectorException {
        LOGGER.debug("Putting id of new machine in master machine known hosts");

        // Scan key ------------------------------------------------------------
        String key = new String();
        String[] cmd = { "/bin/sh", "-c", "ssh-keyscan -t rsa,dsa " + workerIP };
        int errors = 0;
        String errorString = null;
        while (key.isEmpty() && errors < KNOWN_HOSTS_MAX_ALLOWED_ERRORS) {
            InputStream errStream = null;
            InputStream outStream = null;
            try {
                if (LOGGER.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("");
                    for (String param : cmd) {
                        sb.append(param).append(" ");
                    }
                    LOGGER.debug("COMM CMD: " + sb.toString());
                }
                Process p = Runtime.getRuntime().exec(cmd);
                errStream = p.getErrorStream();
                outStream = p.getInputStream();
                p.waitFor();

                boolean commandFailed = false;
                if (p.exitValue() == 0) {
                    key = readInputStream(outStream);
                    if (key.isEmpty()) {
                        commandFailed = true;
                    }
                } else {
                    commandFailed = true;
                    errorString = readInputStream(outStream);
                }

                if (commandFailed) {
                    errors++;
                    LOGGER.debug("Error scaning key. Retrying: " + errors + "/" + KNOWN_HOSTS_MAX_ALLOWED_ERRORS);

                    // Sleep until next retry
                    try {
                        Thread.sleep(RETRY_TIME * S_TO_MS);
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Sleep between keyscan interrupted", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (IOException ioe) {
                LOGGER.error(ERROR_KNOWN_HOSTS, ioe);
                throw new ConnectorException(ERROR_KNOWN_HOSTS, ioe);
            } finally {
                try {
                    if (outStream != null) {
                        outStream.close();
                    }
                    if (errStream != null) {
                        errStream.close();
                    }
                } catch (IOException e) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Exception closing streams (" + e.getMessage() + ")");
                    }
                }

            }
            // LOGGER.debug("Key is " + key);
        }
        if (errors == KNOWN_HOSTS_MAX_ALLOWED_ERRORS) {
            LOGGER.error(ERROR_KEYSCAN + errorString);
            throw new ConnectorException(ERROR_KEYSCAN + errorString);
        }

        // Insert key to known hosts ------------------------------------------------------
        LOGGER.debug("Modifiying known hosts");
        cmd = new String[] { "/bin/sh", "-c", "/bin/echo " + "\"" + key + "\"" + " >> " + System.getProperty("user.home") + File.separator
                + ".ssh" + File.separator + "known_hosts" };

        synchronized (knownHosts) {
            errors = 0;
            int exitValue = -1;
            while (exitValue == 0 && errors < MAX_ALLOWED_ERRORS) {
                InputStream errStream = null;
                InputStream outStream = null;
                try {
                    if (LOGGER.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder("");
                        for (String param : cmd) {
                            sb.append(param).append(" ");
                        }
                        LOGGER.debug("COMM CMD: " + sb.toString());
                    }
                    Process p = Runtime.getRuntime().exec(cmd);
                    errStream = p.getErrorStream();
                    outStream = p.getInputStream();
                    p.waitFor();
                    if (p.exitValue() != 0) {
                        errorString = readInputStream(outStream);
                        errors++;
                        LOGGER.debug("Error inserting key into known_hosts. Retrying: " + errors + "/" + MAX_ALLOWED_ERRORS);

                        // Sleep until next retry
                        try {
                            Thread.sleep(RETRY_TIME * (long)1_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (IOException ioe) {
                    LOGGER.error(ERROR_ADD_MASTER_KEY, ioe);
                    throw new ConnectorException(ERROR_ADD_MASTER_KEY, ioe);
                } finally {
                    try {
                        if (outStream != null) {
                            outStream.close();
                        }
                        if (errStream != null) {
                            errStream.close();
                        }
                    } catch (IOException e) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Exception closing streams (" + e.getMessage() + ")");
                        }
                    }

                }
            }
        }

        if (errors == MAX_ALLOWED_ERRORS) {
            LOGGER.error(ERROR_LOCAL_KNOWN_HOSTS + errorString);
            throw new ConnectorException(ERROR_LOCAL_KNOWN_HOSTS + errorString);
        }

    }

    private void configureKeys(String workerIP, String user, boolean setPassword, String passwordOrKeyPair, String publicKey,
            String privateKey, String keyType) throws ConnectorException {

        LOGGER.debug("Configuring keys for " + workerIP + " user: " + user);

        try {
            String command = "/bin/echo \"" + publicKey + "\" > ~/.ssh/" + keyType + ".pub" + "; "
                    + "/bin/echo \"" + privateKey + "\" > ~/.ssh/" + keyType + "; "
                    + "chmod 600 ~/.ssh/" + keyType + "; "
                    + "/bin/echo \"" + publicKey + "\" >> ~/.ssh/authorized_keys";

            executeTask(workerIP, user, setPassword, passwordOrKeyPair, command);
        } catch (ConnectorException e) {
            String msg = ERROR_CONFIG_KEYS + workerIP + " user: " + user;
            LOGGER.error(msg, e);
            throw new ConnectorException(msg, e);
        }

    }

    private void executeTask(String workerIP, String user, boolean setPassword, String passwordOrKeyPair, String command)
            throws ConnectorException {
        
        int numRetries = 0;
        ConnectorException reason = null;
        do {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Executing command: " + command);
            }
            
            // Try to execute command
            try {
                boolean success = tryToExecuteCommand(workerIP, user, setPassword, passwordOrKeyPair, command);

                if (success) {
                    return;
                } else {
                    throw new ConnectorException(ERROR_EXCEPTION_EXEC_COMMAND + user + "@" + workerIP);
                }
            } catch (ConnectorException ce) {
                ++numRetries;
                LOGGER.error(ERROR_EXCEPTION_EXEC_COMMAND + user + "@" + workerIP, ce);
                LOGGER.error("Retrying: " + numRetries + " of " + MAX_ALLOWED_ERRORS);
                reason = new ConnectorException(ERROR_EXCEPTION_EXEC_COMMAND + user + "@" + workerIP, ce);
            }
            
            // Sleep between connection retries
            try {
                long sleepTime = RETRY_TIME * S_TO_MS;
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                LOGGER.warn("Sleep between CMD execution interrupted", ie);
                Thread.currentThread().interrupt();
            }
            
        } while (numRetries < MAX_ALLOWED_ERRORS);

        // This code is only reached if command was unsuccessful
        LOGGER.error(ERROR_EXCEPTION_EXEC_COMMAND + user + "@" + workerIP, reason);
        throw reason;
    }

    private boolean tryToExecuteCommand(String workerIP, String user, boolean setPassword, String passwordOrKeyPair, String command)
            throws ConnectorException {

        Session session = null;
        ChannelExec exec = null;
        InputStream inputStream = null;
        try {
            // Create session
            session = getSession(workerIP, user, setPassword, passwordOrKeyPair);

            // Configure session
            exec = (ChannelExec) session.openChannel("exec");
            exec.setCommand(command);

            // Waits the command to be executed
            inputStream = exec.getErrStream();
            InputStream stdOut = exec.getInputStream();

            // Execute command
            exec.connect(SERVER_TIMEOUT);

            if (LOGGER.isDebugEnabled()) {
                String output = readInputStream(stdOut);
                LOGGER.debug("Command output: " + output);
            }

            int exitStatus = -1;
            while (exitStatus < 0) {
                exitStatus = exec.getExitStatus();
                if (exitStatus == 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Command successfully executed: " + command);
                    }
                    return true;
                } else if (exitStatus > 0) {
                    String output = readInputStream(inputStream);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(ERROR_COMMAND_EXEC + command + " in " + workerIP + ".\nReturned std error: " + output);
                    }
                    throw new ConnectorException(ERROR_COMMAND_EXEC + command + " in " + workerIP + " (exit status:" + exitStatus + ")");
                }

                LOGGER.debug("Command still on execution");
                try {
                    Thread.sleep(RETRY_TIME * S_TO_MS);
                } catch (InterruptedException e) {
                    LOGGER.debug("Sleep interrupted");
                    Thread.currentThread().interrupt();
                }
            }

        } catch (ConnectorException | IOException | JSchException e) {
            throw new ConnectorException(ERROR_EXCEPTION_EXEC_COMMAND + user + "@" + workerIP, e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOGGER.warn(WARN_INPUTSTREAM_CLOSE);
                }
            }
            if (exec != null && exec.isConnected()) {
                LOGGER.debug("Disconnecting exec channel");
                exec.disconnect();
            }
            if (session != null && session.isConnected()) {
                LOGGER.debug("Disconnecting session");
                session.disconnect();
            }
        }

        return false;
    }

    private String readInputStream(InputStream is) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append('\n');
            } 
        } catch (IOException ioe) {
            throw ioe;
        }
        
        return stringBuilder.toString();
    }

    private Session getSession(String host, String user, boolean password, String keyPairOrPassword) throws ConnectorException {
        // String[] client2server =
        // ("aes256-ctr,aes192-ctr,aes128-ctr,blowfish-ctr,aes256-cbc,aes192-cbc,aes128-cbc,blowfish-cbc").split(",");
        // String[] server2client =
        // ("aes256-ctr,aes192-ctr,aes128-ctr,blowfish-ctr,aes256-cbc,aes192-cbc,aes128-cbc,blowfish-cbc").split(",");

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        if (keyPairOrPassword == null) {
            password = false;
            keyPairOrPassword = KeyManager.getKeyPair();
            LOGGER.warn(WARN_DEFAULT_KEYPAIR + " (" + KeyManager.getKeyPair() + ")");
        }

        int errors = 0;
        JSchException exception = null;
        while (errors < MAX_ALLOWED_ERRORS) {
            Session session = null;
            JSch jsch = new JSch();
            try {
                // Connect session
                if (password) {
                    session = jsch.getSession(user, host, 22);
                    session.setPassword(keyPairOrPassword);
                } else {
                    jsch.addIdentity(keyPairOrPassword);
                    session = jsch.getSession(user, host, 22);
                }
                session.setConfig(config);
                session.connect();

                // Check creation status
                if (session.isConnected()) {
                    if (LOGGER.isDebugEnabled()) {
                        if (password) {
                            LOGGER.debug("Session created as " + user + "@" + host + " with password.");
                        } else {
                            LOGGER.debug("Session created as " + user + "@" + host + " with public key " + keyPairOrPassword); 
                        }
                    }
                    return session;
                    
                } else {
                    ++errors;
                    if (password) {
                        LOGGER.warn("Error connecting to " + user + "@" + host + " with password.");
                    } else {
                        LOGGER.warn("Error connecting to " + user + "@" + host + " with public key" + keyPairOrPassword);
                    }
                    LOGGER.warn("Retrying after " + RETRY_TIME * errors + " seconds...");
                }
            } catch (JSchException e) {
                ++errors;
                exception = e;
                LOGGER.warn("Error creating session to " + user + "@" + host + "(" + e.getMessage() + ").");
                LOGGER.warn("Retrying after " + RETRY_TIME * errors + " seconds...");
                if (session != null && session.isConnected()) {
                    session.disconnect();
                }
            }
            
            // Sleep between retries
            try {
                Thread.sleep(RETRY_TIME * errors * S_TO_MS);
            } catch (InterruptedException e) {
                LOGGER.debug("Sleep interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        // If we reach this point the session has not been correctly initialized
        if (exception != null) {
            LOGGER.error(ERROR_SESSION_CREATION + user + "@" + host, exception);
            throw new ConnectorException(ERROR_SESSION_CREATION + user + "@" + host, exception);
        } else {
            LOGGER.error(ERROR_SESSION_CREATION + user + "@" + host);
            throw new ConnectorException(ERROR_SESSION_CREATION + user + "@" + host);
        }
    }


    private static class PackageTransferProgressMonitor implements SftpProgressMonitor {

        private long max = 1;
        private long count = 0;


        public PackageTransferProgressMonitor() {
            // Values initialized in static. Nothing to do
        }

        @Override
        public void init(int op, String src, String dest, long max) {
            this.max = max;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Starting " + ((op == SftpProgressMonitor.PUT) ? "put" : "get") + ": " + src);
                LOGGER.debug("Operation transfered " + this.count + " out of " + this.max);
            }
        }

        @Override
        public boolean count(long count) {
            this.count += count;
            return true;
        }

        @Override
        public void end() {
            LOGGER.debug("Operation Finished");
        }

    }

}
