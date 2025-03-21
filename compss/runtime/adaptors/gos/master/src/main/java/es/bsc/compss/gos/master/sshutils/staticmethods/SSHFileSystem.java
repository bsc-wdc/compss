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
package es.bsc.compss.gos.master.sshutils.staticmethods;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.gos.master.GOSCopy;
import es.bsc.compss.gos.master.GOSUri;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSDifferentHostsTransfer;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSOneWayTransferMonitor;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSSameHostCopyMonitor;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSTransferMonitor;
import es.bsc.compss.gos.master.monitoring.transfermonitor.sftpmonitor.GOSJschTransferMonitor;
import es.bsc.compss.gos.master.sshutils.SSHChannel;
import es.bsc.compss.gos.master.sshutils.SSHGlobalHostCollection;
import es.bsc.compss.gos.master.sshutils.SSHHost;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SSHFileSystem {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String DBG_PREFIX = "[SSHFileSystem] ";


    /**
     * Transfer file.
     *
     * @param copy the copy
     * @param src the src
     * @param dst the dst
     */
    public static GOSTransferMonitor transferFile(GOSCopy copy, GOSUri src, GOSUri dst)
        throws IOException, JSchException, SftpException {

        if (src.isLocal() && dst.isLocal()) {
            // both uris are local
            new File(dst.getPath()).getParentFile().mkdirs();// create parent directories if not exists

            LOGGER.info(
                DBG_PREFIX + "Sending data with sftp from local: " + src.getPath() + " to local: " + dst.getPath());
            if (!new File(src.getPath()).exists()) {
                ErrorManager.error("Could not find local file: " + src.getPath() + ".");
                throw new IOException(DBG_PREFIX + "No file in src path");
            }
            Files.copy(Paths.get(src.getPath()), Paths.get(dst.getPath()));
            copy.markAsFinished(true);
            copy.notifyEnd();
            return null;
        }
        if (src.isLocal()) {
            // send file to remote
            LOGGER.info(DBG_PREFIX + "Sending data with sftp from local: " + src.getPath() + " to remote: " + dst);
            if (new File(src.getPath()).exists()) {
                return sendFile(copy, src, dst);
            } else {
                ErrorManager.error("Could not find local file: " + src.getPath() + ".");
                throw new IOException(DBG_PREFIX + "No file in src path");
            }
        }
        if (dst.isLocal()) {
            // get file from remote
            LOGGER.info(DBG_PREFIX + "Receiving data with sftp from remote: " + src + " to local: " + dst.getPath());

            return getFile(copy, src, dst);
        }
        // Both uris are remote
        LOGGER.info("Sending data with sftp from remote: " + src + " to remote: " + dst);

        return getAndSendFile(copy, src, dst);

    }

    private static GOSTransferMonitor getAndSendFile(GOSCopy copy, GOSUri src, GOSUri dst)
        throws JSchException, SftpException, IOException {
        SSHGlobalHostCollection ghc = getAllHosts(copy);
        SSHHost dstHost = ghc.getHost(dst.getUser(), dst.getHost());
        SSHHost srcHost = ghc.getHost(src.getUser(), src.getHost());

        if (dstHost.hashCode() == srcHost.hashCode()) {
            return getAndSendFileSameHost(copy, src, dst);
        } else {
            return getAndSendFileDifferentHost(copy, src, dst);
        }
    }

    private static SSHGlobalHostCollection getAllHosts(GOSCopy copy) {
        return copy.getWorkerNode().getAdaptorClass().getHosts();
    }

    private static GOSTransferMonitor getAndSendFileSameHost(GOSCopy copy, GOSUri src, GOSUri dst)
        throws JSchException, SftpException {

        SSHGlobalHostCollection ghc = getAllHosts(copy);

        SSHHost srcHost = ghc.getHost(src.getUser(), src.getHost());
        SSHChannel exec = srcHost.openChannel("exec", "execSameHost");
        exec.setCommand("cp " + src.getPath() + " " + dst.getPath());
        exec.connect();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(exec.getErrStream()));
        } catch (IOException e) {
            ErrorManager.error("Error in executing command", e);
        }

        SSHChannel sftp = srcHost.openChannel("sftp", "sftpSameHost");
        sftp.connect();
        sftp.cd("/");
        final long size = sftp.lstat(dst.getPath()).getSize();

        GOSSameHostCopyMonitor monitor = new GOSSameHostCopyMonitor(copy, exec, sftp, reader, dst.getPath(), size);
        exec.connect();
        return monitor;
    }

    private static GOSTransferMonitor getAndSendFileDifferentHost(GOSCopy copy, GOSUri src, GOSUri dst)
        throws JSchException, SftpException, IOException {
        SSHGlobalHostCollection ghc = getAllHosts(copy);

        SSHHost dstHost = ghc.getHost(dst.getUser(), dst.getHost());
        SSHHost srcHost = ghc.getHost(src.getUser(), src.getHost());

        final String srcPath = src.getPath();
        final String dstPath = dst.getPath();
        SSHChannel getChannel = srcHost.openChannel("sftp", "DifferentHost-src");
        getChannel.connect();
        getChannel.cd("/");
        SSHChannel sendChannel = dstHost.openChannel("sftp", "DifferentHost-dst");
        sendChannel.connect();
        sendChannel.cd("/");

        GOSJschTransferMonitor monitor = new GOSJschTransferMonitor();

        Path p = Files.createTempFile("copy" + copy.getId() + "-transfer", "file");
        File tmp = p.toFile();

        GOSDifferentHostsTransfer transfer =
            new GOSDifferentHostsTransfer(copy, getChannel, sendChannel, srcPath, dstPath, tmp, monitor);
        getChannel.get(srcPath, tmp.getPath(), monitor);
        return transfer;

        // createDirectories(sendChannel, dstPath);
    }

    /**
     * Send file.
     *
     * @param copy the copy
     * @param src the src
     * @param dst the dst
     * @throws JSchException the j sch exception
     * @throws SftpException the sftp exception
     */
    private static GOSTransferMonitor sendFile(GOSCopy copy, GOSUri src, GOSUri dst)
        throws JSchException, SftpException {

        SSHGlobalHostCollection ghc = getAllHosts(copy);

        SSHHost dstHost = ghc.getHost(dst.getUser(), dst.getHost());
        SSHChannel channelSftp = dstHost.openChannel("sftp", "sendFtp");
        channelSftp.connect();
        // createDirectories(channelSftp, dstPath);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(DBG_PREFIX + "pwd: " + channelSftp.pwd());
        }
        channelSftp.cd("/");
        GOSJschTransferMonitor m = new GOSJschTransferMonitor();
        channelSftp.put(src.getPath(), dst.getPath(), m);
        GOSOneWayTransferMonitor monitor = new GOSOneWayTransferMonitor(copy, m, channelSftp);
        return monitor;
    }

    /**
     * Gets file.
     *
     * @param copy the copy
     * @param src the src
     * @param dst the dst
     * @throws JSchException the j sch exception
     * @throws SftpException the sftp exception
     */
    private static GOSTransferMonitor getFile(GOSCopy copy, GOSUri src, GOSUri dst)
        throws JSchException, SftpException {
        SSHGlobalHostCollection ghc = getAllHosts(copy);

        SSHHost srcHost = ghc.getHost(src.getUser(), src.getHost());
        final String srcPath = src.getPath();
        final String dstPath = dst.getPath();

        new File(dst.getPath()).getParentFile().mkdirs();// create parent directories if not exists
        SSHChannel channelSftp = srcHost.openChannel("sftp", "getSftp");
        channelSftp.connect();

        new File(dst.getPath()).getParentFile().mkdirs();// create parent directories if not exists
        GOSJschTransferMonitor m = new GOSJschTransferMonitor();

        channelSftp.get(srcPath, dstPath, m);
        GOSOneWayTransferMonitor monitor = new GOSOneWayTransferMonitor(copy, m, channelSftp);
        return monitor;
    }

    private static void createDirectoriesRemote(ChannelSftp channelSftp, String path) throws SftpException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(DBG_PREFIX + "Creating Directories in remote machine");
        }

        String[] splitPath = path.split("/");
        String homePath = channelSftp.pwd();
        for (int i = 0; i < splitPath.length - 1; i++) {

            if (splitPath[i].isEmpty()) {
                continue;
            }
            try {
                channelSftp.ls(splitPath[i]);

            } catch (SftpException e) {
                channelSftp.mkdir(splitPath[i]);
            }
            channelSftp.cd(splitPath[i]);
        }
        channelSftp.cd(homePath);
    }

    /**
     * Append file.
     *
     * @param host the host
     * @param src the src
     * @param dst the dst
     * @throws JSchException the j sch exception
     * @throws SftpException the sftp exception
     */
    public static void appendFile(SSHHost host, String src, String dst) throws JSchException, SftpException {
        SSHChannel channelSftp = host.openChannel("sftp", "append");
        channelSftp.connect();
        // Path tmp = Files.createTempFile("appendFile" + src, ".file");
        GOSJschTransferMonitor monitor = new GOSJschTransferMonitor();
        channelSftp.get(src, dst, monitor, ChannelSftp.APPEND);
        while (!monitor.finished) {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                //
            }
        }
        channelSftp.disconnect();
        /*
         * BufferedWriter out = new BufferedWriter(new FileWriter(dst, true)); // create a reader for tmpFile
         * BufferedReader in = new BufferedReader(new FileReader(tmp.toFile())); String str; while ((str =
         * in.readLine()) != null) { out.write(str + "\n"); } in.close(); out.close();
         * 
         * new File(tmp.toString()).delete();
         * 
         */

    }
}
