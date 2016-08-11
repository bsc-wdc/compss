/*
 *  A JAVAGAT adaptor for LSF+GPFS Clusters
 *  
 *  	Author: Carlos Díaz
 *      Contact: support-compss@bsc.es
 *
 *	Barcelona Supercomputing Center
 * 	www.bsc.es
 *	
 *	Grid Computing and Clusters
 *	www.bsc.es/computer-sciences/grid-computing
 *
 *	Barcelona, 2014
 */

package org.gridlab.gat.io.cpi.lsf;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.cpi.FileCpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public class LsfFileAdaptor extends FileCpi {

    protected static Logger logger = LoggerFactory.getLogger(LsfFileAdaptor.class);

    public static Map<String, Boolean> getSupportedCapabilities() {
        Map<String, Boolean> capabilities = FileCpi.getSupportedCapabilities();
        capabilities.put("copy", true);
        capabilities.put("canRead", true);
        capabilities.put("canWrite", true);
        capabilities.put("createNewFile", true);
        capabilities.put("delete", true);
        capabilities.put("exists", true);
        capabilities.put("getAbsoluteFile", true);
        capabilities.put("getCanonicalFile", true);
        capabilities.put("getParent", true);
        capabilities.put("getParentFile", true);
        capabilities.put("isDirectory", true);
        capabilities.put("isFile", true);
        capabilities.put("isHidden", true);
        capabilities.put("lastModified", true);
        capabilities.put("length", true);
        capabilities.put("list", true);
        capabilities.put("listFiles", true);
        capabilities.put("mkdir", true);
        capabilities.put("mkdirs", true);
        capabilities.put("move", true);
        capabilities.put("renameTo", true);
        capabilities.put("setLastModified", true);
        capabilities.put("setReadOnly", true);

        return capabilities;
    }
        
    public static String getDescription() {
        return "The Lsf File Adaptor is a simple wrapper around the java.io.File object.";
    }
    
    public static String[] getSupportedSchemes() {
        return new String[] { "lsf", "file", ""};
    }

    private File f;

    /**
     * @param gatContext
     * @param location
     */
    public LsfFileAdaptor(GATContext gatContext, URI location)
            throws GATObjectCreationException {
        super(gatContext, location);
                
        f = getFile(location);
    }
    
    private File getFile(URI location) {
        File file;
        URI correctedURI = newcorrectURI(location);

        logger.debug("corrected uri '" + location + "' to '" + correctedURI
                + "'");

        if ((correctedURI.getPath() == null)
                || (correctedURI.getPath().equals(""))) {
            file = new File(".");
        } else {
            String path = correctedURI.getPath();
            if (java.io.File.separatorChar == '\\') {
                // This means we're on a Windows machine!
                if (path.startsWith("/")) {
                    path = path.substring(1);
                }
                path = path.replace("/", "\\");
            }
            file = new File(path);
        }
        return file;
    }

    private URI newcorrectURI(URI in) {
        // 1. a uri without a scheme gets the scheme 'file'
        // 2. if a uri has a relative path and a hostname, then the uri is
        // resolved to 'user.home'
        // 3. else the uri will be resolved to 'user.dir' (the CWD)

        if (in.getScheme() == null) {
            try {
                in = in.setScheme("file");
            } catch (URISyntaxException e) {
                // ignore
            }
        }
        if (in.getHost() != null && !in.hasAbsolutePath()) {

            try {
                // getPath adds $USER_HOME if necessary.
                in = in.setPath(in.getPath());
            } catch (URISyntaxException e) {
                // ignore
            }
        }
        return in;
    }

    // Make life a bit easier for the programmer:
    // If the URI does not have a scheme part, just consider it a local
    // file.
    // The ctor of java.io.file does not accept this.
    protected static URI correctURI(URI in) {
        if (in.getScheme() == null) {
            try {
                return new URI("file:///" + in.getPath());
            } catch (URISyntaxException e) {
                throw new Error("internal error in LsfFile: " + e);
            }
        }

        return in;
    }
    
    protected void copyDir(File sourceFile, File destFile) throws GATInvocationException {
        
        String sourcePath = sourceFile.getPath();
        String destPath = destFile.getPath();
        
        if (logger.isDebugEnabled()) {
            logger.debug("copyDir '" + sourcePath + "' to '" + destPath + "'");
        }

        boolean existingDir = false;

        if (destFile.exists()) {
            // check whether the target is an existing file
            if ( !destFile.isDirectory()) {   
                throw new GATInvocationException("cannot overwrite non-directory '"
                        + destPath + "' with directory '"
                        + sourcePath + "'!");
            } else {
                existingDir = true;
            }
        }

        if (! existingDir) {
            // copy dir a to b will result in a new directory b with contents
            // that is a copy of the contents of a.
        } else if (gatContext.getPreferences().containsKey("file.directory.copy")
                && ((String) gatContext.getPreferences().get(
                        "file.directory.copy")).equalsIgnoreCase("contents")) {
            // don't modify the dest dir, so copy dir a to dir b ends up as
            // copying a/* to b/*, note that a/dir/* also ends up in b/*
        } else {
            // because copy dir a to dir b ends up as b/a we've to add /a to the
            // dest.
            if (sourcePath.length() > 0) {
                int start = sourcePath.lastIndexOf(File.separator) + 1;
                String separator = "";
                if (!destPath.endsWith(File.separator)) {
                    separator = File.separator;
                }
                destPath = destPath + separator
                            + sourcePath.substring(start);
                destFile = new File(destPath);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("destPath = " + destPath);
        }
        
        // create destination dir
        
        if (gatContext.getPreferences().containsKey("file.create")
                && ((String) gatContext.getPreferences().get("file.create"))
                            .equalsIgnoreCase("true")) {
            destFile.mkdirs();
        } else {
            // if source is a dir 'dir1' and dest is a dir 'dir2' then the
            // result of dir1.copy(dir2) will be dir2/dir1/.. so even if the
            // 'file.create' flag isn't set, create the dir1 in dir2 before
            // copying the files.
            boolean mkdir = destFile.mkdir();
            if (logger.isDebugEnabled()) {
                logger.debug("mkdir: " + mkdir);
            }
        }

        // list all the files and copy recursively.
        File[] files = sourceFile.listFiles();
        if (files == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("copyDirectory: no files in src directory: "
                        + sourcePath);
            }
            return;
        }
        
        for (File file : files) {
            
            if (logger.isDebugEnabled()) {
                logger.debug("copyDirectory: file to copy = " + file);
            }

            String newDestString = destPath;
            newDestString += File.separator + file.getName();

            if (logger.isDebugEnabled()) {
                logger.debug("new dest: " + newDestString);
                logger.debug("src is file: " + file.isFile());
                logger.debug("src is dir: " + file.isDirectory());
            }

            File newDest = new File(newDestString);
            if (file.isFile()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("copyDir: copying " + file);
                }
                copy(file, newDest);
            } else if (file.isDirectory()) {
                copyDir(file, newDest);
            } else {
                throw new GATInvocationException(
                        "don't know how to handle file: " + file.getPath()
                                + " (links are not supported).");
            }
        }
    }

    /**
     * This method copies the physical file represented by this File instance to
     * a physical file identified by the passed URI.
     * 
     * @param destination
     *                The new location
     */
    public void copy(URI destination) throws GATInvocationException {
    	
        destination = newcorrectURI(destination);
        
        String path = getPath();
        String destPath = destination.getPath();

        if (logger.isInfoEnabled()) {
            logger.info("copy of " + path + " to " + destPath);
        }

        if (destPath.equals(path)) {
            if (logger.isInfoEnabled()) {
                logger.info("copy, source is the same file as dest.");
            }
            return;
        }

        File destFile = getFile(destination);
        
        if (!exists()) {
            throw new GATInvocationException(
                    "the source file does not exist, path = " + path);
        }

        if (isDirectory()) {
            if (logger.isDebugEnabled()) {
                logger.debug("copy, it is a dir");
            }

            copyDir(f, destFile);

            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("copy, it is a file");
        }

        if (gatContext.getPreferences().containsKey("file.create")) {
            if (((String) gatContext.getPreferences().get("file.create"))
                    .equalsIgnoreCase("true")) {
               
                File destinationParentFile = destFile.getParentFile();
                if (destinationParentFile != null) {
                    boolean result = destinationParentFile.mkdirs();
                    if (logger.isDebugEnabled()) {
                        logger.debug("new dirs created: " + result);
                    }
                }
            }
        }

        // if the destination URI is a dir, append the file name.
        if (destFile.isDirectory()) {
            destPath = destPath + File.separator + getName();
            destFile = new File(destPath);
        }

        try {
            destFile.createNewFile();
        } catch (IOException e) {
            throw new GATInvocationException("Creating file failed", e);
        }
        copy(f, destFile);
    }
    
    private static void copy(File in, File out) throws GATInvocationException {
        
        FileInputStream inBuf = null;
        FileOutputStream outBuf = null;

        try {
            out.createNewFile();

            // Copy source to destination
            inBuf = new FileInputStream(in);
            outBuf = new FileOutputStream(out);
        } catch (IOException e) {
            throw new GATInvocationException("LsfFile", e);
        }

        try {
            byte[] buf = new byte[8192];

            for (;;) {
                int len = inBuf.read(buf);
                if (len < 0) {
                    break;
                }
                outBuf.write(buf, 0, len);
            }
        } catch (IOException e) {
            throw new GATInvocationException("LsfFile", e);
        } finally {
            if (outBuf != null) {
                try {
                    outBuf.close();
                } catch (IOException e) {
                    throw new GATInvocationException("LsfFile", e);
                }
            }
            if (inBuf != null) {
                try {
                    inBuf.close();
                } catch (IOException e) {
                    throw new GATInvocationException("LsfFile", e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#canRead()
     */
    public boolean canRead() {
        return f.canRead();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#canWrite()
     */
    public boolean canWrite() {
        return f.canWrite();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#createNewFile()
     */
    public boolean createNewFile() throws GATInvocationException {
        try {
            return f.createNewFile();
        } catch (IOException e) {
            throw new GATInvocationException("LsfFile", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#delete()
     */
    public boolean delete() {
        return f.delete();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#exists()
     */
    public boolean exists() {
        return f.exists();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getAbsoluteFile()
     */
    public org.gridlab.gat.io.File getAbsoluteFile()
            throws GATInvocationException {
        try {
            return GAT.createFile(gatContext, localToURI(f.getAbsoluteFile()
                    .getPath()));
        } catch (Exception e) {
            throw new GATInvocationException("default file", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getAbsolutePath()
     */
    public String getAbsolutePath() {
        return f.getAbsolutePath();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getCanonicalFile()
     */
    public org.gridlab.gat.io.File getCanonicalFile()
            throws GATInvocationException {
        try {
            return GAT.createFile(gatContext, localToURI(f.getCanonicalFile()
                    .getPath()));
        } catch (Exception e) {
            throw new GATInvocationException("default file", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getCanonicalPath()
     */
    public String getCanonicalPath() throws GATInvocationException {
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw new GATInvocationException("LsfFile", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getParent()
     */
    public String getParent() {
        return f.getParent();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#getParentFile()
     */
    public org.gridlab.gat.io.File getParentFile()
            throws GATInvocationException {
        File parent = f.getParentFile();
        if (parent == null) {
            return null;
        }
        try {
            return GAT.createFile(gatContext, localToURI(parent.getPath()));
        } catch (Exception e) {
            throw new GATInvocationException("default file", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#isDirectory()
     */
    public boolean isDirectory() {
        return f.isDirectory();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#isFile()
     */
    public boolean isFile() {
        return f.isFile();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#isHidden()
     */
    public boolean isHidden() {
        return f.isHidden();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#lastModified()
     */
    public long lastModified() throws GATInvocationException {
        return f.lastModified();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#length()
     */
    public long length() throws GATInvocationException {
        return f.length();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#list()
     */
    public String[] list() throws GATInvocationException {
        String[] res = f.list();
        if (!ignoreHiddenFiles) {
            return res;
        }
        ArrayList<String> l = new ArrayList<String>();
        for (int i = 0; i < res.length; i++) {
            if (!res[i].startsWith(".")) {
                l.add(res[i]);
            }
        }

        return l.toArray(new String[l.size()]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#list(java.io.FilenameFilter)
     */
    public String[] list(FilenameFilter arg0) {
        String[] res = f.list(arg0);
        if (!ignoreHiddenFiles) {
            return res;
        }
        ArrayList<String> l = new ArrayList<String>();
        for (int i = 0; i < res.length; i++) {
            if (!res[i].startsWith(".")) {
                l.add(res[i]);
            }
        }

        return l.toArray(new String[l.size()]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#listFiles()
     */
    public org.gridlab.gat.io.File[] listFiles() throws GATInvocationException {
        if (!isDirectory()) {
            throw new GATInvocationException("this is not a directory: "
                    + location);
        }
        File[] r = f.listFiles();
        if (r == null) {
            throw new GATInvocationException("IO error in LSF file adaptor");
        }

        ArrayList<File> l = new ArrayList<File>();
        for (int i = 0; i < r.length; i++) {
            if (!(ignoreHiddenFiles && r[i].isHidden())) {
                l.add(r[i]);
            }
        }

        org.gridlab.gat.io.File[] res = new org.gridlab.gat.io.File[l.size()];

        for (int i = 0; i < res.length; i++) {
            try {
                res[i] = GAT.createFile(gatContext,
                        localToURI(l.get(i).getPath()));
            } catch (Exception e) {
                throw new GATInvocationException("LsfFile", e);
            }
        }

        return res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#listFiles(java.io.FileFilter)
     */
    public org.gridlab.gat.io.File[] listFiles(FileFilter arg0)
            throws GATInvocationException {
        File[] r = f.listFiles(arg0);

        ArrayList<File> l = new ArrayList<File>();
        for (int i = 0; i < r.length; i++) {
            if (!(ignoreHiddenFiles && r[i].isHidden())) {
                l.add(r[i]);
            }
        }

        org.gridlab.gat.io.File[] res = new org.gridlab.gat.io.File[r.length];

        for (int i = 0; i < res.length; i++) {
            try {
                res[i] = GAT.createFile(gatContext,
                        localToURI(l.get(i).getPath()));
            } catch (Exception e) {
                throw new GATInvocationException("LsfFile", e);
            }
        }

        return res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#listFiles(java.io.FilenameFilter)
     */
    public org.gridlab.gat.io.File[] listFiles(FilenameFilter arg0)
            throws GATInvocationException {
        File[] r = f.listFiles(arg0);

        ArrayList<File> l = new ArrayList<File>();
        for (int i = 0; i < r.length; i++) {
            if (!(ignoreHiddenFiles && r[i].isHidden())) {
                l.add(r[i]);
            }
        }

        org.gridlab.gat.io.File[] res = new org.gridlab.gat.io.File[r.length];

        for (int i = 0; i < res.length; i++) {
            try {
                res[i] = GAT.createFile(gatContext,
                        localToURI(l.get(i).getPath()));
            } catch (Exception e) {
                throw new GATInvocationException("LsfFile", e);
            }
        }

        return res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#mkdir()
     */
    public boolean mkdir() throws GATInvocationException {
        return f.mkdir();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#mkdirs()
     */
    public boolean mkdirs() {
        return f.mkdirs();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#move(java.net.URI)
     */
    public void move(URI destination) throws GATInvocationException {
        destination = newcorrectURI(destination);
        String path = getPath();
        String destPath = destination.getPath();

        if (logger.isInfoEnabled()) {
            logger.info("move of " + path + " to " + destPath);
        }

        if (destPath.equals(path)) {
            if (logger.isInfoEnabled()) {
                logger.info("move, source is the same file as dest.");
            }

            return;
        }

        if (!exists()) {
            throw new GATInvocationException(
                    "the source file does not exist, path = " + path);
        }

        File tmp = new File(destPath);
        boolean res = f.renameTo(tmp);

        if (!res) {
            throw new GATInvocationException("Could not move file");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#renameTo(java.io.File)
     */
    public boolean renameTo(org.gridlab.gat.io.File arg0)
            throws GATInvocationException {
        URI arg = arg0.toGATURI();
        File tmp = getFile(arg);
        return f.renameTo(tmp);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#setLastModified(long)
     */
    public boolean setLastModified(long arg0) {
        return f.setLastModified(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gridlab.gat.io.File#setReadOnly()
     */
    public boolean setReadOnly() {
        return f.setReadOnly();
    }

    /**
     * Converts a local path into a URI
     * 
     * @return a URI representing the path
     */
    private URI localToURI(String path) throws URISyntaxException {
        String scheme = location.getScheme();
        if (scheme == null || scheme.equals("")) {
            scheme = "file";
        }
        return new URI(scheme + ":///" + path.replace(File.separatorChar, '/'));
    }
}
