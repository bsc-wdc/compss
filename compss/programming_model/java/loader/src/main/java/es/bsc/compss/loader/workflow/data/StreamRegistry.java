/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.loader.workflow.data;

import es.bsc.compss.api.Workflow;
import es.bsc.compss.loader.runtime.data.access.AccessMode;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.util.ErrorManager;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamRegistry {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String LINE_SEP = System.getProperty("line.separator");

    /*
     * Map: file absolute path -> list of opened streams of the file Only local files are accepted, since a java stream
     * cannot be opened on a remote file
     */
    private static final Map<String, StreamList> FILE_TO_STREAMS = new HashMap<>();

    private static final Set<String> TASK_FILES = new HashSet<>();

    private static final boolean ON_WINDOWS = (File.separatorChar == '\\');


    /**
     * Creates a new FileInputStream from the given file {@code file}.
     *
     * @param wf workflow accessing the stream
     * @param file File.
     * @return FileInputStream pointing to the given file.
     * @throws FileNotFoundException When file does not exist.
     */
    public static FileInputStream newFileInputStream(Workflow wf, File file) throws FileNotFoundException {
        Direction direction = Direction.IN;
        StreamList list = obtainList(wf, file, direction);

        /*
         * Create the stream on the renaming of the obtained list for the file, then add it to the list The possible
         * exception is thrown for the application to handle it
         */
        FileInputStream fis = new FileInputStream(list.getRenaming());
        list.addStream(fis);
        try {
            list.addFD(fis.getFD());
        } catch (IOException e) {
            // We must go up as a FileNotFoundException, since it is the one that the application deals with
            throw new FileNotFoundException(
                "Loader - Error creating FileInputStream for file " + file + LINE_SEP + e.getMessage());
        }

        return fis;
    }

    /**
     * Creates a new FileInputStream from the given FileDescriptor {@code fd}.
     *
     * @param wf workflow accessing the stream
     * @param fd FileDescriptor.
     * @return FileInputStream pointing to the given file descriptor.
     */
    public static FileInputStream newFileInputStream(Workflow wf, FileDescriptor fd) {
        StreamList list = obtainList(fd);
        FileInputStream fis = new FileInputStream(fd);
        if (list != null) { // Should always be not null
            list.addStream(fis);
        }
        return fis;
    }

    /**
     * Creates a new FileInputStream from the given fileName {@code fileName}.
     *
     * @param wf workflow accessing the stream
     * @param fileName File name.
     * @return FileInputStream pointing to the given file name.
     * @throws FileNotFoundException When the file denoted by the abstract {@code fileName} does not exist.
     */
    public static FileInputStream newFileInputStream(Workflow wf, String fileName) throws FileNotFoundException {
        return newFileInputStream(wf, new File(fileName));
    }

    /**
     * Creates a new FileOutputStream from the given file {@code file} and the given mode {@code append}.
     *
     * @param wf workflow accessing the stream
     * @param file File.
     * @param append Whether the file must be appended or overwritten.
     * @return FileOutputStream pointing to the given file.
     * @throws FileNotFoundException When the file does not exist.
     */
    public static FileOutputStream newFileOutputStream(Workflow wf, File file, boolean append)
        throws FileNotFoundException {
        Direction direction = (append ? Direction.INOUT : Direction.OUT);
        StreamList list = obtainList(wf, file, direction);

        FileOutputStream fos = new FileOutputStream(list.getRenaming(), append);
        list.addStream(fos);
        try {
            list.addFD(fos.getFD());
        } catch (IOException e) {
            // We must go up as a FileNotFoundException, since it is the one that the application deals with
            throw new FileNotFoundException(
                "Loader - Error creating FileOutputStream for file " + file + LINE_SEP + e.getMessage());
        }

        return fos;
    }

    /**
     * Creates a new FileOutputStream from the given file descriptor {@code fd}.
     *
     * @param wf workflow accessing the stream
     * @param fd File descriptor.
     * @return FileOutputStream pointing to the given file descriptor.
     */
    public static FileOutputStream newFileOutputStream(Workflow wf, FileDescriptor fd) {
        StreamList list = obtainList(fd);
        FileOutputStream fos = new FileOutputStream(fd);
        if (list != null) { // Should always be not null
            list.addStream(fos);
        }

        return fos;
    }

    /**
     * Creates a new FileOutputStream from the given file {@code file}.
     *
     * @param wf workflow accessing the stream
     * @param file File.
     * @return FileOutputStream pointing to the given file.
     * @throws FileNotFoundException When the file does not exist.
     */
    public static FileOutputStream newFileOutputStream(Workflow wf, File file) throws FileNotFoundException {
        return newFileOutputStream(wf, file, false);
    }

    /**
     * Creates a new FileOutputStream from the given file name {@code fileName} with the given access mode
     * {@code append}.
     *
     * @param wf workflow accessing the stream
     * @param fileName File name.
     * @param append {@code true} if the file is opened in append mode, {@code false} for overwrite.
     * @return FileOutputStream pointing to the given file name.
     * @throws FileNotFoundException When path denoted by the given abstract file name does not exist.
     */
    public static FileOutputStream newFileOutputStream(Workflow wf, String fileName, boolean append)
        throws FileNotFoundException {
        return newFileOutputStream(wf, new File(fileName), append);
    }

    /**
     * Creates a new FileOutputStream from the given file name {@code fileName}.
     *
     * @param wf workflow accessing the stream
     * @param fileName File Name.
     * @return FileOutputStream pointing to the given file name.
     * @throws FileNotFoundException When path denoted by the given abstract file name does not exist.
     */
    public static FileOutputStream newFileOutputStream(Workflow wf, String fileName) throws FileNotFoundException {
        return newFileOutputStream(wf, new File(fileName), false);
    }

    /**
     * Replaces the given stream {@code code} stream applying the given filter {@code filter}.
     * 
     * @param wf workflow accessing the stream
     * @param stream Stream to replace.
     * @param filter Filter to apply.
     */
    public static void newFilterStream(Workflow wf, Object stream, Object filter) {
        /*
         * We have to replace the stream in its list by the wrapper filter, since the close will be done on the wrapper
         */
        replaceStream(stream, filter);
    }

    /**
     * TODO javadoc.
     */
    public static RandomAccessFile newRandomAccessFile(Workflow wf, File file, String mode)
        throws FileNotFoundException {
        Direction direction;
        if (mode.length() == 1) { // mode == "r"
            direction = Direction.IN;
        } else { // mode == "rw?"
            direction = Direction.INOUT;
        }

        StreamList list = obtainList(wf, file, direction);

        RandomAccessFile raf = new RandomAccessFile(list.getRenaming(), mode);
        list.addStream(raf);
        try {
            list.addFD(raf.getFD());
        } catch (IOException e) {
            // We must go up as a FileNotFoundException, since it is the one that the application deals with
            throw new FileNotFoundException(
                "Loader - Error creating RandomAccessFile for file " + file + LINE_SEP + e.getMessage());
        }

        return raf;
    }

    public static RandomAccessFile newRandomAccessFile(Workflow wf, String fileName, String mode)
        throws FileNotFoundException {
        return newRandomAccessFile(wf, new File(fileName), mode);
    }

    /**
     * TODO javadoc.
     */
    public static FileReader newFileReader(Workflow wf, File file) throws FileNotFoundException {
        Direction direction = Direction.IN;
        StreamList list = obtainList(wf, file, direction);

        FileReader fr = new FileReader(list.getRenaming());
        list.addStream(fr);

        return fr;
    }

    public static FileReader newFileReader(Workflow wf, String fileName) throws FileNotFoundException {
        return newFileReader(wf, new File(fileName));
    }

    /**
     * TODO javadoc.
     */
    public static FileReader newFileReader(Workflow wf, FileDescriptor fd) {
        StreamList list = obtainList(fd);
        FileReader fr = new FileReader(fd);
        if (list != null) { // Should always be not null
            list.addStream(fr);
        }

        return fr;
    }

    /**
     * TODO javadoc.
     */
    public static InputStreamReader newInputStreamReader(Workflow wf, InputStream is) {
        InputStreamReader isr = new InputStreamReader(is);
        /*
         * We have to replace the old stream in its list because the new one wraps it, and the close will be done on the
         * wrapper It is possible that the stream doesn't correspond to a file and the method replace doesn't find it,
         * but we cannot know if it is opened on a file because certain subclasses of InputStream (i.e.
         * FilterInputStream) can be associated to a file or not. So, we call the method anyway.
         */
        replaceStream(is, isr);

        return isr;
    }

    /**
     * TODO javadoc.
     */
    public static InputStreamReader newInputStreamReader(Workflow wf, InputStream is, Charset cs) {
        InputStreamReader isr = new InputStreamReader(is, cs);
        replaceStream(is, isr);

        return isr;
    }

    /**
     * TODO javadoc.
     */
    public static InputStreamReader newInputStreamReader(Workflow wf, InputStream is, CharsetDecoder dec) {
        InputStreamReader isr = new InputStreamReader(is, dec);
        replaceStream(is, isr);

        return isr;
    }

    /**
     * TODO javadoc.
     */
    public static InputStreamReader newInputStreamReader(Workflow wf, InputStream is, String charsetName)
        throws UnsupportedEncodingException {
        InputStreamReader isr = new InputStreamReader(is, charsetName);
        replaceStream(is, isr);

        return isr;
    }

    /**
     * TODO javadoc.
     */
    public static BufferedReader newBufferedReader(Workflow wf, Reader r) {
        BufferedReader br = new BufferedReader(r);
        /*
         * We have to replace the old stream in its list because the new one wraps it, and the close will be done on the
         * wrapper It is possible that the stream doesn't correspond to a file and the method replace doesn't find it,
         * but we cannot know if it is opened on a file because certain subclasses of Reader (e.g. InputStreamReader)
         * can be associated to a file or not. So, we call the method anyway.
         */
        replaceStream(r, br);

        return br;
    }

    /**
     * TODO javadoc.
     */
    public static BufferedReader newBufferedReader(Workflow wf, Reader r, int size) {
        BufferedReader br = new BufferedReader(r, size);
        replaceStream(r, br);

        return br;
    }

    /**
     * TODO javadoc.
     */
    public static FileWriter newFileWriter(Workflow wf, File file, boolean append) throws IOException {
        Direction direction = append ? Direction.INOUT : Direction.OUT;
        StreamList list = obtainList(wf, file, direction);

        FileWriter fw = new FileWriter(list.getRenaming(), append);
        list.addStream(fw);

        return fw;
    }

    public static FileWriter newFileWriter(Workflow wf, File file) throws IOException {
        return newFileWriter(wf, file, false);
    }

    public static FileWriter newFileWriter(Workflow wf, String fileName, boolean append) throws IOException {
        return newFileWriter(wf, new File(fileName), append);
    }

    public static FileWriter newFileWriter(Workflow wf, String fileName) throws IOException {
        return newFileWriter(wf, new File(fileName), false);
    }

    /**
     * TODO javadoc.
     */
    public static FileWriter newFileWriter(Workflow wf, FileDescriptor fd) {
        StreamList list = obtainList(fd);
        FileWriter fw = new FileWriter(fd);
        if (list != null) { // Should always be not null
            list.addStream(fw);
        }

        return fw;
    }

    /**
     * TODO javadoc.
     */
    public static OutputStreamWriter newOutputStreamWriter(Workflow wf, OutputStream os) {
        OutputStreamWriter osw = new OutputStreamWriter(os);
        /*
         * We have to replace the old stream in its list because the new one wraps it, and the close will be done on the
         * wrapper It is possible that the stream doesn't correspond to a file and the method replace doesn't find it,
         * but we cannot know if it is opened on a file because certain subclasses of OutputStream (i.e.
         * FilterOutputStream) can be associated to a file or not. So, we call the method anyway.
         */
        replaceStream(os, osw);

        return osw;
    }

    /**
     * TODO javadoc.
     */
    public static OutputStreamWriter newOutputStreamWriter(Workflow wf, OutputStream os, Charset cs) {
        OutputStreamWriter osw = new OutputStreamWriter(os, cs);
        replaceStream(os, osw);

        return osw;
    }

    /**
     * TODO javadoc.
     */
    public static OutputStreamWriter newOutputStreamWriter(Workflow wf, OutputStream os, CharsetEncoder dec) {
        OutputStreamWriter osw = new OutputStreamWriter(os, dec);
        replaceStream(os, osw);

        return osw;
    }

    /**
     * TODO javadoc.
     */
    public static OutputStreamWriter newOutputStreamWriter(Workflow wf, OutputStream os, String charsetName)
        throws UnsupportedEncodingException {
        OutputStreamWriter osw = new OutputStreamWriter(os, charsetName);
        replaceStream(os, osw);

        return osw;
    }

    /**
     * TODO javadoc.
     */
    public static BufferedWriter newBufferedWriter(Workflow wf, Writer w) {
        BufferedWriter bw = new BufferedWriter(w);
        /*
         * We have to replace the old stream in its list because the new one wraps it, and the close will be done on the
         * wrapper It is possible that the stream doesn't correspond to a file and the method replace doesn't find it,
         * but we cannot know if it is opened on a file because certain subclasses of Writer (e.g. OutputStreamWriter)
         * can be associated to a file or not. So, we call the method anyway.
         */
        replaceStream(w, bw);

        return bw;
    }

    /**
     * TODO javadoc.
     */
    public static BufferedWriter newBufferedWriter(Workflow wf, Writer w, int size) {
        BufferedWriter bw = new BufferedWriter(w, size);
        replaceStream(w, bw);

        return bw;
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, File file) throws FileNotFoundException {
        Direction direction = Direction.OUT;
        StreamList list = obtainList(wf, file, direction);

        PrintStream ps = new PrintStream(list.getRenaming());
        list.addStream(ps);

        return ps;
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, File file, String csn)
        throws FileNotFoundException, UnsupportedEncodingException {
        Direction direction = Direction.OUT;
        StreamList list = obtainList(wf, file, direction);

        PrintStream ps = new PrintStream(list.getRenaming(), csn);
        list.addStream(ps);

        return ps;
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, String fileName) throws FileNotFoundException {
        return newPrintStream(wf, new File(fileName));
    }

    public static PrintStream newPrintStream(Workflow wf, String fileName, String csn)
        throws FileNotFoundException, UnsupportedEncodingException {
        return newPrintStream(wf, new File(fileName), csn);
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, OutputStream os) {
        PrintStream ps = new PrintStream(os);
        replaceStream(os, ps);

        return ps;
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, OutputStream os, boolean autoFlush) {
        PrintStream ps = new PrintStream(os, autoFlush);
        replaceStream(os, ps);

        return ps;
    }

    /**
     * TODO javadoc.
     */
    public static PrintStream newPrintStream(Workflow wf, OutputStream os, boolean autoFlush, String encoding)
        throws UnsupportedEncodingException {
        PrintStream ps = new PrintStream(os, autoFlush, encoding);
        replaceStream(os, ps);

        return ps;
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, File file) throws FileNotFoundException {
        Direction direction = Direction.OUT;
        StreamList list = obtainList(wf, file, direction);

        PrintWriter pw = new PrintWriter(list.getRenaming());
        list.addStream(pw);

        return pw;
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, File file, String csn)
        throws FileNotFoundException, UnsupportedEncodingException {
        Direction direction = Direction.OUT;
        StreamList list = obtainList(wf, file, direction);

        PrintWriter pw = new PrintWriter(list.getRenaming(), csn);
        list.addStream(pw);

        return pw;
    }

    public static PrintWriter newPrintWriter(Workflow wf, String fileName) throws FileNotFoundException {
        return newPrintWriter(wf, new File(fileName));
    }

    public static PrintWriter newPrintWriter(Workflow wf, String fileName, String csn)
        throws FileNotFoundException, UnsupportedEncodingException {
        return newPrintWriter(wf, new File(fileName), csn);
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, OutputStream os) {
        PrintWriter pw = new PrintWriter(os);
        replaceStream(os, pw);

        return pw;
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, OutputStream os, boolean autoFlush) {
        PrintWriter pw = new PrintWriter(os, autoFlush);
        replaceStream(os, pw);

        return pw;
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, Writer w) {
        PrintWriter pw = new PrintWriter(w);
        replaceStream(w, pw);

        return pw;
    }

    /**
     * TODO javadoc.
     */
    public static PrintWriter newPrintWriter(Workflow wf, Writer w, boolean autoFlush) {
        PrintWriter pw = new PrintWriter(w, autoFlush);
        replaceStream(w, pw);

        return pw;
    }

    public static File newCOMPSsFile(Workflow wf, String filename) {
        File f = new File(filename);
        return checkAndGetNewFile(wf, f);
    }

    public static File newCOMPSsFile(Workflow wf, String parent, String child) {
        File f = new File(parent, child);
        return checkAndGetNewFile(wf, f);
    }

    public static File newCOMPSsFile(Workflow wf, File parent, String child) {
        File f = new File(parent, child);
        return checkAndGetNewFile(wf, f);
    }

    public static File newCOMPSsFile(Workflow wf, URI uri) {
        File f = new File(uri);
        return checkAndGetNewFile(wf, f);
    }

    private static File checkAndGetNewFile(Workflow wf, File f) {
        if (TASK_FILES.contains(f.getAbsolutePath())) {
            return new COMPSsFile(wf, f);
        } else {
            return f;
        }
    }

    // Returns the list of streams to which the newly created stream belongs (creating it if necessary)
    private static StreamList obtainList(Workflow wf, File file, Direction direction) {
        String path = null;
        try {
            // Get the absolute and canonical path of the file
            path = file.getCanonicalPath();
        } catch (IOException e) {
            // The Integrated Toolkit must finish
            ErrorManager
                .fatal("Cannot create stream for file " + file.getAbsolutePath() + " with direction " + direction, e);
            return null;
        }

        if (ON_WINDOWS) {
            // Let's make sure that we have no ambiguities on Windows
            path = path.toLowerCase();
        }

        StreamList list = FILE_TO_STREAMS.get(path);
        if (list == null) {
            // First stream opened for this file
            if (DEBUG) {
                LOGGER.debug("First stream on the list for file " + path + " with direction " + direction);
            }

            AccessMode mode;
            // Obtain the renaming
            switch (direction) {
                case IN:
                case IN_DELETE:
                case CONCURRENT:
                    /*
                     * LEGACY CODE. The last version of the file must be transferred to a temp directory without the
                     * Integrated Toolkit keeping track of this operation. Forthcoming streams on the same file will use
                     * this copy in the tmp dir //renaming = itApi.getFile(path, tempDirPath);
                     */
                    mode = AccessMode.READ;
                    break;
                case OUT:
                    // Must ask the IT to open the file in W mode
                    mode = AccessMode.GENERATE;
                    break;
                case COMMUTATIVE:
                case INOUT:
                    // Must ask the IT to open the file in RW mode
                    mode = AccessMode.UPDATE;
                    break;
                default:
                    mode = null;
            }

            String renaming = wf.openFile(path, mode.getID());
            // Create the list of streams
            list = new StreamList(renaming, direction);
            synchronized (FILE_TO_STREAMS) {
                FILE_TO_STREAMS.put(path, list);
            }
        } else {
            if (direction != Direction.IN || list.written) {
                ErrorManager.error("ERROR: File " + path
                    + " is going to be accessed more than once and one of these accesses is for writting. "
                    + "This can produce and inconsistency");
            }
        }

        // Set the written attribute of the list if the new stream writes the file
        if (direction != Direction.IN) {
            list.setWritten(true);
        }

        if (direction == Direction.INOUT) {
            list.setAppend(true);
        }

        if (DEBUG) {
            LOGGER.debug(
                "New stream for file " + path + " with renaming " + list.getRenaming() + " and direction " + direction);
        }

        return list;
    }

    // Returns the list of streams to which the newly created stream belongs
    private static StreamList obtainList(FileDescriptor fd) {
        synchronized (FILE_TO_STREAMS) {
            for (StreamList list : FILE_TO_STREAMS.values()) {
                if (list.containsFD(fd)) {
                    if (DEBUG) {
                        LOGGER.debug("Found list for file descriptor " + fd + ": file " + list.getRenaming());
                    }

                    return list;
                }
            }
        }
        return null;
    }

    // Replace a given stream by another in its list (if present)
    private static void replaceStream(Object oldStream, Object newStream) {
        synchronized (FILE_TO_STREAMS) {
            Iterator<Entry<String, StreamList>> entryIt = FILE_TO_STREAMS.entrySet().iterator();
            while (entryIt.hasNext()) {
                Entry<String, StreamList> e = entryIt.next();
                StreamList list = e.getValue();
                ListIterator<Object> listIt = list.getIterator();
                while (listIt.hasNext()) {
                    Object listStream = listIt.next();
                    if (listStream.equals(oldStream)) {
                        listIt.set(newStream);

                        if (DEBUG) {
                            LOGGER.debug("Replaced stream of " + oldStream.getClass() + " by another of "
                                + newStream.getClass());
                        }

                        continue;
                    }
                }
            }
        }
    }

    /**
     * TODO javadoc.
     */
    public static void streamClosed(Workflow wf, Object stream) {
        // Remove the stream from its list
        String filePath = null;
        StreamList list = null;
        boolean found = false;

        synchronized (FILE_TO_STREAMS) {
            Iterator<Entry<String, StreamList>> entryIt = FILE_TO_STREAMS.entrySet().iterator();
            while (!found && entryIt.hasNext()) {
                Entry<String, StreamList> e = entryIt.next();
                filePath = e.getKey();
                list = e.getValue();
                ListIterator<Object> listIt = list.getIterator();
                while (listIt.hasNext()) {
                    Object listStream = listIt.next();
                    if (listStream.equals(stream)) {
                        listIt.remove();
                        found = true;
                        break;
                    }
                }
            }
        }
        if (found) {
            if (DEBUG) {
                LOGGER.debug("Found closed stream of " + stream.getClass());
            }

            // Check if it was the last stream to be closed

            /*
             * If the stream list began with a input stream and it had at least one output stream, we must obtain the
             * renaming from the Integrated Toolkit for the new version generated, and rename and move the file to the
             * application's working directory
             */

            if (DEBUG) {
                LOGGER.debug("Empty stream list");
            }

            if (list.isFirstStreamInput() && list.getWritten() && list.getAppend()) {
                wf.closeFile(filePath, AccessMode.UPDATE.getID());
            } else if (list.isFirstStreamInput() && list.getWritten() && !list.getAppend()) {
                wf.closeFile(filePath, AccessMode.GENERATE.getID());
            } else if (list.isFirstStreamInput() && !list.getWritten()) {
                wf.closeFile(filePath, AccessMode.READ.getID());
            }
            if (list.isEmpty()) {
                synchronized (FILE_TO_STREAMS) {
                    FILE_TO_STREAMS.remove(filePath);
                }
            }
        }
    }

    /**
     * TODO javadoc.
     */
    public static boolean isTaskFile(String fileName) {
        if (fileName != null) {
            File f = new File(fileName);
            if (TASK_FILES.contains(f.getAbsolutePath())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * TODO javadoc.
     */
    public static void addTaskFile(String fileName) {
        if (DEBUG) {
            LOGGER.debug("Adding File to the Stream Registry");
        }
        File f = new File(fileName);
        TASK_FILES.add(f.getAbsolutePath());
    }

    /**
     * TODO javadoc.
     */
    public static void deleteTaskFile(String fileName) {
        if (DEBUG) {
            LOGGER.debug("Adding File to the Stream Registry");
        }
        File f = new File(fileName);
        TASK_FILES.remove(f.getAbsolutePath());
    }


    private static class StreamList {

        private String fileRenaming;
        private boolean firstIsInputStream;
        private boolean written;
        private boolean append;
        private List<Object> list;
        private List<FileDescriptor> fds;


        public StreamList(String renaming, Direction direction) {
            this.fileRenaming = renaming;
            this.firstIsInputStream = direction == Direction.IN;
            this.written = false;
            this.append = false;
            this.list = new LinkedList<>();
            this.fds = new LinkedList<>();
        }

        public void addStream(Object stream) {
            list.add(stream);
        }

        public void addFD(FileDescriptor fd) {
            fds.add(fd);
        }

        public String getRenaming() {
            return fileRenaming;
        }

        public boolean isFirstStreamInput() {
            return firstIsInputStream;
        }

        public boolean getWritten() {
            return written;
        }

        public boolean getAppend() {
            return append;
        }

        public ListIterator<Object> getIterator() {
            return (ListIterator<Object>) list.iterator();
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        public boolean containsFD(FileDescriptor fd) {
            return fds.contains(fd);
        }

        public void setWritten(boolean b) {
            written = b;
        }

        public void setAppend(boolean b) {
            append = b;
        }
    }

}
