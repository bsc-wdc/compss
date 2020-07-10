/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FileDeleter {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final ExecutorService DELETE_SERVICE = Executors.newSingleThreadExecutor();


    /**
     * Delete a File Asynchronously.
     * 
     * @param file File to delete
     */
    public static void deleteAsync(final File file) {
        if (file != null) {
            DELETE_SERVICE.execute(new Runnable() {

                @Override
                public void run() {
                    Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
                    deleteFile(file);
                }
            });
        }
    }

    /**
     * Delete a File Synchronously.
     * 
     * @param file File to delete
     */
    public static void deleteSync(final File file) {
        deleteFile(file);
    }

    public static void shutdown() {
        DELETE_SERVICE.shutdown();
    }

    private static void deleteFile(final File f) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Deleting file " + f.getAbsolutePath());
            }
            if (!Files.deleteIfExists(f.toPath())) {
                LOGGER.warn("File " + f.getAbsolutePath() + " not deleted.");
            }
        } catch (DirectoryNotEmptyException dne) {
            // directories must be removed recursively
            Path directory = f.toPath();
            try {
                Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                LOGGER.error("Cannot delete directory " + f.getAbsolutePath(), e);
            }
        } catch (Exception e) {
            LOGGER.error("Cannot delete file " + f.getAbsolutePath(), e);
        }
    }

}
