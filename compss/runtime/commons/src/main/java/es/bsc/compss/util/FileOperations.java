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
package es.bsc.compss.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.logging.log4j.Logger;


public class FileOperations {

    /**
     * Copies a file/folder location from a source to a target.
     * 
     * @param source source file
     * @param target target file location
     * @param logger log where to print the errors
     * @throws IOException error raised during the copy of the file/folder
     */
    public static void copyFile(File source, File target, Logger logger) throws IOException {
        Path tgtPath = (target).toPath();
        Path sourcePath = (source).toPath();
        if (tgtPath.compareTo(sourcePath) != 0) {
            try {
                Files.copy(sourcePath, tgtPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (DirectoryNotEmptyException dne) {
                // directories must be removed recursively
                deleteDirectoryContent(tgtPath, logger);
            }
            Files.walk(sourcePath).forEach((Path content) -> {
                try {
                    Path fileDest = tgtPath.resolve(sourcePath.relativize(content));
                    Files.copy(content, fileDest, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    logger.error("Exception copying file " + content + " to " + tgtPath);
                }
            });
        }
    }

    /**
     * Deletes a file.
     * 
     * @param f file to delete
     * @param logger logger where to print the errors
     * @throws IOException error raised during the file deletion
     */
    public static void deleteFile(File f, Logger logger) throws IOException {

        Path directory = f.toPath();
        try {
            if (!Files.deleteIfExists(directory)) {
                if (logger != null) {
                    logger.debug("File " + f.getAbsolutePath() + " not deleted.");
                }
            }
        } catch (DirectoryNotEmptyException dne) {
            // directories must be removed recursively
            deleteDirectoryContent(directory, logger);
            try {
                Files.delete(directory);
            } catch (NoSuchFileException e) {
                // directory has been removed already
                return;
            }
        }
    }

    /**
     * Deletes content of a given directory.
     *
     * @param directory dir that to delete contents of
     * @param logger logger where to print the errors
     * @throws IOException error raised during the file deletion
     */
    private static void deleteDirectoryContent(Path directory, Logger logger) throws IOException {
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
            if (logger != null) {
                logger.error("Cannot delete directory " + directory, e);
            }
            throw e;
        }
    }

    /**
     * moves a file/folder location from a source to a target.
     * 
     * @param source source file
     * @param target target file location
     * @param logger log where to print the errors
     * @throws DirectoryNotEmptyException trying to move a non empty directory
     * @throws IOException error raised during the copy of the file/folder
     */
    public static void moveFile(File source, File target, Logger logger)
        throws DirectoryNotEmptyException, IOException {
        Path tgtPath = target.toPath();
        Path sourcePath = source.toPath();
        if (tgtPath.compareTo(sourcePath) != 0) {
            try {
                Files.move(sourcePath, tgtPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException amnse) {
                logger.warn("WARN: AtomicMoveNotSupportedException."
                    + " File cannot be atomically moved. Trying to move without atomic");
                Files.move(sourcePath, tgtPath, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

}
