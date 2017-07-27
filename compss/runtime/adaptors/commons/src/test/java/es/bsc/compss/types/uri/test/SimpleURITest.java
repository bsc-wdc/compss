package es.bsc.compss.types.uri.test;

import static org.junit.Assert.*;

import org.junit.Test;

import es.bsc.compss.types.uri.SimpleURI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SimpleURITest {

    // Test Logger
    private static final Logger logger = LogManager.getLogger("Console");


    /*
     * *************************************************************** 
     * CONSTRUCTOR TESTS
     * **************************************************************
     */
    @Test
    public void fileURIs() {
        logger.info("Begin File URI Test");
        SimpleURI fileURI = new SimpleURI("file:///home/compss/.COMPSs/file.IT");
        assertEquals("File Schema", fileURI.getSchema(), "file://");
        assertEquals("File Host", fileURI.getHost(), "");
        assertEquals("File Path", fileURI.getPath(), "/home/compss/.COMPSs/file.IT");

        SimpleURI fileURI2 = new SimpleURI("file://localhost@/home/compss/.COMPSs/remoteFile.IT");
        assertEquals("File Schema", fileURI2.getSchema(), "file://");
        assertEquals("File Host", fileURI2.getHost(), "localhost");
        assertEquals("File Path", fileURI2.getPath(), "/home/compss/.COMPSs/remoteFile.IT");
    }

    @Test
    public void objectURIs() {
        logger.info("Begin Object URI Test");
        SimpleURI fileURI = new SimpleURI("object://d1v1RAND.IT");
        assertEquals("File Schema", fileURI.getSchema(), "object://");
        assertEquals("File Host", fileURI.getHost(), "");
        assertEquals("File Path", fileURI.getPath(), "d1v1RAND.IT");

        SimpleURI fileURI2 = new SimpleURI("object://localhost@d1v1RAND.IT");
        assertEquals("File Schema", fileURI2.getSchema(), "object://");
        assertEquals("File Host", fileURI2.getHost(), "localhost");
        assertEquals("File Path", fileURI2.getPath(), "d1v1RAND.IT");
    }

}
