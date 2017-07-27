package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.exceptions.LangNotDefinedException;
import es.bsc.compss.types.exceptions.UndefinedConstraintsSourceException;
import es.bsc.compss.util.parsers.IDLParser;
import es.bsc.compss.util.parsers.ITFParser;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CEIParser {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    private static final Lang LANG;

    static {
        // Compute language
        Lang l = Lang.JAVA;

        String langProperty = System.getProperty(COMPSsConstants.LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase(COMPSsConstants.Lang.PYTHON.name())) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase(COMPSsConstants.Lang.C.name())) {
                l = Lang.C;
            }
        }

        LANG = l;
    }


    /**
     * Parses the different interfaces
     * 
     * @return
     */
    public static void parse() {
        switch (LANG) {
            case JAVA:
                String appName = System.getProperty(COMPSsConstants.APP_NAME);
                try {
                    loadJava(Class.forName(appName + "Itf"));
                } catch (ClassNotFoundException ex) {
                    throw new UndefinedConstraintsSourceException(appName + "Itf class cannot be found.");
                }
                break;
            case C:
                String constraintsFile = System.getProperty(COMPSsConstants.CONSTR_FILE);
                loadC(constraintsFile);
                break;
            case PYTHON:
                loadPython();
                break;
            default:
                throw new LangNotDefinedException();
        }
    }

    /**
     * JAVA CONSTRUCTOR
     * 
     * Loads the annotated class and initializes the data structures that contain the constraints. For each method found
     * in the annotated interface creates its signature and adds the constraints to the structures.
     *
     * @param annotItfClass
     *            package and name of the Annotated Interface class
     * @return
     */
    public static List<Integer> loadJava(Class<?> annotItfClass) {
        LOGGER.debug("Loading Java Annotation Interface");
        return ITFParser.parseITFMethods(annotItfClass);
    }

    /*
     * C CONSTRUCTOR
     */
    private static void loadC(String constraintsFile) {
        LOGGER.debug("Loading C Annotation Interface");
        IDLParser.parseIDLMethods(constraintsFile);
    }

    /*
     * PYTHON CONSTRUCTOR
     */
    private static void loadPython() {
        LOGGER.debug("Loading Python Annotation Interface");
        // Nothing to do since python CoreElements are registered through TD Requests
    }

}
