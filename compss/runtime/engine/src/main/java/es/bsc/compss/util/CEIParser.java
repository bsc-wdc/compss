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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.exceptions.LangNotDefinedException;
import es.bsc.compss.types.exceptions.UndefinedConstraintsSourceException;
import es.bsc.compss.util.parsers.IDLParser;
import es.bsc.compss.util.parsers.ITFParser;
import java.util.LinkedList;

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
            l = Lang.valueOf(langProperty.toUpperCase());
        }

        LANG = l;
    }


    /**
     * Parses the different interfaces.
     */
    public static void parse() {
        switch (LANG) {
            case JAVA:
                String appName = System.getProperty(COMPSsConstants.APP_NAME);
                if (appName != null) {
                    try {
                        loadJava(Class.forName(appName + "Itf"));
                    } catch (ClassNotFoundException ex) {
                        throw new UndefinedConstraintsSourceException(appName + "Itf class cannot be found.");
                    }
                } else {
                    LOGGER.warn("No defined Application Name. COMPSs is not loading any CEI.");
                }
                break;
            case C:
                String constraintsFile = System.getProperty(COMPSsConstants.CONSTR_FILE);
                loadC(constraintsFile);
                break;
            case PYTHON:
                loadPython();
                break;
            case R:
                loadR();
                break;
            case UNKNOWN:
                // No need to load anything. CEIs will be provided at runtime.
                break;
            default:
                throw new LangNotDefinedException();
        }
    }

    /**
     * JAVA CONSTRUCTOR Loads the annotated class and initializes the data structures that contain the constraints. For
     * each method found in the annotated interface creates its signature and adds the constraints to the structures.
     *
     * @param annotItfClass package and name of the Annotated Interface class
     * @return
     */
    public static List<Integer> loadJava(Class<?> annotItfClass) {
        LOGGER.debug("Loading Java Annotation Interface");
        List<Integer> updatedCEs = new LinkedList<>();
        List<CoreElementDefinition> ceds = ITFParser.parseITFMethods(annotItfClass);
        for (CoreElementDefinition ced : ceds) {
            CoreElement ce = CoreManager.registerNewCoreElement(ced);
            Integer methodId = ce.getCoreId();
            updatedCEs.add(methodId);
        }
        return updatedCEs;
    }

    /*
     * C CONSTRUCTOR
     */
    private static void loadC(String constraintsFile) {
        LOGGER.debug("Loading C Annotation Interface");
        List<CoreElementDefinition> ceds = IDLParser.parseIDLMethods(constraintsFile);
        for (CoreElementDefinition ced : ceds) {
            // Create the core Element if it does not exist
            CoreElement ce = CoreManager.registerNewCoreElement(ced);

            Integer coreId = ce.getCoreId();
            String ceSignature = ce.getSignature();
            LOGGER.debug("CoreId for task " + ceSignature + " is " + coreId);
        }
    }

    /*
     * PYTHON CONSTRUCTOR
     */
    private static void loadPython() {
        LOGGER.debug("Loading Python Annotation Interface");
        // Nothing to do since python CoreElements are registered through TD Requests
    }

    /*
     * R CONSTRUCTOR
     */
    private static void loadR() {
        LOGGER.debug("Loading R Annotation Interface");
        // Nothing to do since R CoreElements are registered through TD Requests
    }

}
