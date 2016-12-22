package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.exceptions.LangNotDefinedException;
import integratedtoolkit.types.exceptions.UndefinedConstraintsSourceException;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.parsers.IDLParser;
import integratedtoolkit.util.parsers.ITFParser;

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CEIParser {

    private static final int DEFAULT_CORE_COUNT_PYTHON = 50;

    private static final Lang LANG;

    private static final Logger logger = LogManager.getLogger(Loggers.TS_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    
    static {
        // Compute language
        Lang l = Lang.JAVA;

        String langProperty = System.getProperty(ITConstants.IT_LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase(ITConstants.Lang.PYTHON.name())) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase(ITConstants.Lang.C.name())) {
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
    public static LinkedList<Integer> parse() {
        LinkedList<Integer> updatedCores = new LinkedList<>();
        switch (LANG) {
            case JAVA:
                String appName = System.getProperty(ITConstants.IT_APP_NAME);
                try {
                    updatedCores = loadJava(Class.forName(appName + "Itf"));
                } catch (ClassNotFoundException ex) {
                    throw new UndefinedConstraintsSourceException(appName + "Itf class cannot be found.");
                }
                break;
            case C:
                String constraintsFile = System.getProperty(ITConstants.IT_CONSTR_FILE);
                updatedCores = loadC(constraintsFile);
                break;
            case PYTHON:
                updatedCores = loadPython();
                break;
            default:
                throw new LangNotDefinedException();
        }
        return updatedCores;
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
    public static LinkedList<Integer> loadJava(Class<?> annotItfClass) {
        return ITFParser.parseITFMethods(annotItfClass);
    }

    
    /*
     * C CONSTRUCTOR
     */
    private static LinkedList<Integer> loadC(String constraintsFile) {
        LinkedList<Integer> updatedMethods = new LinkedList<Integer>();
        HashMap<Integer, LinkedList<MethodImplementation>> readMethodImpls = new HashMap<>();
        HashMap<Integer, LinkedList<String>> readMethodSignatures = new HashMap<>();

        int coreCount = IDLParser.parseIDLMethods(updatedMethods, readMethodImpls, readMethodSignatures, constraintsFile);

        CoreManager.resizeStructures(coreCount);
        for (int i = 0; i < coreCount; i++) {
            LinkedList<MethodImplementation> implList = readMethodImpls.get(i);
            Implementation<?>[] implementations = implList.toArray(new Implementation[implList.size()]);
            LinkedList<String> signaturesList = readMethodSignatures.get(i);
            String[] signatures = signaturesList.toArray(new String[signaturesList.size()]);
            CoreManager.registerImplementations(i, implementations, signatures);
        }
        return updatedMethods;
    }

    /*
     * PYTHON CONSTRUCTOR
     */
    private static LinkedList<Integer> loadPython() {
        // Get python CoreCount
        String countProp = System.getProperty(ITConstants.IT_CORE_COUNT);
        Integer coreCount;
        if (countProp == null) {
            coreCount = DEFAULT_CORE_COUNT_PYTHON;
            if (debug) {
                logger.debug("Warning: using " + coreCount + " as default for number of task types");
            }
        } else {
            coreCount = Integer.parseInt(countProp);
        }

        // Resize runtime structures
        CoreManager.resizeStructures(coreCount);

        // Register implementations
        LinkedList<Integer> updatedMethods = new LinkedList<>();
        for (int i = 0; i < coreCount; i++) {
            Implementation<?>[] implementations = new Implementation[1];
            implementations[0] = new MethodImplementation("", "", i, 0, MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy());
            String[] signatures = new String[] { "" };
            CoreManager.registerImplementations(i, implementations, signatures);

            updatedMethods.add(i);
        }

        // Return index of modified methods
        return updatedMethods;
    }

}
