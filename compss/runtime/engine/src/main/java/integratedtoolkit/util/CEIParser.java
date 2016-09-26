package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.ServiceImplementation;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.MultiConstraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Service;
import integratedtoolkit.types.exceptions.LangNotDefinedException;
import integratedtoolkit.types.exceptions.UndefinedConstraintsSourceException;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CEIParser {

    private static ITConstants.Lang lang = ITConstants.Lang.JAVA;

    private static final Logger logger = LogManager.getLogger(Loggers.TS_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    static {
        String l = System.getProperty(ITConstants.IT_LANG);
        lang = ITConstants.Lang.JAVA;
        if (l != null) {
            if (l.equalsIgnoreCase("c")) {
                lang = ITConstants.Lang.C;
            } else if (l.equalsIgnoreCase("python")) {
                lang = ITConstants.Lang.PYTHON;
            }
        }
    }


    public static LinkedList<Integer> parse() {
        LinkedList<Integer> updatedCores = new LinkedList<Integer>();
        switch (lang) {
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
     * Treats and display possible warnings related to ONE annotation of the Method/Service m
     * 
     * @param m
     *            The method or service to be checked for warnings
     * @param type
     *            Type of the annotation
     * @param i
     *            Number of the annotation (0 for the first parameter, 1 for the second, etc.)
     */
    private static void treatAnnotationWarnings(java.lang.reflect.Method m, int i) {
        int n = i + 1;
        Parameter par = ((Parameter) m.getParameterAnnotations()[i][0]);
        Parameter.Type annotType = par.type();

        final String WARNING_LOCATION = "In parameter number " + n + " of method '" + m.getName() + "' in interface '"
                + m.getDeclaringClass().toString().replace("interface ", "") + "'.";

        /*
         * ////////// Uncomment to enable warnings when the type of a parameter is inferred String inferredType =
         * inferType(m.getParameterTypes()[i], annotType); if (annotType.equals(Parameter.Type.UNSPECIFIED)) { //Using
         * inferred type, warn user ErrorManager.warn("No type specified for parameter number " + n + " of method '" +
         * m.getName() + "'." + ErrorManager.NEWLINE + WARNING_LOCATION + ErrorManager.NEWLINE + "Using inferred type "
         * + inferredType + "."); }
         */

        Parameter.Direction annotDirection = par.direction();
        boolean outOrInout = annotDirection.equals(Parameter.Direction.OUT) || annotDirection.equals(Parameter.Direction.INOUT);
        if (annotType.equals(Parameter.Type.STRING) && outOrInout) {
            ErrorManager.warn("Can't specify a String with direction OUT/INOUT since they are immutable." + ErrorManager.NEWLINE
                    + WARNING_LOCATION + ErrorManager.NEWLINE + "Using direction=IN instead.");
        } else if (annotType.equals(Parameter.Type.OBJECT) && annotDirection.equals(Parameter.Direction.OUT)) {
            ErrorManager.warn("Can't specify an Object with direction OUT." + ErrorManager.NEWLINE + WARNING_LOCATION + ErrorManager.NEWLINE
                    + "Using direction=INOUT instead.");
        } else if (m.getParameterTypes()[i].isPrimitive() && outOrInout) {
            // int, boolean, long, float, char, byte, short, double
            String primType = m.getParameterTypes()[i].getName();
            ErrorManager.warn("Can't specify a primitive type ('" + primType + "') with direction OUT/INOUT, "
                    + "since they are always passed by value. " + ErrorManager.NEWLINE + WARNING_LOCATION + ErrorManager.NEWLINE
                    + "Using direction=IN instead.");
        }
    }

    /**
     * Loads the annotated class and initializes the data structures that contain the constraints. For each method found
     * in the annotated interface creates its signature and adds the constraints to the structures.
     *
     * @param annotItfClass
     *            package and name of the Annotated Interface class
     * @return
     */
    public static LinkedList<Integer> loadJava(Class<?> annotItfClass) {
        LinkedList<Integer> updatedMethods = new LinkedList<Integer>();
        int coreCount = annotItfClass.getDeclaredMethods().length;
        if (debug) {
            logger.debug("Detected methods " + coreCount);
        }
        if (CoreManager.getCoreCount() == 0) {
            CoreManager.resizeStructures(coreCount);
        } else {
            CoreManager.resizeStructures(CoreManager.getCoreCount() + coreCount);
        }

        for (java.lang.reflect.Method m : annotItfClass.getDeclaredMethods()) {
            // Computes the method's signature
            if (debug) {
                logger.debug("Evaluating method " + m.getName());
            }
            StringBuilder buffer = new StringBuilder();
            String methodName = m.getName();
            buffer.append(methodName).append("(");
            int numPars = m.getParameterAnnotations().length;
            String type;
            if (numPars > 0) {
                for (int i = 0; i < numPars; i++) {
                    Parameter.Type annotType = ((Parameter) m.getParameterAnnotations()[i][0]).type();
                    type = inferType(m.getParameterTypes()[i], annotType);
                    if (i >= 1) {
                        buffer.append(",");
                    }
                    buffer.append(type);
                    treatAnnotationWarnings(m, i);
                }
            }
            buffer.append(")");
            if (m.isAnnotationPresent(Method.class)) {
                String methodSignature = buffer.toString();
                Method methodAnnot = m.getAnnotation(Method.class);
                String[] declaringClasses = methodAnnot.declaringClass();
                int implementationCount = declaringClasses.length;
                String[] signatures = new String[implementationCount];
                for (int i = 0; i < signatures.length; i++) {
                    signatures[i] = methodSignature + declaringClasses[i];
                }
                Integer methodId = CoreManager.getCoreId(signatures);
                updatedMethods.add(methodId);
                if (methodId == CoreManager.getCoreCount()) {
                    CoreManager.increaseCoreCount();
                }
                MethodResourceDescription defaultConstraints = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy();
                MethodResourceDescription[] implConstraints = new MethodResourceDescription[implementationCount];
                if (m.isAnnotationPresent(Constraints.class)) {
                    defaultConstraints = new MethodResourceDescription(m.getAnnotation(Constraints.class));
                }
                if (m.isAnnotationPresent(MultiConstraints.class)) {
                    MultiConstraints mc = m.getAnnotation(MultiConstraints.class);
                    mc.value();
                    for (int i = 0; i < implementationCount; i++) {
                        MethodResourceDescription specificConstraints = new MethodResourceDescription(mc.value()[i]);
                        specificConstraints.merge(defaultConstraints);
                        implConstraints[i] = specificConstraints;
                    }
                } else {
                    for (int i = 0; i < implementationCount; i++) {
                        implConstraints[i] = defaultConstraints;
                    }
                }
                for (int i = 0; i < implementationCount; i++) {
                    loadMethodConstraints(methodId, implementationCount, methodName, declaringClasses, implConstraints);
                }
            } else { // Service
                Service serviceAnnot = m.getAnnotation(Service.class);
                buffer.append(serviceAnnot.namespace()).append(',').append(serviceAnnot.name()).append(',').append(serviceAnnot.port());
                String signature = buffer.toString();
                Integer methodId = CoreManager.getCoreId(new String[] { signature });
                if (methodId == CoreManager.getCoreCount()) {
                    CoreManager.increaseCoreCount();
                    updatedMethods.add(methodId);
                }
                loadServiceConstraints(methodId, serviceAnnot);
            }
        }

        return updatedMethods;
    }

    /**
     * Loads the Constraints in case that core is a service. Only in Xpath format since there are no resource where its
     * tasks can run
     *
     * @param coreId
     *            identifier for that core
     * @param service
     *            Service annotation describing the core
     */
    private static void loadServiceConstraints(int coreId, Service service) {
        Implementation<?>[] implementations = new Implementation[1];
        implementations[0] = new ServiceImplementation(coreId, service.namespace(), service.name(), service.port(), service.operation());
        CoreManager.registerImplementations(coreId, implementations);
    }

    /**
     * Loads the Constraints in case that core is a service in XPath format and describing the features of the resources
     * able to run its tasks
     *
     * @param coreId
     *            identifier for that core
     * @param service
     *            Method annotation describing the core
     */
    private static void loadMethodConstraints(int coreId, int implementationCount, String methodName, String[] declaringClasses,
            MethodResourceDescription[] cts) {
        Implementation<?>[] implementations = new Implementation[implementationCount];
        for (int i = 0; i < implementationCount; i++) {
            implementations[i] = new MethodImplementation(declaringClasses[i], methodName, coreId, i, cts[i]);
        }
        CoreManager.registerImplementations(coreId, implementations);
    }

    // C constructor
    public static LinkedList<Integer> loadC(String constraintsFile) {
        LinkedList<Integer> updatedMethods = new LinkedList<Integer>();
        HashMap<Integer, LinkedList<MethodImplementation>> readMethods = new HashMap<Integer, LinkedList<MethodImplementation>>();

        int coreCount = IDLParser.parseIDLMethods(updatedMethods, readMethods, constraintsFile);

        CoreManager.resizeStructures(coreCount);
        for (int i = 0; i < coreCount; i++) {
            LinkedList<MethodImplementation> implList = readMethods.get(i);
            Implementation<?>[] implementations = implList.toArray(new Implementation[implList.size()]);
            CoreManager.registerImplementations(i, implementations);
        }
        CoreManager.setCoreCount(coreCount);
        return updatedMethods;
    }

    // Python constructor
    private static LinkedList<Integer> loadPython() {
        // Get python CoreCount
        String countProp = System.getProperty(ITConstants.IT_CORE_COUNT);
        Integer coreCount;
        if (countProp == null) {
            coreCount = 50;
            if (debug) {
                logger.debug("Warning: using " + coreCount + " as default for number of task types");
            }
        } else {
            coreCount = Integer.parseInt(countProp);
        }

        // Resize runtime structures
        CoreManager.resizeStructures(coreCount);

        // Register implementations
        LinkedList<Integer> updatedMethods = new LinkedList<Integer>();
        for (int i = 0; i < coreCount; i++) {
            Implementation<?>[] implementations = new Implementation[1];
            implementations[0] = new MethodImplementation("", "", i, 0, MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy());
            CoreManager.registerImplementations(i, implementations);

            updatedMethods.add(i);
        }
        // Update coreCount (enable all new registers)
        CoreManager.setCoreCount(coreCount);

        // Return index of modified methods
        return updatedMethods;
    }

    /**
     * Infers the type of a parameter. If the parameter is annotated as a FILE or a STRING, the type is taken from the
     * annotation. If the annotation is UNSPECIFIED, the type is taken from the formal type.
     *
     * @param formalType
     *            Formal type of the parameter
     * @param annotType
     *            Annotation type of the parameter
     * @return A String representing the type of the parameter
     */
    private static String inferType(Class<?> formalType, Parameter.Type annotType) {
        if (annotType.equals(Parameter.Type.UNSPECIFIED)) {
            if (formalType.isPrimitive()) {
                if (formalType.equals(boolean.class)) {
                    return "BOOLEAN_T";
                } else if (formalType.equals(char.class)) {
                    return "CHAR_T";
                } else if (formalType.equals(byte.class)) {
                    return "BYTE_T";
                } else if (formalType.equals(short.class)) {
                    return "SHORT_T";
                } else if (formalType.equals(int.class)) {
                    return "INT_T";
                } else if (formalType.equals(long.class)) {
                    return "LONG_T";
                } else if (formalType.equals(float.class)) {
                    return "FLOAT_T";
                } else // if (formalType.equals(double.class))
                {
                    return "DOUBLE_T";
                }
            } /*
               * else if (formalType.isArray()) { // If necessary }
               */else { // Object
                return "OBJECT_T";
            }
        } else {
            return annotType + "_T";
        }
    }
    
}
