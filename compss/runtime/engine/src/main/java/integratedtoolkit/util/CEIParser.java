package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.loader.LoaderUtils;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.SchedulerHints;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.MPI;
import integratedtoolkit.types.annotations.task.Method;
import integratedtoolkit.types.annotations.task.OmpSs;
import integratedtoolkit.types.annotations.task.OpenCL;
import integratedtoolkit.types.annotations.task.Service;
import integratedtoolkit.types.annotations.task.repeatables.Binaries;
import integratedtoolkit.types.annotations.task.repeatables.MPIs;
import integratedtoolkit.types.annotations.task.repeatables.Methods;
import integratedtoolkit.types.annotations.task.repeatables.MultiOmpSs;
import integratedtoolkit.types.annotations.task.repeatables.OpenCLs;
import integratedtoolkit.types.annotations.task.repeatables.Services;
import integratedtoolkit.types.exceptions.LangNotDefinedException;
import integratedtoolkit.types.exceptions.UndefinedConstraintsSourceException;
import integratedtoolkit.types.implementations.BinaryImplementation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MPIImplementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.OmpSsImplementation;
import integratedtoolkit.types.implementations.OpenCLImplementation;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
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
     * Treats and display possible warnings related to ONE annotation of the Method/Service m
     * 
     * @param m
     *            The method or service to be checked for warnings
     * @param type
     *            Type of the annotation
     * @param i
     *            Number of the annotation (0 for the first parameter, 1 for the second, etc.)
     */
    private static void treatParameterAnnotationWarnings(java.lang.reflect.Method m, int i) {
        int paramNumber = i + 1;
        Parameter par = ((Parameter) m.getParameterAnnotations()[i][0]);
        Parameter.Type annotType = par.type();

        final String WARNING_LOCATION = "In parameter number " + paramNumber + " of method '" + m.getName() + "' in interface '"
                + m.getDeclaringClass().toString().replace("interface ", "") + "'.";

        /*
         * ////////// Uncomment to enable warnings when the type of a parameter is inferred 
         * String inferredType = inferType(m.getParameterTypes()[i], annotType); 
         * if (annotType.equals(Parameter.Type.UNSPECIFIED)) { // Using inferred type, warn user 
         *   ErrorManager.warn("No type specified for parameter number " + n + " of method '"
         *     + m.getName() + "'." + ErrorManager.NEWLINE + WARNING_LOCATION + ErrorManager.NEWLINE + "Using inferred type "
         *     + inferredType + "."); 
         * }
         */

        Parameter.Direction annotDirection = par.direction();
        boolean outOrInout = annotDirection.equals(Parameter.Direction.OUT) || annotDirection.equals(Parameter.Direction.INOUT);
        
        if (annotType.equals(Parameter.Type.STRING) && outOrInout) {
            ErrorManager.warn("Can't specify a String with direction OUT/INOUT since they are immutable." 
                    + ErrorManager.NEWLINE + WARNING_LOCATION 
                    + ErrorManager.NEWLINE + "Using direction=IN instead."
            );
        } else if (annotType.equals(Parameter.Type.OBJECT) && annotDirection.equals(Parameter.Direction.OUT)) {
            ErrorManager.warn("Can't specify an Object with direction OUT." 
                    + ErrorManager.NEWLINE + WARNING_LOCATION 
                    + ErrorManager.NEWLINE + "Using direction=INOUT instead."
            );
        } else if (m.getParameterTypes()[i].isPrimitive() && outOrInout) {
            // int, boolean, long, float, char, byte, short, double
            String primType = m.getParameterTypes()[i].getName();
            ErrorManager.warn("Can't specify a primitive type ('" + primType + "') with direction OUT/INOUT, "
                    + "since they are always passed by value. " + ErrorManager.NEWLINE 
                    + WARNING_LOCATION + ErrorManager.NEWLINE + "Using direction=IN instead."
            );
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
        CoreManager.resizeStructures(CoreManager.getCoreCount() + coreCount);

        for (java.lang.reflect.Method m : annotItfClass.getDeclaredMethods()) {
            /*
             *  Computes the callee method signature
             */
            logger.info("Evaluating method " + m.getName());

            StringBuilder calleeMethodSignature = new StringBuilder();
            String methodName = m.getName();
            calleeMethodSignature.append(methodName).append("(");
            int numPars = m.getParameterAnnotations().length;
            String type;
            if (numPars > 0) {
                for (int i = 0; i < numPars; i++) {
                    Parameter.Type annotType = ((Parameter) m.getParameterAnnotations()[i][0]).type();
                    type = inferType(m.getParameterTypes()[i], annotType);
                    if (i >= 1) {
                        calleeMethodSignature.append(",");
                    }
                    calleeMethodSignature.append(type);
                    treatParameterAnnotationWarnings(m, i);
                }
            }
            calleeMethodSignature.append(")");
            
            /*
             * Global constraints of the method
             */
            MethodResourceDescription defaultConstraints = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy();
            if (m.isAnnotationPresent(Constraints.class)) {
                defaultConstraints = new MethodResourceDescription(m.getAnnotation(Constraints.class));
            }
            
            /*
             * Check that all method annotations are valid
             */
            for (Annotation annot : m.getAnnotations()) {
                if ( !annot.annotationType().getName().equals(Constraints.class.getName())
                        // Simple annotations
                        && !annot.annotationType().getName().equals(Method.class.getName())
                        && !annot.annotationType().getName().equals(Service.class.getName())
                        && !annot.annotationType().getName().equals(MPI.class.getName())
                        && !annot.annotationType().getName().equals(OmpSs.class.getName())
                        && !annot.annotationType().getName().equals(OpenCL.class.getName())
                        && !annot.annotationType().getName().equals(Binary.class.getName())
                        // Repeatable annotations
                        && !annot.annotationType().getName().equals(Methods.class.getName())
                        && !annot.annotationType().getName().equals(Services.class.getName())
                        && !annot.annotationType().getName().equals(MPIs.class.getName())
                        && !annot.annotationType().getName().equals(MultiOmpSs.class.getName())
                        && !annot.annotationType().getName().equals(OpenCLs.class.getName())
                        && !annot.annotationType().getName().equals(Binaries.class.getName())
                        // Scheduler hints
                        && !annot.annotationType().getName().equals(SchedulerHints.class.getName())
                        ) {
                    
                    ErrorManager.warn("Unrecognised annotation " + annot.annotationType().getName() + " . SKIPPING");
                }
            }

            
            /*
             * Check all annotations present at the method for versioning
             */
            Integer methodId = CoreManager.registerCoreId(calleeMethodSignature.toString());            
            if (debug) {
                logger.debug("   * Method methodId = " + methodId + " has "+ m.getAnnotations().length + " annotations");
            }
            
            ArrayList<Implementation<?>> implementations = new ArrayList<>();
            ArrayList<String> signatures = new ArrayList<>();
            int implId = 0;
            
            /*
             * METHOD
             */
            for (Method methodAnnot : m.getAnnotationsByType(Method.class)) {
                logger.debug("   * Processing @Method annotation");
                String declaringClass = methodAnnot.declaringClass();
                
                String methodSignature = calleeMethodSignature.toString() + declaringClass;
                signatures.add(methodSignature);
                
                // Load specific method constraints if present
                MethodResourceDescription implConstraints = defaultConstraints;
                if (methodAnnot.constraints() != null) {
                    implConstraints = new MethodResourceDescription(methodAnnot.constraints());
                    implConstraints.mergeMultiConstraints(defaultConstraints);
                }
                
                // Register method implementation                    
                Implementation<?> impl = new MethodImplementation(  declaringClass, 
                                                                    methodName, 
                                                                    methodId, 
                                                                    implId, 
                                                                    implConstraints
                                                                 );                     
                ++implId;
                implementations.add(impl);
            }
            
            /*
             * SERVICE
             */
            for (Service serviceAnnot : m.getAnnotationsByType(Service.class)) {
                // Services don't have constraints
                logger.debug("   * Processing @Service annotation");
                calleeMethodSignature.append(serviceAnnot.namespace()).append(',');
                calleeMethodSignature.append(serviceAnnot.name()).append(',');
                calleeMethodSignature.append(serviceAnnot.port());
                
                String serviceSignature = calleeMethodSignature.toString();
                signatures.add(serviceSignature);

                // Register service implementation
                Implementation<?> impl = new ServiceImplementation( methodId, 
                                                                    serviceAnnot.namespace(), 
                                                                    serviceAnnot.name(),
                                                                    serviceAnnot.port(), 
                                                                    serviceAnnot.operation()
                                                                  );
                implementations.add(impl);
            }
            
            /*
             * MPI
             */
            for (MPI mpiAnnot : m.getAnnotationsByType(MPI.class)) {
                logger.debug("   * Processing @MPI annotation");
                
                String binary = EnvironmentLoader.loadFromEnvironment(mpiAnnot.binary());
                String mpiRunner = EnvironmentLoader.loadFromEnvironment(mpiAnnot.mpiRunner());

                if (mpiRunner == null || mpiRunner.isEmpty()) {
                    ErrorManager.error("Empty mpiRunner annotation for method " + m.getName());
                }
                if (binary == null || binary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for method " + m.getName());
                }
                
                logger.debug("Binary: " + binary);
                logger.debug("mpiRunner: " + mpiRunner);
                
                String mpiSignature = calleeMethodSignature.toString() + LoaderUtils.MPI_SIGNATURE;
                signatures.add(mpiSignature);
                
                // Load specific method constraints if present
                MethodResourceDescription implConstraints = defaultConstraints;
                if (mpiAnnot.constraints() != null) {
                    implConstraints = new MethodResourceDescription(mpiAnnot.constraints());
                    implConstraints.mergeMultiConstraints(defaultConstraints);
                }
                
                // Register method implementation                    
                Implementation<?> impl = new MPIImplementation( binary, 
                                                                mpiRunner, 
                                                                methodId, 
                                                                implId, 
                                                                implConstraints
                                                               );                     
                ++implId;
                implementations.add(impl);
            }

            /*
             * OMPSS
             */
            for (OmpSs ompssAnnot : m.getAnnotationsByType(OmpSs.class)) {
                logger.debug("   * Processing @OmpSs annotation");
                String binary = EnvironmentLoader.loadFromEnvironment(ompssAnnot.binary());

                if (binary == null || binary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for method " + m.getName());
                }
                
                String ompssSignature = calleeMethodSignature.toString() + LoaderUtils.OMPSS_SIGNATURE;
                signatures.add(ompssSignature);
                
                // Load specific method constraints if present
                MethodResourceDescription implConstraints = defaultConstraints;
                if (ompssAnnot.constraints() != null) {
                    implConstraints = new MethodResourceDescription(ompssAnnot.constraints());
                    implConstraints.mergeMultiConstraints(defaultConstraints);
                }
                
                // Register method implementation                    
                Implementation<?> impl = new OmpSsImplementation( binary, 
                                                                  methodId, 
                                                                  implId, 
                                                                  implConstraints
                                                                );                
                ++implId;
                implementations.add(impl);
            }
            
            /*
             * OPENCL
             */
            for (OpenCL openclAnnot : m.getAnnotationsByType(OpenCL.class)) {
                logger.debug("   * Processing @OpenCL annotation");
                String kernel = EnvironmentLoader.loadFromEnvironment(openclAnnot.kernel());

                if (kernel == null || kernel.isEmpty()) {
                    ErrorManager.error("Empty kernel annotation for method " + m.getName());
                }
                
                String openclSignature = calleeMethodSignature.toString() + LoaderUtils.OPENCL_SIGNATURE;
                signatures.add(openclSignature);
                
                // Load specific method constraints if present
                MethodResourceDescription implConstraints = defaultConstraints;
                if (openclAnnot.constraints() != null) {
                    implConstraints = new MethodResourceDescription(openclAnnot.constraints());
                    implConstraints.mergeMultiConstraints(defaultConstraints);
                }
                
                // Register method implementation                    
                Implementation<?> impl = new OpenCLImplementation( kernel, 
                                                                   methodId, 
                                                                   implId, 
                                                                   implConstraints
                                                                 );                     
                ++implId;
                implementations.add(impl);
            }
            
            /*
             * BINARY
             */
            for (Binary binaryAnnot : m.getAnnotationsByType(Binary.class)) {
                logger.debug("   * Processing @Binary annotation");
                String binary = EnvironmentLoader.loadFromEnvironment(binaryAnnot.binary());
                
                if (binary == null || binary.isEmpty()) {
                    ErrorManager.error("Empty binary annotation for method " + m.getName());
                }
                
                String binarySignature = calleeMethodSignature.toString() + LoaderUtils.BINARY_SIGNATURE;
                signatures.add(binarySignature);
                
                // Load specific method constraints if present
                MethodResourceDescription implConstraints = defaultConstraints;
                if (binaryAnnot.constraints() != null) {
                    implConstraints = new MethodResourceDescription(binaryAnnot.constraints());
                    implConstraints.mergeMultiConstraints(defaultConstraints);
                }
                
                // Register method implementation                    
                Implementation<?> impl = new BinaryImplementation( binary, 
                                                                   methodId, 
                                                                   implId, 
                                                                   implConstraints
                                                                 );                   
                ++implId;
                implementations.add(impl);
            }
            
            // Register all implementations
            updatedMethods.add(methodId);
            Implementation<?>[] impls = new Implementation<?>[implementations.size()];
            for (int i = 0; i < implementations.size(); ++i) {
                impls[i] = implementations.get(i);
            }
            String[] signs = new String[signatures.size()];
            for (int i = 0; i < signatures.size(); ++i) {
                signs[i] = signatures.get(i);
            }
            CoreManager.registerImplementations(methodId, impls, signs);
        
            // END FOR DECLARED METHOD IN ITF
        }

        return updatedMethods;
    }

    // C constructor
    public static LinkedList<Integer> loadC(String constraintsFile) {
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

    // Python constructor
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
