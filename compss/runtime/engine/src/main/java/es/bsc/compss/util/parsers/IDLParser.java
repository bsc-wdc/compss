package es.bsc.compss.util.parsers;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.CoreManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class IDLParser {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // Error constants
    private static final String CONSTR_LOAD_ERR = "Error loading constraints";

    // Parser IDL Tags
    private static final String CONSTRAINT_IDL = "@Constraints";
    private static final String IMPLEMENTS_IDL = "@Implements";
    private static final String PROCESSOR_IDL = "processors";
    private static final String CLASS_METHOD_SEPARATOR = "::";


    private static enum CodeRegion {
        COMMENT, // For comments
        TASK, // For tasks
        CONSTRAINT, // For constraints annotation
        FUNCTION, // Function (not task)
        IMPLEMENTATION // Implementation
    }


    public static void parseIDLMethods(String constraintsFile) {
        LOGGER.debug("Loading file " + constraintsFile);

        try (BufferedReader br = new BufferedReader(new FileReader(constraintsFile))) {
            MethodResourceDescription defaultCtr = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy();
            boolean isReadingCodeRegion = false;
            StringBuilder structureString = new StringBuilder();
            CodeRegion type = null;
            CImplementation implementation = null;
            MethodResourceDescription currConstraints = new MethodResourceDescription(defaultCtr);
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                // System.out.println("Read line: "+ line);
                if (isReadingCodeRegion && type != null) {
                    if (line.startsWith("//")) {
                        // Line is a comment inside the core region ignoring it
                        continue;
                    } else if (type.equals(CodeRegion.COMMENT)) {
                        if (line.endsWith("*/")) {
                            isReadingCodeRegion = false;
                        } else {
                            continue;
                        }
                    } else {
                        if (line.matches(".*[)];")) {
                            isReadingCodeRegion = false;
                            structureString.append(line);
                            if (type.equals(CodeRegion.CONSTRAINT)) {
                                LOGGER.debug("[IDL Parser] Loading constraint: " + structureString.toString());
                                currConstraints = loadCConstraints(structureString.toString());
                            } else if (type.equals(CodeRegion.IMPLEMENTATION)) {
                                LOGGER.debug("[IDL Parser] Loading implementation: " + structureString.toString());
                                implementation = loadCImplementation(structureString.toString());
                            } else if (type.equals(CodeRegion.FUNCTION)) {
                                LOGGER.debug(
                                        "[IDL Parser] Loading function: " + structureString.toString() + " constraint:" + currConstraints);
                                parseCFunction(structureString.toString(), currConstraints, implementation);
                                currConstraints = new MethodResourceDescription(defaultCtr);
                                implementation = null;
                            }
                        } else {
                            structureString.append(line);
                        }
                    }

                } else {
                    if (line.startsWith("//") || line.startsWith("#") || (line.startsWith("/*") && line.endsWith("*/"))) {
                        // Line is a comment of pre-processor pragma ignoring it
                        continue;
                    } else if (line.startsWith("/*")) {
                        // Line starts comment region
                        isReadingCodeRegion = true;
                        type = CodeRegion.COMMENT;
                    } else if (line.matches(CONSTRAINT_IDL + "[(].*[)];")) {
                        // Line contains
                        LOGGER.debug("[IDL Parser] Loading constraint: " + line);
                        currConstraints = loadCConstraints(line);
                        continue;
                    } else if (line.matches(CONSTRAINT_IDL + "[(].*")) {
                        // Line starts a constraints region
                        isReadingCodeRegion = true;
                        structureString = new StringBuilder(line);
                        type = CodeRegion.CONSTRAINT;
                    } else if (line.matches(IMPLEMENTS_IDL + "[(].*[)];")) {
                        // Line implements
                        LOGGER.debug("[IDL Parser] Loading implementation: " + line);
                        implementation = loadCImplementation(line);
                        continue;
                    } else if (line.matches(IMPLEMENTS_IDL + "[(].*")) {
                        // Line starts a constraints region
                        isReadingCodeRegion = true;
                        structureString = new StringBuilder(line);
                        type = CodeRegion.IMPLEMENTATION;
                    } else if (line.matches(".*[(].*[)];")) {
                        // Line contains a function
                        LOGGER.debug("[IDL Parser] Loading function: " + line + " constraint:" + currConstraints);
                        parseCFunction(line, currConstraints, implementation);

                        currConstraints = new MethodResourceDescription(defaultCtr);
                        implementation = null;
                    } else if (line.matches(".*[(].*")) {
                        // Line starts a function region
                        isReadingCodeRegion = true;
                        structureString = new StringBuilder(line);
                        type = CodeRegion.FUNCTION;
                    }
                }
            }
        } catch (IOException ioe) {
            LOGGER.fatal(CONSTR_LOAD_ERR, ioe);
        }
    }

    private static CImplementation loadCImplementation(String line) {
        if (line.startsWith(IMPLEMENTS_IDL)) {
            line = line.substring(line.indexOf("(") + 1, line.indexOf(")"));
        }
        int indexOfSeparator = line.indexOf(CLASS_METHOD_SEPARATOR);
        if (indexOfSeparator > 0) {
            String className = line.substring(0, indexOfSeparator);
            // String methodName = line.substring(indexOfSeparator + CLASS_METHOD_SEPARATOR.length());
            StringBuilder methodNameBuilder = new StringBuilder();
            methodNameBuilder.append(className).append(CLASS_METHOD_SEPARATOR)
                    .append(line.substring(indexOfSeparator + CLASS_METHOD_SEPARATOR.length()));
            String methodName = methodNameBuilder.toString();
            // logger.debug("New C method implementation: "+className+"::"+methodName);
            LOGGER.debug("New C method implementation:");
            LOGGER.debug("\t Classname: " + className);
            LOGGER.debug("\t Methodname: " + methodName);
            return new CImplementation(className, methodName);
        } else {
            // logger.debug("New C method implementation: "+line);
            return new CImplementation("NULL", line);
        }
    }

    private static void parseCFunction(String line, MethodResourceDescription currConstraints, CImplementation implementation) {
        StringBuilder implementedTaskSignatureBuffer = new StringBuilder();
        StringBuilder implementationSignatureBuffer = new StringBuilder();
        // boolean isStatic = false;
        boolean hasReturn = false;
        if (line.startsWith("static ")) {
            // isStatic = true;
            line = line.replace("static ", "");
        }
        if (!line.startsWith("void ")) {
            hasReturn = true;
        }
        line = line.replaceAll("[(|)|,|;|\n|\t]", " ");
        String[] splits = line.split("\\s+");
        CImplementation task = loadCImplementation(splits[1]);
        String methodName = task.getMethodName();
        String declaringClass = task.getClassName();
        if (implementation != null) {
            implementedTaskSignatureBuffer.append(implementation.getMethodName()).append("(");
        } else {
            implementedTaskSignatureBuffer.append(methodName).append("(");
        }
        implementationSignatureBuffer.append(methodName).append("(");
        /*
         * if (declaringClass!="NULL" && !isStatic){ implementedTaskSignatureBuffer.append("FILE_T").append(",");
         * implementationSignatureBuffer.append("FILE_T").append(","); }
         */
        if (hasReturn) {
            implementedTaskSignatureBuffer.append("FILE_T").append(",");
            implementationSignatureBuffer.append("FILE_T").append(",");
        }
        // Computes the method's signature
        for (int i = 2; i < splits.length; i++) {
            String paramDirection = splits[i++];
            String paramType = splits[i++];
            String type = "FILE_T";
            /*
             * OLD version C-binding String type = "OBJECT_T";
             */
            if (paramDirection.toUpperCase().compareTo("INOUT") == 0) {
                type = "FILE_T";
            } else if (paramDirection.toUpperCase().compareTo("OUT") == 0) {
                type = "FILE_T";
            } else if (paramType.toUpperCase().compareTo("FILE") == 0) {
                type = "FILE_T";
            } else if (paramType.compareTo("boolean") == 0) {
                type = "BOOLEAN_T";
            } else if (paramType.compareTo("char") == 0) {
                type = "CHAR_T";
            } else if (paramType.compareTo("int") == 0) {
                type = "INT_T";
            } else if (paramType.compareTo("float") == 0) {
                type = "FLOAT_T";
            } else if (paramType.compareTo("double") == 0) {
                type = "DOUBLE_T";
            } else if (paramType.compareTo("byte") == 0) {
                type = "BYTE_T";
            } else if (paramType.compareTo("short") == 0) {
                type = "SHORT_T";
            } else if (paramType.compareTo("long") == 0) {
                type = "LONG_T";
            } else if (paramType.compareTo("string") == 0) {
                type = "STRING_T";
            }
            implementedTaskSignatureBuffer.append(type).append(",");
            implementationSignatureBuffer.append(type).append(",");
            // String paramName = splits[i];
        }
        implementedTaskSignatureBuffer.deleteCharAt(implementedTaskSignatureBuffer.lastIndexOf(","));
        implementationSignatureBuffer.deleteCharAt(implementationSignatureBuffer.lastIndexOf(","));
        implementedTaskSignatureBuffer.append(")");
        implementationSignatureBuffer.append(")");
        if (implementation != null) {
            implementedTaskSignatureBuffer.append(implementation.getClassName());
        } else {
            implementedTaskSignatureBuffer.append(declaringClass);
        }
        implementationSignatureBuffer.append(declaringClass);

        String taskSignature = implementedTaskSignatureBuffer.toString();
        String implementationSignature = implementationSignatureBuffer.toString();

        // Create the core Element if it does not exist
        Integer coreId = CoreManager.registerNewCoreElement(taskSignature);
        if (coreId == null) {
            // The coreId already exists
            coreId = CoreManager.getCoreId(taskSignature);
        }
        LOGGER.debug("CoreId for task " + taskSignature + " is " + coreId);

        // Add the implementation to the core element
        int implId = CoreManager.getNumberCoreImplementations(coreId);
        List<Implementation> newImpls = new LinkedList<>();
        MethodImplementation m = new MethodImplementation(declaringClass, methodName, coreId, implId, currConstraints);
        newImpls.add(m);
        List<String> newSigns = new LinkedList<>();
        newSigns.add(implementationSignature);
        CoreManager.registerNewImplementations(coreId, newImpls, newSigns);
        LOGGER.debug("[IDL Parser] Adding implementation: " + declaringClass + "." + methodName + " for CE id " + coreId);
    }

    private static MethodResourceDescription loadCConstraints(String line) {
        line = line.substring(CONSTRAINT_IDL.length() + 1);
        String proc = new String();

        if (line.matches(".*" + PROCESSOR_IDL + ".*")) {
            int procStart = line.indexOf("{");
            int procEnd = line.indexOf("}");
            proc = line.substring(procStart, procEnd + 1);
            line = line.replace(proc, "");
            line = line.replace("processors=", "");
            proc = proc.replaceAll("[{}]", "");
            LOGGER.debug("[IDL Parser] Loading processors: " + proc);
            line = line.replaceFirst(",", "");
        }

        line = line.replaceAll("[() ;\n\t]", "");
        String[] constraints = line.split(",");

        MethodResourceDescription mrd = new MethodResourceDescription(constraints, proc);
        // logger.debug("New Constraints detected: " + mrd);
        return mrd;
    }


    private static class CImplementation {

        private final String className;
        private final String methodName;


        public CImplementation(String className, String methodName) {
            this.className = className;
            this.methodName = methodName;
        }

        public String getClassName() {
            return this.className;
        }

        public String getMethodName() {
            return this.methodName;
        }

    }

}
