package integratedtoolkit.util.parsers;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class IDLParser {

    private static final Logger logger = LogManager.getLogger(Loggers.TS_COMP);

    private static final String CONSTR_LOAD_ERR = "Error loading constraints";

    private static final String CONSTRAINT_IDL = "@Constraints";
    private static final String IMPLEMENTS_IDL = "@Implements";
    private static final String PROCESSOR_IDL = "processors";
    private static final String CLASS_METHOD_SEPARATOR = "::";


    private static enum CodeRegion {
        COMMENT, 
        TASK, 
        CONSTRAINT, 
        FUNCTION, 
        IMPLEMENTATION
    }


    public static int parseIDLMethods(LinkedList<Integer> updatedMethods,
            HashMap<Integer, LinkedList<MethodImplementation>> readMethodImpls, HashMap<Integer, LinkedList<String>> readMethodSignatures,
            String constraintsFile) {
        
        MethodResourceDescription defaultCtr = MethodResourceDescription.EMPTY_FOR_CONSTRAINTS.copy();

        logger.debug("Loading file " + constraintsFile);
        BufferedReader br = null;
        String line;
        int coreCount = 0;
        try {
            br = new BufferedReader(new FileReader(constraintsFile));
            boolean isReadingCodeRegion = false;
            StringBuilder structureString = new StringBuilder();
            CodeRegion type = null;
            CImplementation implementation = null;
            MethodResourceDescription currConstraints = new MethodResourceDescription(defaultCtr);
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
                                logger.debug("[IDL Parser] Loading constraint: " + structureString.toString());
                                currConstraints = loadCConstraints(structureString.toString());
                            } else if (type.equals(CodeRegion.IMPLEMENTATION)) {
                                logger.debug("[IDL Parser] Loading implementation: " + structureString.toString());
                                implementation = loadCImplementation(structureString.toString());
                            } else if (type.equals(CodeRegion.FUNCTION)) {
                                logger.debug(
                                        "[IDL Parser] Loading function: " + structureString.toString() + " constraint:" + currConstraints);
                                parseCFunction(structureString.toString(), updatedMethods, readMethodImpls, readMethodSignatures,
                                        currConstraints, implementation);

                                if (implementation == null) {
                                    coreCount++;
                                }
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
                        logger.debug("[IDL Parser] Loading constraint: " + line);
                        currConstraints = loadCConstraints(line);
                        continue;
                    } else if (line.matches(CONSTRAINT_IDL + "[(].*")) {
                        // Line starts a constraints region
                        isReadingCodeRegion = true;
                        structureString = new StringBuilder(line);
                        type = CodeRegion.CONSTRAINT;
                    } else if (line.matches(IMPLEMENTS_IDL + "[(].*[)];")) {
                        // Line implements
                        logger.debug("[IDL Parser] Loading implementation: " + line);
                        implementation = loadCImplementation(line);
                        continue;
                    } else if (line.matches(IMPLEMENTS_IDL + "[(].*")) {
                        // Line starts a constraints region
                        isReadingCodeRegion = true;
                        structureString = new StringBuilder(line);
                        type = CodeRegion.IMPLEMENTATION;
                    } else if (line.matches(".*[(].*[)];")) {
                        // Line contains a function
                        logger.debug("[IDL Parser] Loading function: " + line + " constraint:" + currConstraints);
                        parseCFunction(line, updatedMethods, readMethodImpls, readMethodSignatures, currConstraints, implementation);
                        if (implementation == null) {
                            coreCount++;
                        }
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
        } catch (Exception e) {
            logger.fatal(CONSTR_LOAD_ERR, e);
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException e) {
                    // Nothing to do;
                }
        }

        return coreCount;

    }

    private static CImplementation loadCImplementation(String line) {
        if (line.startsWith(IMPLEMENTS_IDL)) {
            line = line.substring(line.indexOf("(") + 1, line.indexOf(")"));
        }
        int indexOfSeparator = line.indexOf(CLASS_METHOD_SEPARATOR);
        if (indexOfSeparator > 0) {
            String className = line.substring(indexOfSeparator);
            String methodName = line.substring(indexOfSeparator + CLASS_METHOD_SEPARATOR.length(), line.length() - 1);
            // logger.debug("New C method implementation: "+className+"::"+methodName);
            return new CImplementation(className, methodName);
        } else {
            // logger.debug("New C method implementation: "+line);
            return new CImplementation("NULL", line);
        }
    }

    private static void parseCFunction(String line, LinkedList<Integer> updatedMethods,
            HashMap<Integer, LinkedList<MethodImplementation>> readMethodImpls, HashMap<Integer, LinkedList<String>> readMethodSignatures,
            MethodResourceDescription currConstraints, CImplementation implementation) {

        StringBuilder implementedTaskSignatureBuffer = new StringBuilder();
        StringBuilder implementationSignatureBuffer = new StringBuilder();
        line = line.replaceAll("[(|)|,|;|\n|\t]", " ");
        String[] splits = line.split("\\s+");
        // String returnType = splits[0];
        CImplementation task = loadCImplementation(splits[1]);
        String methodName = task.getMethodName();
        String declaringClass = task.getClassName();
        if (implementation != null) {
            implementedTaskSignatureBuffer.append(implementation.getMethodName()).append("(");
        } else {
            implementedTaskSignatureBuffer.append(methodName).append("(");
        }
        implementationSignatureBuffer.append(methodName).append("(");
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
        // Adds a new Signature-Id if not exists in the TreeMap
        Integer methodId = CoreManager.registerCoreId(taskSignature);
        logger.debug("CoreId for task" + taskSignature +" is " +methodId);
        updatedMethods.add(methodId);
        LinkedList<MethodImplementation> impls = readMethodImpls.get(methodId);
        LinkedList<String> signs = readMethodSignatures.get(methodId);
        if (impls == null) {
            impls = new LinkedList<MethodImplementation>();
            signs = new LinkedList<String>();
            logger.debug("[IDL Parser] Creating the implementation list for CE id " + methodId);
            readMethodImpls.put(methodId, impls);
            readMethodSignatures.put(methodId, signs);
        }
        MethodImplementation m = new MethodImplementation(declaringClass, methodName, methodId, impls.size(), currConstraints);
        logger.debug("[IDL Parser] Adding implementation: " + declaringClass + "." + methodName + " for CE id " + methodId);
        impls.add(m);
        signs.add(implementationSignature);
    }

    private static MethodResourceDescription loadCConstraints(String line) {
        line = line.substring(CONSTRAINT_IDL.length() + 1);
        String proc = new String();


        if (line.matches(".*" + PROCESSOR_IDL + ".*")){
                int procStart = line.indexOf("{");
                int procEnd = line.indexOf("}");
                proc = line.substring(procStart, procEnd+1);
                line = line.replace(proc, "");
                line = line.replace("processors=", "");
                proc = proc.replaceAll("[{}]", "");
                logger.debug("[IDL Parser] Loading processors: " + proc);
                line = line.replaceFirst(",", "");
        }
        
        
        line = line.replaceAll("[() ;\n\t]", "");
        String[] constraints = line.split(",");

        MethodResourceDescription mrd = new MethodResourceDescription(constraints, proc);
        // logger.debug("New Constraints detected: " + mrd);
        return mrd;
    }

    
    private static class CImplementation {
    
        private String className;
        private String methodName;
    
    
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
