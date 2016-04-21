package integratedtoolkit.loader.total;

import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.log4j.Logger;

import javassist.CannotCompileException;
//import javassist.CtClass;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.ExprEditor;
import javassist.expr.FieldAccess;
import javassist.expr.MethodCall;
import javassist.expr.NewExpr;
import integratedtoolkit.loader.LoaderUtils;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Service;


public class ITAppEditor extends ExprEditor {

    private Method[] remoteMethods;
    private CtMethod[] instrCandidates; // methods that will be instrumented if they are not remote 
    private String itApiVar;
    private String itExeVar;
    private String itSRVar;
    private String itORVar;
    private String itAppIdVar;
	private CtClass appClass;
    private static final String aawClassName = ArrayAccessWatcher.class.getCanonicalName();
    private static final Logger logger = Logger.getLogger(Loggers.LOADER);
    private static final boolean debug = logger.isDebugEnabled();

    
    public ITAppEditor(Method[] remoteMethods, CtMethod[] instrCandidates, String itApiVar, String itExeVar, String itSRVar, String itORVar, String itAppIdVar, CtClass appClass) {
        super();
        this.remoteMethods = remoteMethods;
        this.instrCandidates = instrCandidates;
        this.itApiVar = itApiVar;
        this.itExeVar = itExeVar;
        this.itSRVar = itSRVar;
        this.itORVar = itORVar;
        this.itAppIdVar = itAppIdVar;
        this.appClass = appClass;
    }


    // Instrument the creation of streams and stream wrappers
    public void edit(NewExpr ne) throws CannotCompileException {
        String fullName = ne.getClassName();
        boolean isInternal = fullName.startsWith("integratedtoolkit."),
                isIO = fullName.startsWith("java.io.");

        if (!isInternal) {
            StringBuilder modifiedExpr = new StringBuilder(),
                    callPars = new StringBuilder(),
                    toSerialize = new StringBuilder();
            try {
                CtClass[] paramTypes = ne.getConstructor().getParameterTypes();
                if (paramTypes.length > 0) {
                    int i = 1;
                    for (CtClass parType : paramTypes) {
                        if (i > 1) {
                            callPars.append(',');
                        }
                        String parId = "$" + (i++);
                        if (parType.isPrimitive()) {
                            callPars.append(parId);
                        } else { // Object (also array)
                            if (debug) {
                                logger.debug("Parameter " + (i - 1) + " of constructor " + ne.getConstructor() + " is an object, adding access");
                            }

                            String internalObject = itORVar + ".getInternalObject(" + parId + ')';
                            modifiedExpr.insert(0, itORVar + ".newObjectAccess(" + parId + ");");
                            callPars.append(internalObject).append(" == null ? ").append(parId).append(" : ").append("(" + parType.getName() + ")").append(internalObject);
                            toSerialize.append(itORVar).append(".serializeLocally(").append(parId).append(");");
                        }
                    }
                }
            } catch (NotFoundException e) {
                throw new CannotCompileException(e);
            }

            if (isIO) {
                String className = fullName.substring(8);

                if (debug) {
                    logger.debug("Inspecting the creation of an object of class " + className);
                }

                // $$ = pars separated by commas, $args = pars in an array of objects
                if (className.equals("FileInputStream")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newFileInputStream(" + callPars + ");");
                } else if (className.equals("FileOutputStream")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newFileOutputStream(" + callPars + ");");
                } else if (className.equals("InputStreamReader")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newInputStreamReader(" + callPars + ");");
                } else if (className.equals("BufferedReader")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newBufferedReader(" + callPars + ");");
                } else if (className.equals("FileWriter")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newFileWriter(" + callPars + ");");
                } else if (className.equals("PrintWriter")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newPrintWriter(" + callPars + ");");
                } else if (className.equals("FileReader")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newFileReader(" + callPars + ");");
                } else if (className.equals("OutputStreamWriter")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newOutputStreamWriter(" + callPars + ");");
                } else if (className.equals("BufferedWriter")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newBufferedWriter(" + callPars + ");");
                } else if (className.equals("PrintStream")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newPrintStream(" + callPars + ");");
                } else if (className.equals("RandomAccessFile")) {
                    modifiedExpr.append("$_ = " + itSRVar + ".newRandomAccessFile(" + callPars + ");");
                } else {
                    String internalObject = itORVar + ".getInternalObject($1)";
                    String par1 = internalObject + " == null ? (Object)$1 : " + internalObject;
                    modifiedExpr.append("$_ = $proceed(" + callPars + "); "
                            + "if ($_ instanceof java.io.FilterInputStream || $_ instanceof java.io.FilterOutputStream) {"
                            + itSRVar + ".newFilterStream(" + par1 + ", (Object)$_); }");
                }
            } else {
                modifiedExpr.append("$_ = $proceed(").append(callPars).append(");");
                modifiedExpr.append(toSerialize);
            }

            if (debug) {
                logger.debug("Replacing regular constructor call of class " + fullName + " by " + modifiedExpr.toString());
            }

            ne.replace(modifiedExpr.toString());
        }
    }

    // Replace calls to remote methods by calls to executeTask or black-boxes methods
    public void edit(MethodCall mc) throws CannotCompileException {
        Method declaredMethod = null;
        CtMethod calledMethod = null;
        try {
            calledMethod = mc.getMethod();
            declaredMethod = LoaderUtils.checkRemote(calledMethod, remoteMethods);
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }

        if (declaredMethod != null) { // Current method must be executed remotely, change the cal
        	String methodName = mc.getMethodName();

            if (debug) {
                logger.debug("Found call to remote method " + methodName);
            }

            Class<?> retType = declaredMethod.getReturnType();
            boolean isVoid = retType.equals(void.class);
            boolean isStatic = Modifier.isStatic(calledMethod.getModifiers());
            boolean isMethod = declaredMethod.isAnnotationPresent(integratedtoolkit.types.annotations.Method.class);
            Class<?> paramTypes[] = declaredMethod.getParameterTypes();
            int numParams = paramTypes.length;
            if (!isStatic) {
                numParams++;
            }
            if (!isVoid) {
                numParams++;
            }
            String methodClass = mc.getClassName();
            Annotation[][] paramAnnot = declaredMethod.getParameterAnnotations();

            //	Build the executeTask call string
            StringBuilder executeTask = new StringBuilder();
            executeTask.append(itExeVar).append(".executeTask(");

            executeTask.append(itAppIdVar).append(',');

            if (isMethod) {
                integratedtoolkit.types.annotations.Method methodAnnot = declaredMethod.getAnnotation(integratedtoolkit.types.annotations.Method.class);
                executeTask.append("\"").append(methodClass).append("\"").append(',');
                executeTask.append("\"").append(methodName).append("\"").append(',');
                executeTask.append(methodAnnot.priority()).append(',');
            } else { // Service
                Service serviceAnnot = declaredMethod.getAnnotation(Service.class);
                executeTask.append("\"").append(serviceAnnot.namespace()).append("\"").append(',');
                executeTask.append("\"").append(serviceAnnot.name()).append("\"").append(',');
                executeTask.append("\"").append(serviceAnnot.port()).append("\"").append(',');
                executeTask.append("\"").append(methodName).append("\"").append(',');
                executeTask.append(serviceAnnot.priority()).append(',');
            }

            executeTask.append(!isStatic).append(',');
            executeTask.append(numParams);

            if (numParams == 0) {
                executeTask.append(",null);");
            } else {
                executeTask.append(",new Object[]{");
                
                // Add the actual parameters of the method
                for (int i = 0; i < paramAnnot.length; i++) {
                    String type = null, direction = null;
                    Class<?> formalType = paramTypes[i];
                    Parameter.Type annotType = ((Parameter) paramAnnot[i][0]).type();

                    /* Append the value of the current parameter according to the type.
                     * Basic types must be wrapped by an object first
                     */
                    if (annotType.equals(Parameter.Type.FILE)) {
                        // The File type needs to be specified explicitly, since its formal type is String
                        type = "ITExecution.ParamType.FILE_T";
                        executeTask.append('$').append(i + 1).append(',');
                        executeTask.insert(0, itSRVar + ".addTaskFile($"+(i+1)+");");
                    } else if (annotType.equals(Parameter.Type.STRING)) {
                        /* Mechanism to make a String be treated like a list of chars instead of like another object.
                         * Dependencies won't be watched for the string.
                         */
                        type = "ITExecution.ParamType.STRING_T";
                        executeTask.append('$').append(i + 1).append(',');
                    } else if (formalType.isPrimitive()) {
                        if (formalType.equals(boolean.class)) {
                            type = "ITExecution.ParamType.BOOLEAN_T";
                            executeTask.append("new Boolean(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(char.class)) {
                            type = "ITExecution.ParamType.CHAR_T";
                            executeTask.append("new Character(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(byte.class)) {
                            type = "ITExecution.ParamType.BYTE_T";
                            executeTask.append("new Byte(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(short.class)) {
                            type = "ITExecution.ParamType.SHORT_T";
                            executeTask.append("new Short(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(int.class)) {
                            type = "ITExecution.ParamType.INT_T";
                            executeTask.append("new Integer(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(long.class)) {
                            type = "ITExecution.ParamType.LONG_T";
                            executeTask.append("new Long(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(float.class)) {
                            type = "ITExecution.ParamType.FLOAT_T";
                            executeTask.append("new Float(").append("$").append(i + 1).append("),");
                        } else if (formalType.equals(double.class)) {
                            type = "ITExecution.ParamType.DOUBLE_T";
                            executeTask.append("new Double(").append("$").append(i + 1).append("),");
                        }
                    } else { // Object or Self-Contained Object or Persistent SCO
                    	type = "LoaderUtils.checkSCOType($" + (i + 1) + ")";
                		executeTask.append("LoaderUtils.checkSCOPersistent($").append(i + 1).append("),");
                    }

                    switch (((Parameter) paramAnnot[i][0]).direction()) {
                        case IN:
                            direction = "ITExecution.ParamDirection.IN";
                            break;
                        case OUT:
                            direction = "ITExecution.ParamDirection.OUT";
                            break;
                        case INOUT:
                            direction = "ITExecution.ParamDirection.INOUT";
                            break;
                        default: // null
                            direction = "ITExecution.ParamDirection.IN";
                            break;
                    }

                    // Append the type and the direction of the current parameter
                    executeTask.append(type).append(",");
                    executeTask.append(direction);
                    if (i < paramAnnot.length - 1) {
                        executeTask.append(",");
                    }
                }

                // Add the target object of the call as an IN/INOUT parameter, for class methods
                if (!isStatic) {
                    // Assuming object, it is unlikely that a user selects a method invoked on an array
                    if ((isVoid ? numParams : numParams - 1) > 1) {
                        executeTask.append(',');
                    }
                    executeTask.append("LoaderUtils.checkSCOPersistent($0),").append("LoaderUtils.checkSCOType($0)");
                    // Check if the method will modify the target object (default yes)
                    if (isMethod) {
                        integratedtoolkit.types.annotations.Method methodAnnot = declaredMethod.getAnnotation(integratedtoolkit.types.annotations.Method.class);
                        if (methodAnnot.isModifier()) {
                            executeTask.append(',').append("ITExecution.ParamDirection.INOUT");
                        } else {
                            executeTask.append(',').append("ITExecution.ParamDirection.IN");
                        }
                    } else // Service
                    {
                        executeTask.append(',').append("ITExecution.ParamDirection.INOUT");
                    }
                }

                // Add the return value as an OUT parameter, if any
                StringBuilder afterExecute = new StringBuilder("");
                if (!isVoid) {
                    if (numParams > 1) {
                        executeTask.append(',');
                    }

                    String typeName = retType.getName();
                    if (retType.isPrimitive()) {
                        String tempRetVar = "ret" + System.nanoTime();
                        executeTask.append(tempRetVar).append(',').append("ITExecution.ParamType.OBJECT_T")
                                .append(',').append("ITExecution.ParamDirection.OUT");

                        String retValueCreation = "Object " + tempRetVar + " = ",
                                cast, converterMethod;
                        if (typeName.equals(boolean.class.getName())) {
                            retValueCreation += "new Boolean(false);";
                            cast = "(Boolean)";
                            converterMethod = "booleanValue()";
                        } else if (typeName.equals(char.class.getName())) {
                            retValueCreation += "new Character(Character.MIN_VALUE);";
                            cast = "(Character)";
                            converterMethod = "charValue()";
                        } else if (typeName.equals(byte.class.getName())) {
                            retValueCreation += "new Byte(Byte.MIN_VALUE);";
                            cast = "(Byte)";
                            converterMethod = "byteValue()";
                        } else if (typeName.equals(short.class.getName())) {
                            retValueCreation += "new Short(Short.MIN_VALUE);";
                            cast = "(Short)";
                            converterMethod = "shortValue()";
                        } else if (typeName.equals(int.class.getName())) {
                            retValueCreation += "new Integer(Integer.MIN_VALUE);";
                            cast = "(Integer)";
                            converterMethod = "intValue()";
                        } else if (typeName.equals(long.class.getName())) {
                            retValueCreation += "new Long(Long.MIN_VALUE);";
                            cast = "(Long)";
                            converterMethod = "longValue()";
                        } else if (typeName.equals(float.class.getName())) {
                            retValueCreation += "new Float(Float.MIN_VALUE);";
                            cast = "(Float)";
                            converterMethod = "floatValue()";
                        } else { // (typeName.equals(double.class.getName()))
                            retValueCreation += "new Double(Double.MIN_VALUE);";
                            cast = "(Double)";
                            converterMethod = "doubleValue()";
                        }

                        // Before executeTask, declare and instance a temp wrapper object containing the primitive value
                        executeTask.insert(0, retValueCreation);

                        /* After execute task, register an access to the wrapper object,
                         * get its (remotely) generated value and
                         * assign it to the application's primitive type var
                         */
                        afterExecute.append(itORVar).append(".newObjectAccess(").append(tempRetVar).append(");");
                        afterExecute.append("$_ = (")
                                .append(cast).append(itORVar).append(".getInternalObject(").append(tempRetVar).append(")).")
                                .append(converterMethod).append(";");
                    } else if (retType.isArray()) {
                        //throw new CannotCompileException("Return type (array) for method " + declaredMethod + " not supported");
                        Class<?> compType = retType.getComponentType();
                        int numDim = typeName.lastIndexOf('[');
                        String dims = "[0]";
                        while (numDim-- > 0) {
                            dims += "[]";
                        }
                        while (compType.getComponentType() != null) {
                            compType = compType.getComponentType();
                        }
                        String compTypeName = compType.getName();
                        executeTask.insert(0, "$_ = new " + compTypeName + dims + ';');
                        executeTask.append("$_,").append("ITExecution.ParamType.OBJECT_T");
                        executeTask.append(',').append("ITExecution.ParamDirection.OUT");
                    } else { // Object
                        // Wrapper for a primitive type: return a default value
                        if (typeName.equals(Boolean.class.getName())) {
                            executeTask.insert(0, "$_ = new Boolean(false);");
                        } else if (typeName.equals(Character.class.getName())) {
                            executeTask.insert(0, "$_ = new Character(Character.MIN_VALUE);");
                        } else if (typeName.equals(Byte.class.getName())) {
                            executeTask.insert(0, "$_ = new Byte(Byte.MIN_VALUE);");
                        } else if (typeName.equals(Short.class.getName())) {
                            executeTask.insert(0, "$_ = new Short(Short.MIN_VALUE);");
                        } else if (typeName.equals(Integer.class.getName())) {
                            executeTask.insert(0, "$_ = new Integer(Integer.MIN_VALUE);");
                        } else if (typeName.equals(Long.class.getName())) {
                            executeTask.insert(0, "$_ = new Long(Long.MIN_VALUE);");
                        } else if (typeName.equals(Float.class.getName())) {
                            executeTask.insert(0, "$_ = new Float(Float.MIN_VALUE);");
                        } else if (typeName.equals(Double.class.getName())) {
                            executeTask.insert(0, "$_ = new Double(Double.MIN_VALUE);");
                        } // Object (maybe String): use the no-args constructor
                        else {
                            executeTask.insert(0, "$_ = new " + typeName + "();");
                        }

                        executeTask.append("$_,").append("LoaderUtils.checkSCOType($_)");
                        executeTask.append(',').append("ITExecution.ParamDirection.OUT"); 
                    }
                }

                executeTask.append("});");
                executeTask.append(afterExecute);
            }

            if (debug) {
                logger.debug("Replacing local method call by: " + executeTask.toString());
            }

            // Replace the call to the method by the call to executeTask
            mc.replace(executeTask.toString());
        } else if (LoaderUtils.isStreamClose(mc)) {
            if (debug) {
                logger.debug("Replacing close on a stream of class " + mc.getClassName());
            }

            /* Close call on a stream
             * No need to instrument the stream object, assuming it will always be local
             */
            mc.replace("$_ = $proceed($$); " + itSRVar + ".streamClosed($0);");
        } else if (LoaderUtils.isFileDelete(mc)) {
            mc.replace("$_ = " + itApiVar + ".deleteFile($0.getCanonicalPath());");
        } else {
            /* The method is not remote
             * Check if it is an instrumented method or a black-box method
             */
            //boolean isBlackBox = !calledMethod.getDeclaringClass().getPackageName().startsWith("integratedtoolkit") && !LoaderUtils.contains(instrCandidates, calledMethod);
            
        	boolean isBlackBox = !LoaderUtils.contains(instrCandidates, calledMethod);
            if (isBlackBox) {
                if (debug) {
                    logger.debug("Inspecting method call to black-box method " + mc.getMethodName() + ", looking for objects");
                }

                StringBuilder modifiedCall = new StringBuilder(),
                        toSerialize = new StringBuilder();

                // Check if the black-box we're going to is one of the array watch methods
                boolean isArrayWatch = calledMethod.getDeclaringClass().getName().equals(aawClassName);

                // First check the target object
                modifiedCall.append(itORVar).append(".newObjectAccess($0);");
                toSerialize.append(itORVar).append(".serializeLocally($0);");

                /* Now add the call.
                 * If the target object of the call is a task object,
                 * invoke the method on the internal object stored by the runtime.
                 * Also check the parameters.
                 * We need to control the parameters of non-remote and non-instrumented methods (black box),
                 * since they represent the border to the code where we can't intercept anything.
                 * If any of these parameters is an object we kept track of, synchronize
                 */
                String redirectedCallPars = null;
                //	   normalCallPars 	  = "";
                try {
                    CtClass[] paramTypes = mc.getMethod().getParameterTypes();
                    if (paramTypes.length > 0) {
                        int i = 1;
                        StringBuilder aux1 = new StringBuilder("new Object[] {");
                        //aux2 = new StringBuilder();
                        for (CtClass parType : paramTypes) {
                            if (i > 1) {
                                aux1.append(','); /*aux2.append(',');*/ }
                            String parId = "$" + i;
                            if (parType.isPrimitive()) {
                                if (parType.equals(CtClass.booleanType)) {
                                    aux1.append("new Boolean(").append(parId).append(')');
                                } else if (parType.equals(CtClass.charType)) {
                                    aux1.append("new Character(").append(parId).append(')');
                                } else if (parType.equals(CtClass.byteType)) {
                                    aux1.append("new Byte(").append(parId).append(')');
                                } else if (parType.equals(CtClass.shortType)) {
                                    aux1.append("new Short(").append(parId).append(')');
                                } else if (parType.equals(CtClass.intType)) {
                                    aux1.append("new Integer(").append(parId).append(')');
                                } else if (parType.equals(CtClass.longType)) {
                                    aux1.append("new Long(").append(parId).append(')');
                                } else if (parType.equals(CtClass.floatType)) {
                                    aux1.append("new Float(").append(parId).append(')');
                                } else //if (parType.equals(CtClass.doubleType))
                                {
                                    aux1.append("new Double(").append(parId).append(')');
                                }
                                //aux2.append(parId);
                            } else if(parType.getName().equals(String.class.getName())){ //This is a string
                            	
                            	if (debug) {
                                    logger.debug("Parameter " + i + " of black-box method " + mc.getMethodName() + " is an String, adding File/object access");
                                }
                            	if (isArrayWatch && i == 3) {
                                    // Prevent from synchronizing task return objects to be stored in an array position
                                    aux1.append(parId);
                                } else {
                                	String calledClass = mc.getClassName();
                                    if (calledClass.equals(PrintStream.class.getName())||calledClass.equals(StringBuilder.class.getName())){
                                    	// If the call is inside a PrintStream or StringBuilder, only synchronize objects files already has the name
                                    	String internalObject = itORVar + ".getInternalObject(" + parId + ')';
                                        modifiedCall.insert(0, itORVar + ".newObjectAccess(" + parId + ");");
                                        //aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append(internalObject);
                                        aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append("(" + parType.getName() + ")").append(internalObject);
                                        toSerialize.append(itORVar).append(".serializeLocally(").append(parId).append(");");
                                	}else{
                                		String internalObject = itORVar + ".getInternalObject(" + parId + ')';
                                		String taskFile = itSRVar+".isTaskFile("+ parId +")";
                                		String apiOpenFile = itApiVar+".openFile("+ parId +", integratedtoolkit.api.IntegratedToolkit.OpenMode.APPEND)";
                                		modifiedCall.insert(0, itORVar + ".newObjectAccess(" + parId + ");");
                                		//aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append(internalObject);
                                		//Adding check of task files
                                		aux1.append(taskFile).append(" ? ").append(apiOpenFile).append(" : ").append(internalObject).append(" == null ? ").append(parId).append(" : ").append("(" + parType.getName() + ")").append(internalObject);
                                		//OLD: aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append("(" + parType.getName() + ")").append(internalObject);
                                		toSerialize.append(itORVar).append(".serializeLocally(").append(parId).append(");");
                                	}
                                }
                            } else { // Object (also array)
                                if (debug) {
                                    logger.debug("Parameter " + i + " of black-box method " + mc.getMethodName() + " is an object, adding access");
                                }

                                if (isArrayWatch && i == 3) {
                                    // Prevent from synchronizing task return objects to be stored in an array position
                                    aux1.append(parId);
                                } else {
                                    String internalObject = itORVar + ".getInternalObject(" + parId + ')';
                                    modifiedCall.insert(0, itORVar + ".newObjectAccess(" + parId + ");");
                                    //aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append(internalObject);
                                    aux1.append(internalObject).append(" == null ? ").append(parId).append(" : ").append("(" + parType.getName() + ")").append(internalObject);
                                    toSerialize.append(itORVar).append(".serializeLocally(").append(parId).append(");");
                                }
                            }
                            i++;
                        }
                        aux1.append('}');
                        redirectedCallPars = aux1.toString();
                        //normalCallPars = aux2.toString();
                    }
                } catch (NotFoundException e) {
                    throw new CannotCompileException(e);
                }
                String internalObject = itORVar + ".getInternalObject($0)";
                modifiedCall.append("if (").append(internalObject).append(" != null) {")
                        .append("$_ = ($r)LoaderUtils.runMethodOnObject(").append(internalObject).append(",$class,\"").append(mc.getMethodName()).append("\",").append(redirectedCallPars).append(",$sig);")
                        //.append("$_ = ($r)").append(appName).append(".runMethodOnObjectIT(").append(internalObject).append(",$class,\"").append(mc.getMethodName()).append("\",").append(redirectedCallPars).append(",$sig);")
                        // This should work but it crashes because of a Javassist bug
                        //.append("$_ = ((").append(calledMethod.getDeclaringClass().getName()).append(')').append(internalObject).append(").").append(mc.getMethodName()).append('(').append(normalCallPars).append(");")
                        //.append("$_ = ").append(calledMethod.getDeclaringClass().getName()).append(".class.cast(").append(internalObject).append(").").append(mc.getMethodName()).append('(').append(normalCallPars).append(");")
                        //.append("$_ = $class.cast(").append(internalObject).append(").").append(mc.getMethodName()).append('(').append(normalCallPars).append(");")
                        //.append("$_ = ((").append(calledMethod.getDeclaringClass().getName()).append(")$class.cast(").append(internalObject).append(")).").append(mc.getMethodName()).append('(').append(normalCallPars).append(");")
                        .append("}else { $_ = ($r)LoaderUtils.runMethodOnObject($0,$class,\"").append(mc.getMethodName()).append("\",").append(redirectedCallPars).append(",$sig); }");
	            			//.append("}else { $_ = ($r)").append(appName).append(".runMethodOnObjectIT($0,$class,\"").append(mc.getMethodName()).append("\",").append(redirectedCallPars).append(",$sig); }");
                // This should work but it crashes because of a Javassist bug
                //.append("}else { $_ = $proceed(").append(normalCallPars).append("); }");
                //.append("}else { $_ = $proceed($$); }");

                // Serialize the (internal) objects locally after the call
                modifiedCall.append(toSerialize);

                if (debug) {
                    logger.debug("Replacing regular method call by " + modifiedCall.toString());
                }

                mc.replace(modifiedCall.toString());
            }
        }
    }

	/* 
	 * Check the access to fields of objects
     */
    public void edit(FieldAccess fa) throws CannotCompileException {
        CtField field = null;
        try {
            field = fa.getField();
            if (Modifier.isStatic(field.getModifiers())) {
                return;
            }
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }
        String fieldName = field.getName();

        if (debug) {
            logger.debug("Keeping track of access to field " + fieldName + " of class " + field.getDeclaringClass().getName());
        }

        boolean isWriter = fa.isWriter();

        // First check the object containing the field
        StringBuilder toInclude = new StringBuilder();
        toInclude.append(itORVar).append(".newObjectAccess($0,").append(isWriter).append(");");

        // Execute the access on the internal object
        String internalObject = itORVar + ".getInternalObject($0)";
        String objectClass = fa.getClassName();
        toInclude.append("if (").append(internalObject).append(" != null) {");
        if (isWriter) {
            toInclude.append("((").append(objectClass).append(')').append(internalObject).append(").").append(fieldName).append(" = $1;"); // store a new value in the field
            toInclude.append("} else { $_ = $proceed($$); }");
            // Serialize the (internal) object locally after the access
            toInclude.append(itORVar).append(".serializeLocally($0);");
        } else {
            toInclude.append("$_ = ((").append(objectClass).append(')').append(internalObject).append(").").append(fieldName).append(';'); // read the field value
            toInclude.append("} else { $_ = $proceed($$); }");
        }

        fa.replace(toInclude.toString());
        
        if (debug) {
            logger.debug("Replaced regular field access by " + toInclude.toString());
        }
    }
    

}
