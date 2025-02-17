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
package es.bsc.compss.loader;

import es.bsc.compss.loader.exceptions.NameNotFoundException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AddOrchestration {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);


    /**
     * TODO javadoc.
     *
     * @param args description
     * @throws NotFoundException description
     * @throws NameNotFoundException description
     * @throws CannotCompileException description
     * @throws IOException description
     */
    public static void main(String[] args)
        throws NotFoundException, NameNotFoundException, CannotCompileException, IOException {
        if (args.length < 2) {
            ErrorManager.fatal("Error: missing arguments for loader");
        }
        String className = args[0];
        String classPackage = getPackage(className);

        // Pool creation
        ClassPool pool = ClassPool.getDefault();
        if (classPackage != null && classPackage.trim().length() > 0) {
            pool.importPackage(classPackage);
        }

        // Extracting the class
        CtClass cc = pool.getCtClass(className);
        ClassFile ccFile = cc.getClassFile();
        ConstPool constpool = ccFile.getConstPool();
        for (int i = 1; i < args.length; i++) {
            String methodLabel = args[i];
            String methodName = getMethodName(methodLabel);
            CtClass[] params = getParamClasses(methodLabel, pool);
            CtMethod methodDescriptor = cc.getDeclaredMethod(methodName, params);
            AnnotationsAttribute attr =
                (AnnotationsAttribute) methodDescriptor.getMethodInfo().getAttribute(AnnotationsAttribute.visibleTag);
            if (attr == null) {
                // Create the annotation
                attr = new AnnotationsAttribute(constpool, AnnotationsAttribute.visibleTag);
            }
            Annotation annot = new Annotation(LoaderConstants.CLASS_ANNOTATIONS_ORCHESTRATION, constpool);
            attr.addAnnotation(annot);
            methodDescriptor.getMethodInfo().addAttribute(attr);
        }
        cc.writeFile();
    }

    private static CtClass[] getParamClasses(String label, ClassPool pool)
        throws NotFoundException, NameNotFoundException {
        List<CtClass> classes = new LinkedList<>();
        List<String> params = getParametersTypeFromLabel(label);
        if (params != null && params.size() > 0) {
            for (String className : params) {
                String pack = getPackage(className);
                if (pack != null) {
                    pool.importPackage(pack);
                }
                classes.add(pool.getCtClass(className));
            }
            return classes.toArray(new CtClass[classes.size()]);
        } else {
            return new CtClass[0];
        }
    }

    /**
     * Parses the list of type parameters {@code label} and returns a list with the type of each parameter.
     * 
     * @param label String containing the parameters between brackets and separated by commas.
     * @return List of types of each parameter.
     * @throws NameNotFoundException When {@code label} is malformed.
     */
    public static List<String> getParametersTypeFromLabel(String label) throws NameNotFoundException {
        int begin = label.indexOf("(");
        int end = label.indexOf(")");
        if (begin > 0 && end > 0 && end > begin) {
            String parsString = label.substring(begin + 1, end);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Parameters: " + parsString);
            }
            List<String> parameters = new LinkedList<String>();
            if (parsString != null && parsString.trim().length() > 0) {
                String[] parametersArray = parsString.split(", ");
                if (parametersArray != null && parametersArray.length > 0) {
                    for (String parameter : parametersArray) {
                        parameters.add(parameter);

                    }
                }
            }
            return parameters;
        } else {
            throw new NameNotFoundException("Error incorrect label " + label);
        }
    }

    private static String getMethodName(String label) throws NameNotFoundException {
        int i = label.indexOf("(");
        if (i > 0) {
            return label.substring(0, i);
        } else {
            throw new NameNotFoundException("Error method name from label " + label);
        }
    }

    private static String getPackage(String className) throws NameNotFoundException {
        if (className != null && className.trim().length() > 0) {
            int i = className.lastIndexOf(".");
            if (i >= 0) {
                return className.substring(i + 1).trim();
            } else {
                return null;
            }
        } else {
            throw new NameNotFoundException("ClassName is null");
        }
    }

}
