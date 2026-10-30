/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.execution.invokers;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface InvokerFactory {

    class Registry {

        private static Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

        private static final InvokerFactory[][] factories = new InvokerFactory[MethodType.values().length][];

        static {
            discoverFactories();
        }


        private static void discoverFactories() {
            logger.debug("Discovering InvokerFactories loaded in classpath");
            List<InvokerFactory>[] lists = new List[MethodType.values().length];

            for (int i = 0; i < lists.length; i++) {
                lists[i] = new ArrayList<>();
            }

            for (InvokerFactory invFac : ServiceLoader.load(InvokerFactory.class)) {
                logger.debug("\tDiscovered InvokerFactories {} supporting types {}",
                    invFac.getClass().getCanonicalName(), invFac.getSupportedTypes());
                for (MethodType mt : invFac.getSupportedTypes()) {
                    List<InvokerFactory> mtFacts = lists[mt.ordinal()];
                    mtFacts.add(invFac);
                }
            }

            for (int i = 0; i < lists.length; i++) {
                factories[i] = lists[i].toArray(new InvokerFactory[0]);
            }
        }

        /**
         * Preloads all the necessary infrastructure to host executions of a method type and a language.
         * 
         * @param platform platform that will host the execution
         * @param mt Type of method to be supported
         * @param lang Language of the methods to be supported
         */
        public static void preLoad(ExecutorContext platform, MethodType mt, COMPSsConstants.Lang lang) {
            for (InvokerFactory fact : factories[mt.ordinal()]) {
                fact.preLoad(platform, mt, lang);
            }
        }

        /**
         * Constructs a new Invoker to run a specific task.
         *
         * @param context Invocation context
         * @param inv invocation to run
         * @param wDir sandbox where the task will be executed
         * @param resources resources allocated for the invoker to run the invocation
         * @return New invoker to run the invocation in the specific context
         * @throws JobExecutionException Could not create an invoker to run the invocation
         */
        public static Invoker<?> getInvoker(Executor executor, InvocationContext context, Invocation inv,
            ExecutionSandbox wDir, InvocationResources resources) throws JobExecutionException {
            MethodType mt = inv.getMethodImplementation().getMethodType();
            for (InvokerFactory factory : factories[mt.ordinal()]) {
                Invoker<?> invoker = factory.build(executor, context, inv, wDir, resources);
                if (invoker != null) {
                    return invoker;
                }
            }
            throw new JobExecutionException("Undefined invoker. It could be cause by an incoherent task type");
        }
    }


    MethodType[] getSupportedTypes();

    /**
     * Preloads all the necessary infrastructure to host executions of a method type with a language.
     * 
     * @param platform platform that will host the executions
     * @param mt method type to support
     * @param lang language to support
     */
    void preLoad(ExecutorContext platform, MethodType mt, COMPSsConstants.Lang lang);

    Invoker<?> build(Executor executor, InvocationContext context, Invocation inv, ExecutionSandbox wDir,
        InvocationResources resources) throws JobExecutionException;
}
