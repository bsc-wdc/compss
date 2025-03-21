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
package es.bsc.compss.gat.worker;

import es.bsc.compss.log.Loggers;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.AppenderRefComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;


public class GATLog {

    private static final String APPENDER_NAME = "Stdout";
    private static final String PATTERN = "[(%r)(%d) %19c{1}]    @%-15.15M  -  %m%n";


    /**
     * Initializes the GAT Logger with the given level.
     * 
     * @param debug Whether the debug level is enabled or not.
     */
    public static void init(boolean debug) {
        final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationFactory.newConfigurationBuilder();

        // Creating Appender
        final AppenderComponentBuilder appenderBuilder = builder.newAppender(APPENDER_NAME, "CONSOLE");
        appenderBuilder.addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        final LayoutComponentBuilder layoutBuilder = builder.newLayout("PatternLayout");
        layoutBuilder.addAttribute("pattern", PATTERN);
        appenderBuilder.add(layoutBuilder);
        builder.add(appenderBuilder);

        // ADD ROOT LEVEL
        final Level level = debug ? Level.DEBUG : Level.OFF;
        final AppenderRefComponentBuilder appender = builder.newAppenderRef(APPENDER_NAME);
        final RootLoggerComponentBuilder rootLogger = builder.newRootLogger(level);
        rootLogger.add(appender);
        builder.add(rootLogger);

        // ADD LOGGERS
        addLogger(Loggers.WORKER, level, appender, builder);
        addLogger(Loggers.WORKER_INVOKER, level, appender, builder);

        final Configuration conf = builder.build();
        Configurator.initialize(conf);
    }

    private static void addLogger(String name, Level level, AppenderRefComponentBuilder appender,
        ConfigurationBuilder<?> builder) {

        final LoggerComponentBuilder logger = builder.newLogger(name, level);
        logger.add(appender);
        logger.addAttribute("additivity", false);
        builder.add(logger);
    }
}
