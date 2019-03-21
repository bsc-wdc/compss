/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.maven.instrumentation;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.xml.sax.SAXException;

import es.bsc.servicess.ide.CommonUtils;
import es.bsc.servicess.ide.ProjectMetadata;
import es.bsc.servicess.ide.model.ElementLabelUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;


/**
 * Goal which instruments the application.
 *
 * @goal instrument
 * 
 * @phase process-classes
 * 
 * @requiresDependencyResolution compile+runtime
 */

public class InstrumentMojo extends AbstractMojo {

    /**
     * Location of the instrumented classes.
     * 
     * @parameter expression="${project.build.directory}"
     * @required
     */
    private File outputDirectory;

    /**
     * @parameter expression="${project}" defaultValue = "${project}"
     * @required
     * @readOnly
     */
    private MavenProject project;

    /**
     * Location of the metadata file.
     * 
     * @parameter expression="${project.basedir}/META-INF/metadata.xml"
     */
    private File metadataFile;

    /**
     * Location of the compss libraries.
     * 
     * @parameter defaultValue="/opt/COMPSs/"
     * 
     */
    private String compssLocation;

    /**
     * @parameter
     */
    private String mainClass;

    /**
     * @parameter
     */
    private List<String> webServiceClasses;

    /**
     * @parameter
     */
    private String[] orchestrations;


    public void execute() throws MojoExecutionException {
        try {
            getLog().info("COMPSs code instrumentation...");
            if (outputDirectory != null && outputDirectory.exists() && project != null) {
                String classesFolder = outputDirectory.getAbsolutePath().concat(File.separator + "classes");
                getLog().debug("Classes folder " + classesFolder);
                List<String> ccp = project.getCompileClasspathElements();
                List<String> cp = project.getRuntimeClasspathElements();
                cp.addAll(ccp);

                if (metadataFile != null && metadataFile.exists()) {
                    getLog().info("Getting values from metadata file...");

                    ProjectMetadata prMeta = new ProjectMetadata(metadataFile);
                    String[] extClasses = prMeta.getExternalOrchestrationClasses();
                    compssLocation = prMeta.getRuntimeLocation();
                    for (String cl : extClasses) {
                        getLog().warn("External classses not supported yet, skipping cl");
                        // extractPackages
                        // String classesFolder = addOrchestration(cp, cl,
                        // prMeta.getOrchestrationElementFormExternalClass(cl));
                        // instrument(cp, classesFolder, cl);
                    }
                    String[] intClasses = prMeta.getNonExternalOrchestrationClasses();
                    CommonUtils.instrumentOrchestrations(compssLocation, intClasses, classesFolder, cp,
                            prMeta.getOrchestrationClassesTypes(intClasses), mainClass);
                } else {
                    getLog().info("Metadata does not exists.");
                }
                if (orchestrations != null && orchestrations.length > 0) {
                    getLog().info("Treating defined orchestrations ...");
                    Map<String, List<String>> orchClassAndElements = ElementLabelUtils.getClassesAndElements(orchestrations);
                    for (Entry<String, List<String>> entry : orchClassAndElements.entrySet()) {
                        CommonUtils.preInstrumentOrchestration(compssLocation, entry.getKey(), entry.getValue(), classesFolder, cp);
                        String cl = entry.getKey();
                        boolean isWs = webServiceClasses != null && webServiceClasses.contains(cl) ? true : false;
                        boolean isMainClass = mainClass != null && cl.equals(mainClass) ? true : false;
                        CommonUtils.instrumentOrchestration(compssLocation, cl, classesFolder, cp, isWs, isMainClass);
                    }
                } else {
                    getLog().info("There are not defined orchestrations.");
                }
            } else {
                getLog().error("Output dir " + outputDirectory.getAbsolutePath() + " or maven project does not exist");
                throw new MojoExecutionException("Output dir or maven project does not exist");
            }

        } catch (DependencyResolutionRequiredException e) {
            getLog().error("Exception getting project classpath");
            getLog().error(e);
            throw new MojoExecutionException("Exception getting project classpath", e);
        } catch (SAXException e) {
            getLog().error("Exception parsing metadata file");
            getLog().error(e);
            throw new MojoExecutionException("Exception parsing metadata file", e);
        } catch (IOException e) {
            getLog().error("Exception parsing metadata file");
            getLog().error(e);
            throw new MojoExecutionException("Exception parsing metadata file", e);
        } catch (ParserConfigurationException e) {
            getLog().error("Exception parsing metadata file");
            getLog().error(e);
            throw new MojoExecutionException("Exception parsing metadata file", e);
        } catch (Exception e) {
            getLog().error("Exception getting orchestration Classes");
            getLog().error(e);
            throw new MojoExecutionException("Exception getting orchestration Classes", e);
        }

    }
}
