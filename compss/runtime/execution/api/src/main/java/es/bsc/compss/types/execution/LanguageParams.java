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
package es.bsc.compss.types.execution;

/**
 * Marker interface for language-specific parameter bundles. Each programming language declares its own refinement (for
 * example C, Java, Python and R parameters) carrying the configuration that its invokers need. Bundles are retrieved
 * from the invocation context through {@code InvocationContext.getLanguageParams(Lang)}.
 */
public interface LanguageParams {

}
