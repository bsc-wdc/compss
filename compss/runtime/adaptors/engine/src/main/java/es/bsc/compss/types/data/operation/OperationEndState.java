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
package es.bsc.compss.types.data.operation;

/**
 * End state of any data operation.
 */
public enum OperationEndState {
    OP_OK, // Success
    OP_IN_PROGRESS, // In progress
    OP_FAILED, // Failed
    OP_PREPARATION_FAILED, // Preparation failed
    OP_WAITING_SOURCES; // Waiting for resources
}
