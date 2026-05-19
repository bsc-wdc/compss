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

#ifndef TRACING_INTERFACE_H
#define TRACING_INTERFACE_H

typedef struct TracingInterface TracingInterface;

struct TracingInterface {

    void (*StartSynchronization)(
        long value
    );

    void (*EndSynchronization)(
        void
    );

    void (*ActiveComponent)(
        int id,
        char* description
    );

    void (*InactiveComponent)(
        void
    );

    void (*DefineNewEventType)(
        int code,
        char* description,
        int endable,
        int numEvents,
        int* eventIDs,
        char** eventLabels
    );

    void (*EmitEvent)(
        int type,
        long id
    );

};

#endif // TRACING_INTERFACE_H
