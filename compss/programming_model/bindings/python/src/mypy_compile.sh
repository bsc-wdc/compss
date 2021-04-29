#!/bin/bash

###################################
## Compilation with mypyc script ##
###################################

mypyc --scripts-are-modules ./pycompss/api/parameter.py
#mypyc --scripts-are-modules ./pycompss/api/local.py
#mypyc --scripts-are-modules ./pycompss/api/opencl.py
#mypyc --scripts-are-modules ./pycompss/api/on_failure.py
#mypyc --scripts-are-modules ./pycompss/api/reduction.py
#mypyc --scripts-are-modules ./pycompss/api/ompss.py
#mypyc --scripts-are-modules ./pycompss/api/container.py
#mypyc --scripts-are-modules ./pycompss/api/mpi.py
#mypyc --scripts-are-modules ./pycompss/api/compss.py
mypyc --scripts-are-modules ./pycompss/api/commons/data_type.py
#mypyc --scripts-are-modules ./pycompss/api/commons/decorator.py
mypyc --scripts-are-modules ./pycompss/api/commons/information.py
mypyc --scripts-are-modules ./pycompss/api/commons/error_msgs.py
#mypyc --scripts-are-modules ./pycompss/api/binary.py
#mypyc --scripts-are-modules ./pycompss/api/constraint.py
#mypyc --scripts-are-modules ./pycompss/api/task.py
#mypyc --scripts-are-modules ./pycompss/api/decaf.py
#mypyc --scripts-are-modules ./pycompss/api/IO.py
#mypyc --scripts-are-modules ./pycompss/api/api.py
mypyc --scripts-are-modules ./pycompss/api/dummy/on_failure.py
mypyc --scripts-are-modules ./pycompss/api/dummy/reduction.py
mypyc --scripts-are-modules ./pycompss/api/dummy/container.py
mypyc --scripts-are-modules ./pycompss/api/dummy/constraint.py
mypyc --scripts-are-modules ./pycompss/api/dummy/task.py
mypyc --scripts-are-modules ./pycompss/api/dummy/api.py
#mypyc --scripts-are-modules ./pycompss/api/implement.py
#mypyc --scripts-are-modules ./pycompss/api/exceptions.py
#mypyc --scripts-are-modules ./pycompss/api/multinode.py
#mypyc --scripts-are-modules ./pycompss/interactive.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/commons.py
#mypyc --scripts-are-modules ./pycompss/runtime/binding.py
#mypyc --scripts-are-modules ./pycompss/runtime/mpi/keys.py
#mypyc --scripts-are-modules ./pycompss/runtime/constants.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/COMPSs.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/classes.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/direction.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/synchronization.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/link.py
#mypyc --scripts-are-modules ./pycompss/runtime/management/object_tracker.py
#mypyc --scripts-are-modules ./pycompss/runtime/launch.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/parameter.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/keys.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/commons.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/master.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/worker.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/arguments.py
#mypyc --scripts-are-modules ./pycompss/runtime/task/core_element.py
#mypyc --scripts-are-modules ./pycompss/functions/data.py
mypyc --scripts-are-modules ./pycompss/functions/elapsed_time.py
mypyc --scripts-are-modules ./pycompss/functions/reduce.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/functions/profile.py
#mypyc --scripts-are-modules ./pycompss/util/storages/persistent.py
#mypyc --scripts-are-modules ./pycompss/util/logger/helpers.py
#mypyc --scripts-are-modules ./pycompss/util/objects/util.py
#mypyc --scripts-are-modules ./pycompss/util/objects/sizer.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/objects/properties.py
#mypyc --scripts-are-modules ./pycompss/util/objects/replace.py
#mypyc --scripts-are-modules ./pycompss/util/jvm/parser.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/state.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/helpers.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/outwatcher.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/utils.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/flags.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/graphs.py
#mypyc --scripts-are-modules ./pycompss/util/interactive/events.py
#mypyc --scripts-are-modules ./pycompss/util/environment/configuration.py
#mypyc --scripts-are-modules ./pycompss/util/mpi/helper.py
#mypyc --scripts-are-modules ./pycompss/util/serialization/serializer.py
#mypyc --scripts-are-modules ./pycompss/util/serialization/extended_support.py
#mypyc --scripts-are-modules ./pycompss/util/std/redirects.py
#mypyc --scripts-are-modules ./pycompss/util/supercomputer/scs.py
#mypyc --scripts-are-modules ./pycompss/util/tracing/helpers.py
mypyc --scripts-are-modules ./pycompss/util/arguments.py
mypyc --scripts-are-modules ./pycompss/util/context.py
mypyc --scripts-are-modules ./pycompss/util/exceptions.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/warnings/modules.py
#mypyc --scripts-are-modules ./pycompss/worker/container/container_worker.py
#mypyc --scripts-are-modules ./pycompss/worker/container/pythonpath_fixer.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/cache/tracker.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/cache/setup.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/piper_worker.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/commons/executor.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/commons/utils.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/commons/constants.py
#mypyc --scripts-are-modules ./pycompss/worker/piper/mpi_piper_worker.py
#mypyc --scripts-are-modules ./pycompss/worker/commons/executor.py
#mypyc --scripts-are-modules ./pycompss/worker/commons/worker.py
#mypyc --scripts-are-modules ./pycompss/worker/commons/constants.py
#mypyc --scripts-are-modules ./pycompss/worker/external/mpi_executor.py
#mypyc --scripts-are-modules ./pycompss/worker/gat/commons/constants.py
#mypyc --scripts-are-modules ./pycompss/worker/gat/worker.py
# Unsupported yet - dds
#mypyc --scripts-are-modules ./pycompss/dds/partition_generators.py
#mypyc --scripts-are-modules ./pycompss/dds/heapq3.py
#mypyc --scripts-are-modules ./pycompss/dds/tasks.py
#mypyc --scripts-are-modules ./pycompss/dds/examples.py
#mypyc --scripts-are-modules ./pycompss/dds/example_tasks.py
#mypyc --scripts-are-modules ./pycompss/dds/dds.py
# Unsupported yet - streams
#mypyc --scripts-are-modules ./pycompss/streams/distro_stream.py
#mypyc --scripts-are-modules ./pycompss/streams/components/objects/kafka_connectors.py
#mypyc --scripts-are-modules ./pycompss/streams/components/distro_stream_client.py
#mypyc --scripts-are-modules ./pycompss/streams/environment.py
#mypyc --scripts-are-modules ./pycompss/streams/types/requests.py


# TODO: CHANGE THE SOURCE NAMES TO *.py_source
