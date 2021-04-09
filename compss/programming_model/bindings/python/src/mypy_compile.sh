#!/bin/bash

###################################
## Compilation with mypyc script ##
###################################

mypyc ./pycompss/api/parameter.py
#mypyc ./pycompss/api/local.py
#mypyc ./pycompss/api/opencl.py
#mypyc ./pycompss/api/on_failure.py
#mypyc ./pycompss/api/reduction.py
#mypyc ./pycompss/api/ompss.py
#mypyc ./pycompss/api/container.py
#mypyc ./pycompss/api/mpi.py
#mypyc ./pycompss/api/compss.py
#mypyc ./pycompss/api/commons/data_type.py
#mypyc ./pycompss/api/commons/decorator.py
#mypyc ./pycompss/api/commons/information.py
#mypyc ./pycompss/api/commons/error_msgs.py
#mypyc ./pycompss/api/binary.py
#mypyc ./pycompss/api/constraint.py
#mypyc ./pycompss/api/task.py
#mypyc ./pycompss/api/decaf.py
#mypyc ./pycompss/api/IO.py
#mypyc ./pycompss/api/api.py
#mypyc ./pycompss/api/dummy/on_failure.py
#mypyc ./pycompss/api/dummy/reduction.py
#mypyc ./pycompss/api/dummy/container.py
#mypyc ./pycompss/api/dummy/constraint.py
#mypyc ./pycompss/api/dummy/task.py
#mypyc ./pycompss/api/dummy/api.py
#mypyc ./pycompss/api/implement.py
#mypyc ./pycompss/api/exceptions.py
#mypyc ./pycompss/api/multinode.py
#mypyc ./pycompss/interactive.py
#mypyc ./pycompss/runtime/commons.py
#mypyc ./pycompss/runtime/binding.py
#mypyc ./pycompss/runtime/mpi/keys.py
#mypyc ./pycompss/runtime/constants.py
#mypyc ./pycompss/runtime/management/COMPSs.py
#mypyc ./pycompss/runtime/management/classes.py
#mypyc ./pycompss/runtime/management/direction.py
#mypyc ./pycompss/runtime/management/synchronization.py
#mypyc ./pycompss/runtime/management/link.py
#mypyc ./pycompss/runtime/management/object_tracker.py
#mypyc ./pycompss/runtime/launch.py
#mypyc ./pycompss/runtime/task/parameter.py
#mypyc ./pycompss/runtime/task/keys.py
#mypyc ./pycompss/runtime/task/commons.py
#mypyc ./pycompss/runtime/task/master.py
#mypyc ./pycompss/runtime/task/worker.py
#mypyc ./pycompss/runtime/task/arguments.py
#mypyc ./pycompss/runtime/task/core_element.py
#mypyc ./pycompss/dds/partition_generators.py
#mypyc ./pycompss/dds/heapq3.py
#mypyc ./pycompss/dds/tasks.py
#mypyc ./pycompss/dds/examples.py
#mypyc ./pycompss/dds/example_tasks.py
#mypyc ./pycompss/dds/dds.py
#mypyc ./pycompss/functions/data.py
#mypyc ./pycompss/functions/elapsed_time.py
#mypyc ./pycompss/functions/reduce.py
#mypyc ./pycompss/functions/profile.py
#mypyc ./pycompss/util/storages/persistent.py
#mypyc ./pycompss/util/logger/helpers.py
#mypyc ./pycompss/util/objects/util.py
#mypyc ./pycompss/util/objects/sizer.py
#mypyc ./pycompss/util/objects/properties.py
#mypyc ./pycompss/util/objects/replace.py
#mypyc ./pycompss/util/jvm/parser.py
#mypyc ./pycompss/util/interactive/state.py
#mypyc ./pycompss/util/interactive/helpers.py
#mypyc ./pycompss/util/interactive/outwatcher.py
#mypyc ./pycompss/util/interactive/utils.py
#mypyc ./pycompss/util/interactive/flags.py
#mypyc ./pycompss/util/interactive/graphs.py
#mypyc ./pycompss/util/interactive/events.py
#mypyc ./pycompss/util/environment/configuration.py
#mypyc ./pycompss/util/mpi/helper.py
#mypyc ./pycompss/util/serialization/serializer.py
#mypyc ./pycompss/util/serialization/extended_support.py
#mypyc ./pycompss/util/std/redirects.py
#mypyc ./pycompss/util/supercomputer/scs.py
# mypyc ./pycompss/util/arguments.py
#mypyc ./pycompss/util/tracing/helpers.py
# mypyc ./pycompss/util/context.py
mypyc ./pycompss/util/exceptions.py
#mypyc ./pycompss/util/warnings/modules.py
#mypyc ./pycompss/worker/container/container_worker.py
#mypyc ./pycompss/worker/container/pythonpath_fixer.py
#mypyc ./pycompss/worker/piper/cache/tracker.py
#mypyc ./pycompss/worker/piper/cache/setup.py
#mypyc ./pycompss/worker/piper/piper_worker.py
#mypyc ./pycompss/worker/piper/commons/executor.py
#mypyc ./pycompss/worker/piper/commons/utils.py
#mypyc ./pycompss/worker/piper/commons/constants.py
#mypyc ./pycompss/worker/piper/mpi_piper_worker.py
#mypyc ./pycompss/worker/commons/executor.py
#mypyc ./pycompss/worker/commons/worker.py
#mypyc ./pycompss/worker/commons/constants.py
#mypyc ./pycompss/worker/external/mpi_executor.py
#mypyc ./pycompss/worker/gat/commons/constants.py
#mypyc ./pycompss/worker/gat/worker.py
#mypyc ./pycompss/streams/distro_stream.py
#mypyc ./pycompss/streams/components/objects/kafka_connectors.py
#mypyc ./pycompss/streams/components/distro_stream_client.py
#mypyc ./pycompss/streams/environment.py
#mypyc ./pycompss/streams/types/requests.py


# TODO: CHANGE THE SOURCE NAMES TO *.py_source
