#!/bin/bash -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC2164
cd "${SCRIPT_DIR}/../../src/"

###################################
## Compilation with mypyc script ##
###################################

mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/parameter.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/local.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/opencl.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/on_failure.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/reduction.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/ompss.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/container.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/mpi.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/compss.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/constants.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/implementation_types.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/data_type.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/decorator.py  # superclass that could be removed
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/information.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/commons/error_msgs.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/binary.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/constraint.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/task.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/decaf.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/IO.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/api.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/on_failure.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/reduction.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/container.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/constraint.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/task.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/dummy/api.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/implement.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/exceptions.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/api/multinode.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/interactive.py  # mypy failure with __builtin__
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/commons.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/binding.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/mpi/keys.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/constants.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/COMPSs.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/classes.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/direction.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/synchronization.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/link.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/management/object_tracker.py
# mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/launch.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/parameter.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/keys.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/commons.py  # superclass that could be removed
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/master.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/worker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/arguments.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/runtime/task/core_element.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/functions/data.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/functions/elapsed_time.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/functions/reduce.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/functions/profile.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/storages/persistent.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/logger/helpers.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/objects/util.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/objects/sizer.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/objects/properties.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/objects/replace.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/jvm/parser.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/state.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/helpers.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/outwatcher.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/utils.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/flags.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/graphs.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/interactive/events.py  # mypy failure with builtin function __pre_run_cell__
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/environment/configuration.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/mpi/helper.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/serialization/serializer.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/serialization/extended_support.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/std/redirects.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/supercomputer/scs.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/tracing/helpers.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/arguments.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/context.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/exceptions.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/util/warnings/modules.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/container/container_worker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/container/pythonpath_fixer.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/cache/tracker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/cache/setup.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/piper_worker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/commons/executor.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/commons/utils.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/commons/constants.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/piper/mpi_piper_worker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/commons/executor.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/commons/worker.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/commons/constants.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/external/mpi_executor.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/gat/commons/constants.py
mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/worker/gat/worker.py
# Unsupported yet - dds
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/partition_generators.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/heapq3.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/tasks.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/examples.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/example_tasks.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/dds/dds.py
# Unsupported yet - streams
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/streams/distro_stream.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/streams/components/objects/kafka_connectors.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/streams/components/distro_stream_client.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/streams/environment.py
#mypyc --scripts-are-modules --ignore-missing-imports ./pycompss/streams/types/requests.py

# shellcheck disable=SC2164
cd "${CURRENT_DIR}"

# Exit all ok
exit 0