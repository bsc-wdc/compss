####################
# VARIABLES
####################

variable "LABELS" {
  default = {
    "maintainer" = "COMPSs Support <support-compss@bsc.es>"
    "vendor" = "Barcelona Supercomputing Center (BSC)"
    "url"= "https://compss.bsc.es"
  }
  type = map(string)
}

variable "TAGS" {
  default = ["latest"]
  type = set(string)
}

variable "ARCHS" {
  default = ["amd"]
  validation {
    condition = setintersection(ARCHS, ["amd", "arm", "riscv"]) == ARCHS
    error_message = "ARCHS must only contain 'amd', 'arm' and/or 'riscv'."
  }
  type = set(string)
}

variable "PUSH" {
  default = false
  type = bool
}

####################
# PRIVATE TARGETS
####################

target "_common" {
  args = {
    # apt
    AUTOMAKE_VERSION = "1:1.16.5-1.3ubuntu1"
    BC_VERSION = "1.07.1-3ubuntu4"
    BUILD_ESSENTIAL_VERSION = "12.10ubuntu1"
    CMAKE_VERSION = "3.28.3-1build7"
    FILE_VERSION = "1:5.45-3build1"
    GFORTRAN_VERSION = "4:13.2.0-7ubuntu1"
    GIT_VERSION = "1:2.43.0-1ubuntu7.3"
    GPP_VERSION = "4:13.2.0-7ubuntu1"
    GRAPHVIZ_VERSION = "2.42.2-9ubuntu0.1"
    JQ_VERSION = "1.7.1-3ubuntu0.24.04.2"
    LIBBOOST_SERIALIZATION_DEV_VERSION = "1.83.0.1ubuntu2"
    LIBBOOST_SYSTEM_DEV_VERSION = "1.83.0.1ubuntu2"
    LIBJPEG_DEV_VERSION = "8c-2ubuntu11"
    LIBOPENMPI_DEV_VERSION = "4.1.6-7ubuntu2"
    LIBTOOL_VERSION = "2.4.7-7build1"
    LIBXML2_DEV_VERSION = "2.9.14+dfsg-1.3ubuntu3.8"
    MAKE_VERSION = "4.3-4.1build2"
    OPENSSH_SERVER_VERSION = "1:9.6p1-3ubuntu13.16"
    PAPI_TOOLS_VERSION = "7.1.0-5build1"
    PYTHON3_VERSION = "3.12.3-0ubuntu2.1"
    PYTHON3_DEV_VERSION = "3.12.3-0ubuntu2.1"
    PYTHON3_PIP_VERSION = "24.0+dfsg-1ubuntu1.3"
    R_BASE_VERSION = "4.3.3-2build2"
    UNZIP_VERSION = "6.0-28ubuntu4.1"

    # web
    DOCKER_CE_CLI_VERSION = "29.4.3-1~ubuntu.24.04~noble"
    GRADLE_VERSION = "8.7"
    MAVEN_VERSION = "3.9.15"

    # pip
    BLACK_JUPYTER_VERSION = "26.3.1"
    DATACLAY_VERSION = "4.3.1"
    DECORATOR_VERSION = "5.2.1"
    DILL_VERSION = "0.4.1"
    GUPPY3_VERSION = "3.1.6"
    IPYWIDGETS_VERSION = "8.1.8"
    JUPYTERLAB_VERSION = "4.5.7"
    KAFKA_PYTHON_VERSION = "2.3.1"
    MATPLOTLIB_VERSION = "3.10.9"
    MEMORY_PROFILER_VERSION = "0.61.0"
    MPI4PY_VERSION = "4.1.1"
    MYPY_VERSION = "1.20.2"
    NUMBA_VERSION = "0.65.1"
    PANDAS_VERSION = "3.0.2"
    PYCODESTYLE_VERSION = "2.14.0"
    PYDOCSTYLE_VERSION = "6.3.0"
    PYTEST_VERSION = "9.0.3"
    PYTZ_VERSION = "2026.2"
    REDIS_PY_CLUSTER_VERSION = "2.1.3"
    ROC_VALIDATOR_VERSION = "0.8.1"
    ROCRATE_VERSION = "0.14.2"
    TABULATE_VERSION = "0.10.0"
    TYPES_TABULATE_VERSION = "0.10.0.20260508"

    # git
    DLB_VERSION="3.6.0"
    EXTRAE_VERSION="3.5.3"
  }
  platforms = [for arch in ARCHS: "linux/${arch}64"]
}

target "build" {
  inherits = ["_common"]
  # output = [{ type = "cacheonly" }]
  tags = ["build"]
}

target "tests" {
  inherits = ["_common"]
  context = "tests"
  contexts = {
    build = "target:build"
  }
  platforms = [BAKE_LOCAL_PLATFORM]
  tags = ["tests"]
}

####################
# PUBLIC TARGETS
####################

target "_public" {
  inherits = ["_common"]
  contexts = {
    build = "target:build"
  }
  labels = LABELS
  output = PUSH ? [{ type = "registry" }] : [{ type = "image" }]
}

target "bindings" {
  inherits = ["_public"]
  name = "${lan}compss${hpc}"
  matrix = {
    lan = ["", "py", "c", "r"]
    hpc = ["", "-hpc"]
  }
  target = "${lan}compss${hpc}"
  dockerfile = "utils/docker/release/${lan}compss.Dockerfile"
  tags = [for tag in TAGS: "${lan}compss${hpc}:${tag}"]
}

target "cases" {
  inherits = ["_public"]
  name = "compss-${var}"
  matrix = {
    var = ["full", "orchestra", "tutorial"]
  }
  target = "compss-${var}"
  dockerfile = "utils/docker/release/${var}.Dockerfile"
  platforms = ["linux/amd64"]
  tags = [for tag in TAGS: "compss-${var}:${tag}"]
}

####################
# GROUPS
####################

group "default" {
  targets = ["pycompss", "compss-orchestra"]
}

group "release" {
  targets = ["bindings", "cases"]
}