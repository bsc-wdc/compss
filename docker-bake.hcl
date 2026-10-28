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

####################
# PRIVATE TARGETS
####################

target "_common" {
  args = {
    # apt

    # web
    DOCKER_CE_CLI_VERSION = "29.9.2-1~ubuntu.24.04~noble"
    GRADLE_VERSION = "8.14.2"
    MAVEN_VERSION = "3.9.16"

    # pip
    MYPY_VERSION = "1.20.2"
    ROC_VALIDATOR_VERSION = "0.11.4"
    ROCRATE_VERSION = "0.16.0"
  }
}

target "build" {
  inherits = ["_common"]
  # output = [{ type = "cacheonly" }]
  platforms = [for arch in ARCHS: "linux/${arch}64"]
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
  platforms = [for arch in setsubtract(ARCHS, hpc == "" ? [] : ["riscv"]): "linux/${arch}64"]
  tags = [for tag in TAGS: "compss/${lan}compss${hpc}:${tag}"]
}

target "cases" {
  inherits = ["_public"]
  name = "compss-${var}"
  matrix = {
    var = ["full", "orchestrator", "tutorial"]
  }
  target = "compss-${var}"
  dockerfile = "utils/docker/release/${var}.Dockerfile"
  platforms = [for arch in setsubtract(ARCHS, ["riscv"]): "linux/${arch}64"]
  tags = [for tag in TAGS: "compss/compss-${var}:${tag}"]
}

####################
# GROUPS
####################

group "default" {
  targets = ["pycompss", "compss-orchestrator"]
}

group "release" {
  targets = ["bindings", "cases"]
}
