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

target "build" {
  # output = [{ type = "cacheonly" }]
  platforms = [for arch in ARCHS: "linux/${arch}64"]
  tags = ["build"]
}

target "tests" {
  context = "tests"
  contexts = {
    build = "target:build"
  }
  platforms = [BAKE_LOCAL_PLATFORM]
  tags = ["tests"]
}

target "_common" {
  contexts = {
    build = "target:build"
  }
  labels = LABELS
  output = PUSH ? [{ type = "registry" }] : [{ type = "image" }]
  platforms = [for arch in ARCHS: "linux/${arch}64"]
}

target "bindings" {
  inherits = ["_common"]
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
  inherits = ["_common"]
  name = "compss-${var}"
  matrix = {
    var = ["orchestra", "full"]
  }
  target = "compss-${var}"
  dockerfile = "utils/docker/release/${var}.Dockerfile"
  platforms = ["linux/amd64"]
  tags = [for tag in TAGS: "compss-${var}:${tag}"]
}

target "tutorial" {
  inherits = ["_common"]
  dockerfile = "utils/docker/release/tutorial.Dockerfile"
  contexts = {
    compss-full = "target:compss-full"
  }
  platforms = ["linux/amd64"]
  tags = ["tutorial"]
}

group "default" {
  targets = ["pycompss", "compss-orchestra"]
}

group "release" {
  targets = ["bindings", "cases", "tutorial"]
}