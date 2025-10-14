group "default" {
  targets = [
    "linstor-csi",
    "nfs-server",
  ]
}

variable TAGS {
  default = "latest"
}

variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}

variable "REGISTRY" {
  default = "quay.io/piraeusdatastore"
}

variable "VERSION" {
  default = "unknown"
}

target "docker-metadata-action" {
  // Set when run in Gitlab Action, setting labels
  tags = split(",", TAGS)
}


function tags {
  params = [name, tags]
  result = flatten([
    for registry in split(",", REGISTRY) : [
      for tag in tags : "${registry}/${name}:${tag}"
    ]
  ])
}

target "linstor-csi" {
  inherits = ["docker-metadata-action"]
  tags = tags("piraeus-csi", target.docker-metadata-action.tags)
  context    = "./"
  dockerfile = "Dockerfile"
  platforms = split(",", PLATFORMS)
  args = {
    VERSION = VERSION
  }
}

target "nfs-server" {
  inherits = ["docker-metadata-action"]
  tags = tags("piraeus-csi-nfs-server", target.docker-metadata-action.tags)
  context    = "./"
  dockerfile = "nfs/Dockerfile"
  platforms = split(",", PLATFORMS)
  args = {
    VERSION = VERSION
  }
}
