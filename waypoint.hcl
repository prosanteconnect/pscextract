project = "prosanteconnect/${workspace.name}/pscextract"

# Labels can be specified for organizational purposes.
labels = { "domaine" = "psc" }

runner {
    enabled = true
    profile = "secpsc-${workspace.name}"
    data_source "git" {
        url = "https://github.com/prosanteconnect/pscextract.git"
        ref = "${workspace.name}"
    }
    poll {
        enabled = false
    }
}

# An application to deploy.
app "prosanteconnect/pscextract" {
  # the Build step is required and specifies how an application image should be built and published. In this case,
  # we use docker-pull, we simply pull an image as is.
  build {
    use "docker" {
      build_args = {
        "proxy_address" = var.proxy_address
      }
      dockerfile = "${path.app}/${var.dockerfile_path}"
    }
    # Uncomment below to use a remote docker registry to push your built images.
    registry {
      use "docker" {
        image = "${var.registry_username}/pscextract"
        tag   = gitrefpretty()
        username = var.registry_username
        password = var.registry_password
      }
    }
  }

  # Deploy to Nomad
  deploy {
    use "nomad-jobspec" {
      jobspec = templatefile("${path.app}/pscextract.nomad.tpl", {
        datacenter = var.datacenter
        nomad_namespace = var.nomad_namespace
        registry_username = var.registry_username
      })
    }
  }
}

variable "datacenter" {
  type    = string
  default = "dc1"
}

variable "registry_username" {
  type    = string
  default = ""
}

variable "registry_password" {
  type    = string
  default = ""
}

variable "dockerfile_path" {
  type = string
  default = "Dockerfile"
}

variable "proxy_address" {
  type = string
  default = "proxy_address"
}

variable "registry_path" {
  type = string
  default = "registry.repo.proxy-dev-forge.asip.hst.fluxus.net/prosanteconnect"
}
