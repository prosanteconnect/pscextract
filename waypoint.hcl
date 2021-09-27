project = "prosanteconnect/pscextract"

# Labels can be specified for organizational purposes.
labels = { "domaine" = "psc" }

runner {
    enabled = true
    data_source "git" {
        url = "https://github.com/prosanteconnect/pscextract.git"
    }
}

# An application to deploy.
app "prosanteconnect/pscextract" {
  # the Build step is required and specifies how an application image should be built and published. In this case,
  # we use docker-pull, we simply pull an image as is.
  build {
    use "docker-pull" {
      image = var.image
      tag   = "latest"
    }
  }

  # Deploy to Nomad
  deploy {
    use "nomad-jobspec" {
      jobspec = templatefile("${path.app}/pscextract.nomad.tpl", {
        datacenter = var.datacenter
        image = var.image
        tag = "latest"
      })
    }
  }
}

variable "datacenter" {
  type    = string
  default = "production"
}

variable "image" {
  type    = string
  default = "prosanteconnect/pscextract"
}

