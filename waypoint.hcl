project = "prosanteconnect/pscextract"

# Labels can be specified for organizational purposes.
labels = { "domaine" = "psc" }

runner {
    enabled = true
    data_source "git" {
        url = "https://github.com/prosanteconnect/pscextract.git"
    }
}

variable "public_hostname" {
    type    = string
    default = "forge.psc.henix.asipsante.fr"
}

# An application to deploy.
app "prosanteconnect/pscextract" {
    # Build specifies how an application should be deployed. In this case,
    # we'll build using a Dockerfile and keeping it in a local registry.
    build {
        use "pack" {
	  builder = "gcr.io/buildpacks/builder:v1"
	}

        # Uncomment below to use a remote docker registry to push your built images.
        #
        registry {
           use "docker" {
             image = artifact.image
             tag   = gitrefpretty()
             encoded_auth = filebase64("/secrets/dockerAuth.json")
           }
        }
    }

    # Deploy to Nomad
    deploy {
      use "nomad-jobspec" {
        jobspec = templatefile("${path.app}/pscextract.nomad.tpl", {
		public_hostname = var.public_hostname
		})
      }
    }
}
