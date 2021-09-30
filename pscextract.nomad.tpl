job "pscextract" {
  datacenters = ["${datacenter}"]
  type = "service"

  vault {
    policies = ["psc-ecosystem"]
    change_mode = "restart"
  }

  group "pscextract-services" {
    count = "1"
    restart {
      attempts = 3
      delay = "60s"
      interval = "1h"
      mode = "fail"
    }

    update {
      max_parallel = 1
      min_healthy_time = "30s"
      progress_deadline = "5m"
      healthy_deadline = "2m"
    }

    network {
      port "http" {
        to = 8080
      }
    }

    task "pscextract" {
      driver = "docker"
      env {
        JAVA_TOOL_OPTIONS = "-Dspring.config.location=/secrets/application.properties"
      }
      config {
        image = "${image}:${tag}"
        volumes = [
          "name=pscextract-data,io_priority=high,size=10,repl=3:/app/extract-repo"
        ]
        volume_driver = "pxd"
        ports = ["http"]
      }
      template {
        destination = "local/file.env"
        env = true
        data = <<EOF
PUBLIC_HOSTNAME={{ with secret "psc-ecosystem/pscextract" }}{{ .Data.data.public_hostname }}{{ end }}
EOF
      }
      template {
        data = <<EOF
server.servlet.context-path=/pscextract/v1
mongodb.host={{ range service "psc-mongodb" }}{{ .Address }}{{ end }}
mongodb.port={{ range service "psc-mongodb" }}{{ .Port }}{{ end }}
mongodb.name=mongodb
mongodb.username={{ with secret "psc-ecosystem/mongodb" }}{{ .Data.data.root_user}}{{ end }}
mongodb.password={{ with secret "psc-ecosystem/mongodb" }}{{ .Data.data.root_pass}}{{ end }}
mongodb.admin.database=admin
files.directory=/app/extract-repo
extract.name=Extraction_Pro_sante_connect
extract.test.name=Extraction_Pro_sante_connect_cartes_de_test_bascule
EOF
        destination = "secrets/application.properties"
      }
      resources {
        cpu = 1000
        memory = 1024
      }
      service {
        name = "$\u007BNOMAD_JOB_NAME\u007D"
        tags = ["urlprefix-$\u007BPUBLIC_HOSTNAME\u007D/pscextract/v1/"]
        port = "http"
        check {
          type = "http"
          path = "/pscextract/v1/check"
          port = "http"
          interval = "10s"
          timeout = "2s"
        }
      }
    }
  }
}
