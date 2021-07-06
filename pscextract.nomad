job "pscextract" {
  datacenters = ["dc1"]
  type = "service"

  group "pscextract-services" {
    count = "1"
    restart {
      attempts = 3
      delay = "60s"
      interval = "1h"
      mode = "fail"
    }

    update {
      max_parallel      = 1
      canary            = 1
      min_healthy_time  = "30s"
      progress_deadline = "5m"
      healthy_deadline  = "2m"
      auto_revert       = true
      auto_promote      = true
    }

    network {
      port "http" {
        to = 8080
      }
    }

    task "pscextract" {
      env {
        JAVA_TOOL_OPTIONS="-Xms1500m -Xmx1500m -XX:+UseG1GC -Dspring.config.location=/secrets/application.properties"
      }
      driver = "docker"
      config {
        image = "prosanteconnect/pscextract:latest"
        volumes = [
          "name=pscextract-data,io_priority=high,size=3,repl=3:/app/extract-repo"
        ]
        volume_driver = "pxd"
        ports = ["http"]
      }
      template {
        data = <<EOF
server.servlet.context-path=/pscextract/v1
mongodb.addr={{ range service "psc-mongodb" }}{{ .Address }}:{{ .Port }}{{ end }}
mongodb.name=mongodb
files.directory=/app/extract-repo
extract.name=PSC-extract
EOF
        destination = "secrets/application.properties"
      }
      resources {
        cpu = 2000
        memory = 2048
      }
      service {
        name = "${NOMAD_JOB_NAME}"
        tags = ["urlprefix-/pscextract/v1/"]
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
