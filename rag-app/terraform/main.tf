terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 2.21.0"
    }
  }
}

provider "docker" {}

# Docker network
resource "docker_network" "rag_network" {
  name = "rag_network"
}

# Build Flask API image
resource "docker_image" "flask_api_image" {
  name = var.flask_image_name
  build {
    path = "../src/api"
  }
}

# Run Flask API container
resource "docker_container" "flask_api" {
  name  = "rag_backend"
  image = docker_image.flask_api_image.latest
  networks_advanced {
    name = docker_network.rag_network.name
  }
  ports {
    internal = 8000
    external = 8000
  }
  env = [
    "ENV=local",
    "DEBUG=True"
  ]
}
