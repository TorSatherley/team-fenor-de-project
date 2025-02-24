terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

    local = {
      source  = "hashicorp/local"
      version = "2.5.2"
    }

    archive = {
      source  = "hashicorp/archive"
      version = "2.7.0"
    }

    null = {
      source  = "hashicorp/null"
      version = "3.2.3"
    }

  }
  backend "s3" {
    bucket = "terraform-tfstate-totesys-project"
    key    = "terraform-tfstate-totesys/totesys_project.tfstate"
    region = "eu-west-2"
  }

}


provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      Owner       = "Team Fenor"
      Project     = "Totesys DE Project"
      Environment = "Development"
    }
  }
}

