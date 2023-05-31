provider "aws" {
  region  = var.aws_region
  version = "~> 3.37"
}

terraform {
  backend "s3" {
    bucket = "terraform-state-igti-paulo"
    region = var.aws_region
  }
}
