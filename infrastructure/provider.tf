provider "aws" {
  region = var.aws_region
  version = "~> 3.7"

}


terraform {

  backend "s3" {
      bucket = "terraform-state-igti-paulo"
      region = "us-east-2"
  }
}