provider "aws" {
  region  = "us-east-2"
  version = "~> 3.37"
}


terraform {
  backend "s3" {
    bucket = "terraform-state-igti-paulo"
    region = "us-east-2"
  }
}
