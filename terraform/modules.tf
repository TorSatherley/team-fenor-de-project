module "extract_module" {
  source         = "./extract_module"
  aws_account_id = local.aws_account_id
  aws_region     = local.aws_region
  default_tags   = var.default_tags
}


