
 # Project

variable "aws_account_id" {
    type = string
}

variable "aws_region" {
    type = string
}

variable "default_tags" {
  type = map(string)
}


# Function names 

variable "lambda_extract_handler" {
  type = string 
  default = "lambda_extract_handler"
}

variable "lambda_transform_handler" {
  type = string 
  default = "lambda_transform_handler"
}

variable "lambda_load_handler" {
  type = string 
  default = "lambda_load_handler"
}


variable "enable-disable-eventbridge" {
  type    = string
}


