variable "name_prefix" {
  description = "Prefix applied to created AWS resources."
  type        = string
  default     = "eventmesh"
}

variable "aws_region" {
  description = "AWS region for EventMesh infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the EventMesh VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones used for subnets."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDR blocks."
  type        = list(string)
  default     = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDR blocks."
  type        = list(string)
  default     = ["10.20.101.0/24", "10.20.102.0/24"]
}

variable "sqs_visibility_timeout_seconds" {
  description = "Visibility timeout for the primary SQS queue."
  type        = number
  default     = 30
}

variable "sqs_message_retention_seconds" {
  description = "Message retention period for SQS queues."
  type        = number
  default     = 1209600
}

variable "sqs_receive_wait_time_seconds" {
  description = "Long polling wait time for the primary SQS queue."
  type        = number
  default     = 20
}

variable "sqs_max_receive_count" {
  description = "Maximum receive count before messages are sent to the DLQ."
  type        = number
  default     = 5
}

variable "tags" {
  description = "Tags applied to all created resources."
  type        = map(string)
  default = {
    project = "eventmesh"
  }
}
