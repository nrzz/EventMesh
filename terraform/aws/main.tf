terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_vpc" "eventmesh" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-vpc"
  })
}

resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id                  = aws_vpc.eventmesh.id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = false

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-private-${count.index + 1}"
  })
}

resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)

  vpc_id                  = aws_vpc.eventmesh.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-public-${count.index + 1}"
  })
}

resource "aws_internet_gateway" "eventmesh" {
  vpc_id = aws_vpc.eventmesh.id

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-igw"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.eventmesh.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.eventmesh.id
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_sqs_queue" "eventmesh" {
  name                       = "${var.name_prefix}-events"
  visibility_timeout_seconds = var.sqs_visibility_timeout_seconds
  message_retention_seconds  = var.sqs_message_retention_seconds
  receive_wait_time_seconds  = var.sqs_receive_wait_time_seconds

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-events"
  })
}

resource "aws_sqs_queue" "eventmesh_dlq" {
  name                      = "${var.name_prefix}-events-dlq"
  message_retention_seconds = var.sqs_message_retention_seconds

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-events-dlq"
  })
}

resource "aws_sqs_queue_redrive_policy" "eventmesh" {
  queue_url = aws_sqs_queue.eventmesh.id

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.eventmesh_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
}
