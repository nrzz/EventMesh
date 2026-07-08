output "vpc_id" {
  description = "ID of the EventMesh VPC."
  value       = aws_vpc.eventmesh.id
}

output "private_subnet_ids" {
  description = "IDs of private subnets."
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "IDs of public subnets."
  value       = aws_subnet.public[*].id
}

output "sqs_queue_url" {
  description = "URL of the primary EventMesh SQS queue."
  value       = aws_sqs_queue.eventmesh.url
}

output "sqs_queue_arn" {
  description = "ARN of the primary EventMesh SQS queue."
  value       = aws_sqs_queue.eventmesh.arn
}

output "sqs_dlq_url" {
  description = "URL of the EventMesh dead-letter queue."
  value       = aws_sqs_queue.eventmesh_dlq.url
}
