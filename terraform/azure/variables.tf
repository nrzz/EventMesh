variable "resource_group_name" {
  description = "Azure resource group name for EventMesh resources."
  type        = string
  default     = "eventmesh-rg"
}

variable "location" {
  description = "Azure region for EventMesh resources."
  type        = string
  default     = "eastus"
}

variable "servicebus_namespace_name" {
  description = "Azure Service Bus namespace name."
  type        = string
  default     = "eventmesh-sb"
}

variable "servicebus_sku" {
  description = "Azure Service Bus SKU."
  type        = string
  default     = "Standard"
}

variable "events_queue_name" {
  description = "Primary events queue name."
  type        = string
  default     = "eventmesh-events"
}

variable "events_dlq_name" {
  description = "Dead-letter queue name."
  type        = string
  default     = "eventmesh-events-dlq"
}

variable "max_delivery_count" {
  description = "Maximum delivery count before dead-lettering."
  type        = number
  default     = 5
}

variable "lock_duration" {
  description = "Queue lock duration."
  type        = string
  default     = "PT1M"
}

variable "default_message_ttl" {
  description = "Default message time-to-live."
  type        = string
  default     = "P14D"
}

variable "tags" {
  description = "Tags applied to Azure resources."
  type        = map(string)
  default = {
    project = "eventmesh"
  }
}

output "resource_group_name" {
  description = "Name of the EventMesh resource group."
  value       = azurerm_resource_group.eventmesh.name
}

output "servicebus_namespace_id" {
  description = "ID of the Azure Service Bus namespace."
  value       = azurerm_servicebus_namespace.eventmesh.id
}

output "events_queue_id" {
  description = "ID of the primary events queue."
  value       = azurerm_servicebus_queue.events.id
}

output "events_dlq_id" {
  description = "ID of the events dead-letter queue."
  value       = azurerm_servicebus_queue.events_dlq.id
}
