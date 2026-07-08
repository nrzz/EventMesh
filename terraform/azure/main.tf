terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "eventmesh" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_servicebus_namespace" "eventmesh" {
  name                = var.servicebus_namespace_name
  location            = azurerm_resource_group.eventmesh.location
  resource_group_name = azurerm_resource_group.eventmesh.name
  sku                 = var.servicebus_sku
  tags                = var.tags
}

resource "azurerm_servicebus_queue" "events" {
  name                                    = var.events_queue_name
  namespace_id                            = azurerm_servicebus_namespace.eventmesh.id
  max_delivery_count                      = var.max_delivery_count
  dead_lettering_on_message_expiration    = true
  lock_duration                           = var.lock_duration
  default_message_ttl                     = var.default_message_ttl
}

resource "azurerm_servicebus_queue" "events_dlq" {
  name         = var.events_dlq_name
  namespace_id = azurerm_servicebus_namespace.eventmesh.id
}
