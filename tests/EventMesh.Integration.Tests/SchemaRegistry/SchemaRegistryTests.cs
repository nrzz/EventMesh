using EventMesh.SchemaRegistry;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Integration.Tests.SchemaRegistry;

public sealed class SchemaRegistryTests
{
    [Fact]
    public async Task InMemorySchemaRegistry_registers_and_retrieves_json_schemas()
    {
        var services = new ServiceCollection();
        services.AddEventMeshSchemaRegistry();
        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<ISchemaRegistry>();

        const string definition = """
            {
              "$schema": "https://json-schema.org/draft/2020-12/schema",
              "type": "object",
              "properties": {
                "orderId": { "type": "string" }
              },
              "required": ["orderId"]
            }
            """;

        var registration = await registry.RegisterAsync("orders.created", SchemaFormat.Json, definition);

        registration.Version.Should().Be(1);
        var latest = await registry.GetLatestAsync("orders.created");
        latest.Should().NotBeNull();
        latest!.Definition.Should().Be(definition);
    }

    [Fact]
    public async Task InMemorySchemaRegistry_registers_avro_schemas()
    {
        var services = new ServiceCollection();
        services.AddEventMeshSchemaRegistry();
        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<ISchemaRegistry>();

        const string definition = """
            {
              "type": "record",
              "name": "OrderCreated",
              "fields": [
                { "name": "orderId", "type": "string" }
              ]
            }
            """;

        var registration = await registry.RegisterAsync("orders.created.avro", SchemaFormat.Avro, definition);

        registration.Format.Should().Be(SchemaFormat.Avro);
        (await registry.ListSubjectsAsync()).Should().Contain("orders.created.avro");
    }
}
