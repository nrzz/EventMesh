using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class PublishCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var destinationOption = new Option<string>(
            ["--destination", "-d"],
            "Topic or queue name.")
        {
            IsRequired = true,
        };

        var bodyOption = new Option<string>(
            ["--body", "-b"],
            "Message body.")
        {
            IsRequired = true,
        };

        var routingKeyOption = new Option<string?>(
            ["--routing-key", "-k"],
            "Optional routing key for topic-based routing.");

        var command = new Command("publish", "Publish a message to a topic or queue.");
        command.Add(destinationOption);
        command.Add(bodyOption);
        command.Add(routingKeyOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var destination = context.ParseResult.GetValueForOption(destinationOption)!;
            var body = context.ParseResult.GetValueForOption(bodyOption)!;
            var routingKey = context.ParseResult.GetValueForOption(routingKeyOption);
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);

            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            await client.PublishAsync(destination, body, routingKey, context.GetCancellationToken());
            Console.WriteLine($"Published message to '{destination}'.");
        });

        return command;
    }
}
