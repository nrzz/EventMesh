using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class SubscribeCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var destinationOption = new Option<string>(
            ["--destination", "-d"],
            "Topic, queue, or subscription name.")
        {
            IsRequired = true,
        };

        var maxMessagesOption = new Option<int?>(
            ["--max-messages", "-n"],
            "Maximum number of messages to receive before exiting.");

        var command = new Command("subscribe", "Subscribe to a topic or queue and print messages.");
        command.Add(destinationOption);
        command.Add(maxMessagesOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var destination = context.ParseResult.GetValueForOption(destinationOption)!;
            var maxMessages = context.ParseResult.GetValueForOption(maxMessagesOption);
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);

            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            Console.Error.WriteLine($"Subscribed to '{destination}'. Press Ctrl+C to stop.");
            await client.SubscribeAsync(destination, maxMessages, context.GetCancellationToken());
        });

        return command;
    }
}
