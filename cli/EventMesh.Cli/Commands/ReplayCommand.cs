using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Abstractions.Messaging;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class ReplayCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var sourceOption = new Option<string>(
            ["--source", "-s"],
            "Source topic, stream, or archive.")
        {
            IsRequired = true,
        };

        var destinationOption = new Option<string?>(
            "--destination",
            "Optional destination for replayed messages.");

        var fromOption = new Option<DateTimeOffset?>(
            "--from",
            "Inclusive replay start time (ISO 8601).");

        var toOption = new Option<DateTimeOffset?>(
            "--to",
            "Exclusive replay end time (ISO 8601).");

        var maxMessagesOption = new Option<int?>(
            ["--max-messages", "-n"],
            "Maximum number of messages to replay.");

        var command = new Command("replay", "Replay messages from a topic or archive.");
        command.Add(sourceOption);
        command.Add(destinationOption);
        command.Add(fromOption);
        command.Add(toOption);
        command.Add(maxMessagesOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var source = context.ParseResult.GetValueForOption(sourceOption)!;
            var destination = context.ParseResult.GetValueForOption(destinationOption);
            var from = context.ParseResult.GetValueForOption(fromOption);
            var to = context.ParseResult.GetValueForOption(toOption);
            var maxMessages = context.ParseResult.GetValueForOption(maxMessagesOption);
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);

            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var replayed = await client.ReplayAsync(
                source,
                new ReplayOptions
                {
                    Source = source,
                    Destination = destination,
                    From = from,
                    To = to,
                    MaxMessages = maxMessages,
                },
                context.GetCancellationToken());

            Console.WriteLine($"Replayed {replayed} message(s) from '{source}'.");
        });

        return command;
    }
}
