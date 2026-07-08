using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class TopicsCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var command = new Command("topics", "List provisioned topics.");

        command.SetHandler(async (InvocationContext context) =>
        {
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);
            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var topics = await client.ListTopicsAsync(context.GetCancellationToken());

            if (topics.Count == 0)
            {
                Console.WriteLine("No topics found.");
                return;
            }

            foreach (var topic in topics)
            {
                Console.WriteLine(topic);
            }
        });

        return command;
    }
}
