using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class QueuesCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var command = new Command("queues", "List provisioned queues.");

        command.SetHandler(async (InvocationContext context) =>
        {
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);
            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var queues = await client.ListQueuesAsync(context.GetCancellationToken());

            if (queues.Count == 0)
            {
                Console.WriteLine("No queues found.");
                return;
            }

            foreach (var queue in queues)
            {
                Console.WriteLine(queue);
            }
        });

        return command;
    }
}
