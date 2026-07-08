using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class HealthCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var command = new Command("health", "Check EventMesh health.");

        command.SetHandler(async (InvocationContext context) =>
        {
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);
            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var report = await client.GetHealthAsync(context.GetCancellationToken());

            Console.WriteLine($"Status: {report.Status}");

            if (!string.IsNullOrWhiteSpace(report.Transport))
            {
                Console.WriteLine($"Transport: {report.Transport}");
            }

            if (!string.IsNullOrWhiteSpace(report.Mode))
            {
                Console.WriteLine($"Mode: {report.Mode}");
            }
        });

        return command;
    }
}
