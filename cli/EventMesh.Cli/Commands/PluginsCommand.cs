using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class PluginsCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var directoryOption = new Option<string?>("--directory")
        {
            Description = "Optional plugin directory to scan for IEventMeshPlugin implementations.",
        };

        var command = new Command("plugins", "List registered or discovered plugins.");
        command.Add(directoryOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var directory = context.ParseResult.GetValueForOption(directoryOption);
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);

            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var plugins = await client.ListPluginsAsync(directory, context.GetCancellationToken());

            if (plugins.Count == 0)
            {
                Console.WriteLine("No plugins found.");
                return;
            }

            foreach (var plugin in plugins)
            {
                var line = $"{plugin.Name}\t{plugin.Version}";
                if (!string.IsNullOrWhiteSpace(plugin.Source))
                {
                    line += $"\t{plugin.Source}";
                }

                if (!string.IsNullOrWhiteSpace(plugin.Error))
                {
                    line += $"\t(error: {plugin.Error})";
                }

                Console.WriteLine(line);
            }
        });

        return command;
    }
}
