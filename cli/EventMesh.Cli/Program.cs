using System.CommandLine;
using EventMesh.Cli.Commands;

var apiUrlOption = new Option<string?>("--api-url")
{
    Description = "Management API base URL. When omitted, the local in-memory transport is used.",
};

var rootCommand = new RootCommand("EventMesh command-line tool for publish, subscribe, replay, and operations.");
rootCommand.AddGlobalOption(apiUrlOption);

ICliCommand[] commands =
[
    new PublishCommand(),
    new SubscribeCommand(),
    new ReplayCommand(),
    new QueuesCommand(),
    new TopicsCommand(),
    new PluginsCommand(),
    new HealthCommand(),
    new BenchmarkCommand(),
];

foreach (var command in commands)
{
    rootCommand.Add(command.CreateCommand(apiUrlOption));
}

return await rootCommand.InvokeAsync(args);
