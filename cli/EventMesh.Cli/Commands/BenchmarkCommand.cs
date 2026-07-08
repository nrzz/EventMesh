using System.CommandLine;
using System.CommandLine.Invocation;
using EventMesh.Cli.Services;

namespace EventMesh.Cli.Commands;

public sealed class BenchmarkCommand : ICliCommand
{
    public Command CreateCommand(Option<string?> apiUrlOption)
    {
        var destinationOption = new Option<string>(
            ["--destination", "-d"],
            "Topic or queue used for benchmark publishes.")
        {
            IsRequired = true,
        };

        var messageCountOption = new Option<int>(
            ["--messages", "-m"],
            "Number of messages to publish.");
        messageCountOption.SetDefaultValue(1000);

        var concurrencyOption = new Option<int>(
            ["--concurrency", "-c"],
            "Maximum parallel publish operations.");
        concurrencyOption.SetDefaultValue(4);

        var command = new Command("benchmark", "Run a quick publish benchmark.");
        command.Add(destinationOption);
        command.Add(messageCountOption);
        command.Add(concurrencyOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var destination = context.ParseResult.GetValueForOption(destinationOption)!;
            var messageCount = context.ParseResult.GetValueForOption(messageCountOption);
            var concurrency = context.ParseResult.GetValueForOption(concurrencyOption);
            var apiUrl = context.ParseResult.GetValueForOption(apiUrlOption);

            await using var client = await CliMeshClient.CreateAsync(apiUrl, context.GetCancellationToken());
            var result = await client.RunBenchmarkAsync(
                messageCount,
                concurrency,
                destination,
                context.GetCancellationToken());

            Console.WriteLine($"Destination: {result.Destination}");
            Console.WriteLine($"Messages: {result.MessageCount}");
            Console.WriteLine($"Concurrency: {result.Concurrency}");
            Console.WriteLine($"Elapsed: {result.ElapsedMilliseconds:F2} ms");
            Console.WriteLine($"Throughput: {result.MessagesPerSecond:F0} msg/s");
        });

        return command;
    }
}
