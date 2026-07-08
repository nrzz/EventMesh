using System.CommandLine;

namespace EventMesh.Cli.Commands;

/// <summary>
/// Common contract for CLI subcommands.
/// </summary>
public interface ICliCommand
{
    Command CreateCommand(Option<string?> apiUrlOption);
}
