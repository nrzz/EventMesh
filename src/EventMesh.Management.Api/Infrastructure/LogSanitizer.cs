using System.Globalization;
using System.Text;

namespace EventMesh.Management.Api.Infrastructure;

/// <summary>
/// Neutralizes caller-supplied text before it is written to a log entry, so a request value cannot
/// forge extra log lines or smuggle terminal escape sequences into the log stream.
/// </summary>
internal static class LogSanitizer
{
    /// <summary>
    /// The default cap, in characters, for a sanitized value. It is above the longest name any
    /// supported broker accepts, so legitimate topic and queue names are never shortened.
    /// </summary>
    internal const int DefaultMaxLength = 256;

    /// <summary>
    /// Removes carriage returns, line feeds, every other control character and the Unicode line and
    /// paragraph separators from <paramref name="value"/>, then caps the result at
    /// <paramref name="maxLength"/> characters.
    /// </summary>
    /// <param name="value">The caller-supplied text; <see langword="null"/> yields an empty string.</param>
    /// <param name="maxLength">The maximum number of characters to keep.</param>
    /// <returns>A single-line string that is safe to pass to a logger.</returns>
    internal static string Sanitize(string? value, int maxLength = DefaultMaxLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxLength);

        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(Math.Min(value.Length, maxLength));
        foreach (var c in value)
        {
            if (builder.Length == maxLength)
            {
                break;
            }

            if (IsLineBreakOrControl(c))
            {
                continue;
            }

            builder.Append(c);
        }

        // Do not leave half of a surrogate pair behind when the cap cut one in two.
        if (builder.Length > 0 && char.IsHighSurrogate(builder[^1]))
        {
            builder.Length--;
        }

        return builder.ToString();
    }

    private static bool IsLineBreakOrControl(char c) =>
        char.IsControl(c)
        || char.GetUnicodeCategory(c) is UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator;
}
