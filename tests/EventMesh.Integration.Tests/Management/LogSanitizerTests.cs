using EventMesh.Management.Api.Infrastructure;
using FluentAssertions;

namespace EventMesh.Integration.Tests.Management;

public sealed class LogSanitizerTests
{
    [Theory]
    [InlineData("orders\r\n2026-01-01 INFO forged entry", "orders2026-01-01 INFO forged entry")]
    [InlineData("first\nsecond", "firstsecond")]
    [InlineData("first\rsecond", "firstsecond")]
    public void Sanitize_removes_carriage_returns_and_line_feeds(string input, string expected)
    {
        LogSanitizer.Sanitize(input).Should().Be(expected);
    }

    [Fact]
    public void Sanitize_removes_other_control_characters()
    {
        var input = "a" + (char)0 + "b" + (char)9 + "c" + (char)27 + "d" + (char)127 + "e" + (char)0x85 + "f";

        LogSanitizer.Sanitize(input).Should().Be("abcdef");
    }

    [Fact]
    public void Sanitize_removes_unicode_line_and_paragraph_separators()
    {
        var input = "a" + (char)0x2028 + "b" + (char)0x2029 + "c";

        LogSanitizer.Sanitize(input).Should().Be("abc");
    }

    [Theory]
    [InlineData("orders.created-v1")]
    [InlineData("tenant_a/billing events")]
    public void Sanitize_leaves_ordinary_names_unchanged(string input)
    {
        LogSanitizer.Sanitize(input).Should().Be(input);
    }

    [Fact]
    public void Sanitize_leaves_non_ascii_letters_unchanged()
    {
        var input = "caf" + (char)0xE9 + "-topic";

        LogSanitizer.Sanitize(input).Should().Be(input);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Sanitize_returns_an_empty_string_for_missing_values(string? input)
    {
        LogSanitizer.Sanitize(input).Should().BeEmpty();
    }

    [Fact]
    public void Sanitize_caps_the_length_at_the_default_limit()
    {
        var result = LogSanitizer.Sanitize(new string('a', LogSanitizer.DefaultMaxLength * 4));

        result.Should().HaveLength(LogSanitizer.DefaultMaxLength);
    }

    [Fact]
    public void Sanitize_caps_the_length_after_stripping_control_characters()
    {
        var result = LogSanitizer.Sanitize("a\r\nb\r\nc\r\nd", maxLength: 3);

        result.Should().Be("abc");
    }

    [Fact]
    public void Sanitize_does_not_leave_half_a_surrogate_pair_when_capping()
    {
        var input = new string('a', 9) + char.ConvertFromUtf32(0x1F600);

        var result = LogSanitizer.Sanitize(input, maxLength: 10);

        result.Should().Be(new string('a', 9));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Sanitize_rejects_a_non_positive_limit(int maxLength)
    {
        var act = () => LogSanitizer.Sanitize("value", maxLength);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
