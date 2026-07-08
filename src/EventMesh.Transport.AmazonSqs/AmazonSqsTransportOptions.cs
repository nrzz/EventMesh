using Amazon;
using Amazon.Runtime;
using Amazon.SQS;

namespace EventMesh.Transport.AmazonSqs;

/// <summary>
/// Configuration options for the Amazon SQS broker transport.
/// </summary>
public sealed class AmazonSqsTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:AmazonSqs";

    /// <summary>
    /// Gets or sets the AWS region system name.
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Gets or sets an optional custom service URL for LocalStack or other SQS-compatible endpoints.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// Gets or sets the AWS access key identifier.
    /// </summary>
    public string? AccessKeyId { get; set; }

    /// <summary>
    /// Gets or sets the AWS secret access key.
    /// </summary>
    public string? SecretAccessKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS session token.
    /// </summary>
    public string? SessionToken { get; set; }

    /// <summary>
    /// Gets or sets the AWS account identifier used when building queue ARNs for redrive policies.
    /// </summary>
    public string AccountId { get; set; } = "000000000000";

    /// <summary>
    /// Gets or sets the default queue visibility timeout in seconds.
    /// </summary>
    public int VisibilityTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Gets or sets the default maximum receive count before redrive to a dead-letter queue.
    /// </summary>
    public int DefaultMaxReceiveCount { get; set; } = 5;

    /// <summary>
    /// Gets or sets the suffix appended when no explicit dead-letter destination is configured.
    /// </summary>
    public string DefaultDeadLetterSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Gets or sets the interval used while polling for messages when none are available.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the long-polling wait time in seconds for receive operations.
    /// </summary>
    public int ReceiveWaitTimeSeconds { get; set; } = 1;

    /// <summary>
    /// Creates a configured <see cref="AmazonSQSClient"/> from the current options.
    /// </summary>
    public AmazonSQSClient CreateClient()
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(Region),
        };

        if (!string.IsNullOrWhiteSpace(ServiceUrl))
        {
            config.ServiceURL = ServiceUrl;
        }

        AWSCredentials? credentials = null;
        if (!string.IsNullOrWhiteSpace(AccessKeyId) && !string.IsNullOrWhiteSpace(SecretAccessKey))
        {
            credentials = string.IsNullOrWhiteSpace(SessionToken)
                ? new BasicAWSCredentials(AccessKeyId, SecretAccessKey)
                : new SessionAWSCredentials(AccessKeyId, SecretAccessKey, SessionToken);
        }
        else if (!string.IsNullOrWhiteSpace(ServiceUrl))
        {
            credentials = new BasicAWSCredentials("test", "test");
        }

        return credentials is null
            ? new AmazonSQSClient(config)
            : new AmazonSQSClient(credentials, config);
    }
}
