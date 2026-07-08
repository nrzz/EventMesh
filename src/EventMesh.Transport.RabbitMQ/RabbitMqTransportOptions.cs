namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Configuration options for the RabbitMQ broker transport.
/// </summary>
public sealed class RabbitMqTransportOptions
{
    /// <summary>
    /// Gets the configuration section name.
    /// </summary>
    public const string SectionName = "EventMesh:Transports:RabbitMQ";

    /// <summary>
    /// Gets or sets the RabbitMQ host name.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the user name used to authenticate with RabbitMQ.
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the password used to authenticate with RabbitMQ.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the virtual host to connect to.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets the consumer prefetch count.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Gets or sets a value indicating whether publisher confirms are enabled.
    /// </summary>
    public bool PublisherConfirmsEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the timeout used when waiting for publisher confirms.
    /// </summary>
    public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum number of channels retained in the pool.
    /// </summary>
    public int ChannelPoolCapacity { get; set; } = 16;

    /// <summary>
    /// Gets or sets the polling interval used while waiting for messages during receive operations.
    /// </summary>
    public TimeSpan ReceivePollInterval { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the interval used to promote delayed messages when the delayed exchange plugin is absent.
    /// </summary>
    public TimeSpan DelayCheckInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Gets or sets the default maximum priority supported by declared queues.
    /// </summary>
    public byte MaxPriority { get; set; } = 10;

    /// <summary>
    /// Gets or sets the name of the delayed message exchange used when the plugin is available.
    /// </summary>
    public string DelayedExchangeName { get; set; } = "eventmesh.delayed";

    /// <summary>
    /// Gets or sets the application-specific connection name reported to RabbitMQ.
    /// </summary>
    public string? ClientProvidedName { get; set; } = "EventMesh.RabbitMQ";
}
