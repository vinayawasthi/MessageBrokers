using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ.Consumer;

class Program
{
    public static void Main(string[] args)
    {
        RunQ().Wait();
    }

    private static async Task RunQ()
    {
        // Load configuration
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables() // allow overrides via env vars
            .Build();

        var section = config.GetSection("RabbitMQ");
        var hostName = section["HostName"] ?? "localhost";
        var port = int.TryParse(section["Port"], out var p) ? p : 5672;
        var userName = section["UserName"] ?? "guest";
        var password = section["Password"] ?? "guest";
        var vhost = section["VirtualHost"] ?? "/";
        var queueName = section["QueueName"] ?? "demo.queue";
        var exchange = section["Exchange"] ?? "";          // default exchange
        var routingKey = section["RoutingKey"] ?? queueName;
        var durable = bool.TryParse(section["Durable"], out var d) ? d : true;
        var exclusive = bool.TryParse(section["Exclusive"], out var ex) ? ex : false;
        var autoDelete = bool.TryParse(section["AutoDelete"], out var ad) ? ad : false;
        var publisherConfirm = bool.TryParse(section["PublisherConfirm"], out var pc) ? pc : true;

        var factory = new ConnectionFactory
        {
            HostName = hostName,
            Port = port,
            UserName = userName,
            Password = password,
            VirtualHost = vhost,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        };

        Console.WriteLine($"Connecting to RabbitMQ at {hostName}:{port} vhost='{vhost}' as '{userName}'...");
        try
        {

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // Ensure the queue exists (durable, non-exclusive, not auto-deleted)
            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: durable,
                exclusive: exclusive,
                autoDelete: autoDelete,
                arguments: null
            );

            Console.WriteLine(" [*] Waiting for messages.");

            
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"    [x] Received {message}");
                return Task.CompletedTask;
            };

            await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);
        }
        catch (Exception exx)
        {
            Console.Error.WriteLine($"❌ Connection/Channel error: {exx.Message}");
        }

    }
}