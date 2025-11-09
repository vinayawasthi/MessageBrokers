using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RabbitMQ.Producer;

class Program
{
    public static void Main(string[] args)
    {
        //RunQ().Wait();

        RunOrderData().Wait();
    }

    private static async Task RunQ()
    {
        Console.WriteLine(Environment.CurrentDirectory);

        // Load configuration
        var config = new ConfigurationBuilder()
            .AddJsonFile("./appsettings.json", optional: true)
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

            // 1️⃣ Declare a fanout exchange
            await channel.ExchangeDeclareAsync(
                exchange: exchange,
                durable: durable,
                autoDelete: autoDelete,
                type: ExchangeType.Fanout);

            // Ensure the queue exists (durable, non-exclusive, not auto-deleted)
            //await channel.QueueDeclareAsync(
            //    queue: queueName,
            //    durable: durable,
            //    exclusive: exclusive,
            //    autoDelete: autoDelete
            //);

            var msgs = new string[]
            {
                "Agatha Christie",
                "Arthur Conan Doyle",
                "Charlotte Brontë",
                "Dan Brown",
                "Emily Brontë",
                "Ernest Hemingway",
                "F. Scott Fitzgerald",
                "Frank Herbert",
                "George Orwell",
                "H.G. Wells",
                "Harper Lee",
                "Isaac Asimov",
                "J.K. Rowling",
                "J.R.R. Tolkien",
                "Jane Austen",
                "Leo Tolstoy"
            };

            foreach (var msg in msgs)
            {
                var body = Encoding.UTF8.GetBytes(msg);

                // Publish to the default exchange with routing key == queue name
                var props = new BasicProperties
                {
                    Persistent = true // make message persistent when queue is durable
                };

                await channel.BasicPublishAsync(
                    exchange: exchange,            // default (amq.default)
                    routingKey: routingKey,
                    mandatory: false,
                    basicProperties: props,
                    body: body
                );

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
        catch (Exception exx)
        {
            Console.Error.WriteLine($"❌ Connection/Channel error: {exx.Message}");
        }

    }


    private static async Task RunOrderData()
    {
        Console.WriteLine(Environment.CurrentDirectory);

        // Load configuration
        var config = new ConfigurationBuilder()
            .AddJsonFile("./appsettings.json", optional: true)
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

        exchange = "orders-ex-topic";

        Console.WriteLine($"Connecting to RabbitMQ at {hostName}:{port} vhost='{vhost}' as '{userName}'...");
        try
        {

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // 1️⃣ Declare a fanout exchange
            await channel.ExchangeDeclareAsync(
                exchange: exchange,
                durable: durable,
                autoDelete: false,
                type: ExchangeType.Topic);

            var orders = GetOrderData();

            foreach (var order in orders)
            {
                string orderJson = JsonSerializer.Serialize(order);
                var body = Encoding.UTF8.GetBytes(orderJson);

                // Publish to the default exchange with routing key == queue name

                var props = new BasicProperties
                {
                    Persistent = true,
                    MessageId = Guid.NewGuid().ToString(),
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                    CorrelationId = Guid.NewGuid().ToString(),
                    Headers = new Dictionary<string, object?>
                                {
                                    { "type", order.Type }
                                }
                };
                
                await channel.BasicPublishAsync(
                    exchange: exchange,
                    routingKey: order.Type,
                    mandatory: false,
                    basicProperties: props,
                    body: body
                );

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
        catch (Exception exx)
        {
            Console.Error.WriteLine($"❌ Connection/Channel error: {exx.Message}");
        }

    }

    public static OrderData[] GetOrderData()
    {
        // Path to your JSON file (e.g., orders.json in project folder)
        string filePath = "data.json";

        if (!File.Exists(filePath))
        {
            Console.WriteLine($"File not found: {filePath}");
            return [];
        }

        // Read the JSON file
        string jsonString = File.ReadAllText(filePath);

        // Deserialize JSON into an array of OrderData
        OrderData[]? orders = JsonSerializer.Deserialize<OrderData[]>(jsonString);

        if (orders == null || orders.Length == 0)
        {
            Console.WriteLine("No data found in JSON file.");
            return [];
        }

        // Print data to console
        foreach (var order in orders)
        {
            Console.WriteLine($"Order ID: {order.Id}, Type: {order.Type}");
        }

        return orders;
    }
}

public class OrderData
{
    [JsonPropertyName("order-id")]
    public required int Id { get; set; }
    [JsonPropertyName("type")]
    public required string Type { get; set; }
}