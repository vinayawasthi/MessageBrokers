using System.Reflection.PortableExecutable;
using System.Text.Json.Serialization;
using System.Text.Json;
using System.Text;
using Confluent.Kafka;

namespace Kafka.Producer;


internal class Program
{

    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "192.168.1.11:9093" // Kafka broker address
        };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
            var orders = GetOrderData();
            foreach (var order in orders)
            {
                string orderJson = JsonSerializer.Serialize(order);
                var body = Encoding.UTF8.GetBytes(orderJson);
                var result = await producer.ProduceAsync("order-topic", new Message<string, string>
                {
                    Key = order.Id.ToString(),
                    Value = orderJson,
                    Headers = new Headers()
                    {
                        { "type", System.Text.Encoding.UTF8.GetBytes(order.Type) },
                        { "correlationId", System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) },
                        { "content-type", System.Text.Encoding.UTF8.GetBytes("application/json") }
                    }
                });
                Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
            }
        }

        //Console.WriteLine("Enter messages to send to Kafka (type 'exit' to quit):");
        //string? message;
        //while ((message = Console.ReadLine()) != "exit")
        //{
        //    var result = await producer.ProduceAsync("order-topic", new Message<string, string> { Value = message });
        //    Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
        //}
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
    public int Id { get; set; }
    [JsonPropertyName("type")]
    public string Type { get; set; }
}