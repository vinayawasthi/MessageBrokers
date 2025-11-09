
using Confluent.Kafka;
using System;


namespace Kafka.Consumer;

internal class Program
{

    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "192.168.1.11:9093",
            GroupId = "dotnet-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("order-topic");

        Console.WriteLine("Consuming messages from Kafka...");
        try
        {
            while (true)
            {
                var cr = consumer.Consume();
                var type = cr.Message.Headers.Where(x => x.Key == "type").Select(x => x.Key).FirstOrDefault();
                Console.WriteLine($"{type} => Consumed message '{cr.Message.Key}' => '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'");
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }

}
