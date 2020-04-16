using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Common
{
    public class KfkManagement
    {
        string brokerList;
        ProducerConfig producerConfig;
        ILoggerFactory loggerFactory;
        ILogger<KfkManagement> logger;
        public KfkManagement(string brokerList,ILoggerFactory loggerFactory)
        {
            this.brokerList = brokerList;
            this.loggerFactory = loggerFactory;
            logger = loggerFactory.CreateLogger<KfkManagement>();
            producerConfig = new ProducerConfig
            {
                MessageTimeoutMs = 3000,
                BootstrapServers = brokerList,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 100,
                Acks = Acks.Leader,
                BatchNumMessages = 200,
                LingerMs = 10
            };
            producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        IProducer<string, string> producer;

        public void Produce(string topic, string data, string key = "", Action exceptionAct = null)
        {
            Action<DeliveryReport<string, string>> deliveryHandler = (DeliveryReport<string, string> deliveryReport) =>
            {
                if (deliveryReport.Error.IsError)
                {
                    logger.LogError($"deliveryReportError:[{deliveryReport.Error.Code}] message: {deliveryReport.Error.Reason}, topic: {topic}, data: {data}, key: {key}");
                    exceptionAct?.Invoke();
                }

            };

            while (true)
            {
                try
                {
                    var message = new Message<string, string> { Key = key, Value = data };
                    producer.Produce(topic, message, deliveryHandler);
                    break;
                }
                catch (ProduceException<string, string> ke)
                {
                    logger.LogError($"[{ke.Error.Code}] message: {ke.Message}, {ke.StackTrace} topic: {topic}, data: {data}, key: {key}");
                    if (ke.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        producer.Poll(TimeSpan.FromSeconds(1));
                        continue;
                    }
                    exceptionAct?.Invoke();
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError($"message: { ex.Message}, { ex.StackTrace} topic: { topic}, data: { data}, key: { key}");
                    exceptionAct?.Invoke();
                    break;
                }

            }


        }

        public void Produce(string topic, object[] datas, string key = "", Action action = null)
        {
            foreach (var item in datas)
            {
                Produce(topic, item, key, action);
            }
        }


        public void Produce(string topic, object obj, string key = "", Action action = null)
        {
            string data = string.Empty;
            try
            {
                data = System.Text.Json.JsonSerializer.Serialize(obj);
            }
            catch (Exception ex)
            {
                logger.LogError("message: " + ex.Message + ", StackTrace" + ex.StackTrace);
                action?.Invoke();
                return;
            }
            Produce(topic, data, key, action);
        }

        public IConsumer<string, string> CreateConsumer(string topicName, IEnumerable<string> topics)
        {
            var configConsumer = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = topicName + "Consumer",
                EnableAutoCommit = true,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Latest,
                AutoCommitIntervalMs = 100
            };

            var consumer = new ConsumerBuilder<string, string>(configConsumer)
                .SetErrorHandler((_, e) =>
                {
                    logger.LogError($"ConsumerGroupId{topicName + "Consumer"} KfkConsumerError: {e.Reason}");
                })
                .Build();

            consumer.Subscribe(topics);

            return consumer;
        }

        public void Flush() => producer.Flush();
    }
}
