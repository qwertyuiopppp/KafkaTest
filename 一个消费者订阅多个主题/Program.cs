using Common;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace 一个消费者订阅多个主题
{
	class Program
	{
		

		static void Main(string[] args)
		{
			CancellationTokenSource tokenSource = new CancellationTokenSource();
			var token = tokenSource.Token;
			Console.WriteLine("Hello World!");
			ConsoleLoggerOptions consoleLoggerOptions = new ConsoleLoggerOptions();
			MyOptionsMonitor<ConsoleLoggerOptions> optionsMonitor = new MyOptionsMonitor<ConsoleLoggerOptions>(consoleLoggerOptions);
			ConsoleLoggerProvider loggerProvider = new ConsoleLoggerProvider(optionsMonitor);
			var f = new LoggerFactory();
			f.AddProvider(loggerProvider);
			var logger = f.CreateLogger<Program>();
			string brokerList = "192.168.1.112:9092,192.168.1.112:9093";
			KfkManagement kfkManagement = new KfkManagement(brokerList, f);
			List<string> topics = new List<string>();
			string topicName = "zjmtestw";
			for (int i = 0; i < 50; i++)
			{
				topics.Add(topicName + i.ToString());
			}
			var consumer = kfkManagement.CreateConsumer("clientw", topics);
			ConcurrentDictionary<string, int> kv = new ConcurrentDictionary<string, int>();
			var t = Task.Run(() =>
			{
				while (!token.IsCancellationRequested)
				{
					lock (consumer)
					{
						var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(10));
						if (consumeResult == null)
						{
							continue;
						}
						kv.AddOrUpdate(consumeResult.Topic, 1, (k, v) => Interlocked.Increment(ref v));

						//logger.LogInformation($"topic:{consumeResult.Topic}, offset;:{consumeResult.Offset.ToString()}, {consumeResult.Message.Value}");
					}
				}
			}).ContinueWith((t)=> {
				foreach (var item in kv)
				{
					Console.WriteLine($"{item.Key}:{item.Value}");
				}
				
			});
			while (true)
			{
				string m = Console.ReadLine();
				if (string.IsNullOrWhiteSpace(m)) continue;
				string op = m.Split(" ")[0];
				string topic = m.Split(" ")[1];
				switch (op)
				{

					case "add":
						lock (consumer)
						{
							var mtopics = consumer.Subscription.ToList();
							mtopics.Add(topic);
							consumer.Subscribe(mtopics);
						}
						break;
					case "pause":
						lock (consumer)
						{
							var array = consumer.Assignment.Where(x => x.Topic.Equals(topic)).ToList();
							consumer.Pause(array);
							Console.WriteLine($"pause:{string.Join(",", array)}");
						}
						break;
					case "resume":
						lock (consumer)
						{
							var array = consumer.Assignment.Where(x => x.Topic.Equals(topic)).ToList();
							consumer.Resume(array);
							Console.WriteLine($"pause:{string.Join(",", array)}");
						}
						break;
					case "exit":
						tokenSource.Cancel();
						break;
					default:
						break;
				}
			}

		}


	}
	public class MyOptionsMonitor<T> : IOptionsMonitor<T>
	{
		T t;

		public MyOptionsMonitor(T t)
		{
			this.t = t;
		}

		public T CurrentValue => t;

		public T Get(string name)
		{
			return t;
		}

		public IDisposable OnChange(Action<T, string> listener)
		{
			return null;
		}
	}
}
