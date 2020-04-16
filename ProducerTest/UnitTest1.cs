using Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using System;
using System.Collections.Generic;
using System.Threading;
using Xunit;
using 一个消费者订阅多个主题;

namespace ProducerTest
{
	public class UnitTest1
	{
		[Fact]
		public void Test1()
		{
			ConsoleLoggerOptions consoleLoggerOptions = new ConsoleLoggerOptions();
			MyOptionsMonitor<ConsoleLoggerOptions> optionsMonitor = new MyOptionsMonitor<ConsoleLoggerOptions>(consoleLoggerOptions);
			ConsoleLoggerProvider loggerProvider = new ConsoleLoggerProvider(optionsMonitor);
			var f = new LoggerFactory();
			f.AddProvider(loggerProvider);

			string brokerList = "192.168.1.112:9092,192.168.1.112:9093";
			KfkManagement kfkManagement = new KfkManagement(brokerList, f);
			List<string> topics = new List<string>() { "test000", "zjmtest0" };
			topics.Clear();
			string topicName = "zjmtestw";
			for (int i = 0; i < 50; i++)
			{
				topics.Add(topicName + i.ToString());
			}

			int p = 0;
			int num = 100000;
			foreach (var topic in topics)
			{
				for (int j = 0; j < num; j++)
				{
					kfkManagement.Produce(topic,p.ToString());
				}
			}
			kfkManagement.Flush();
		}
	}
}
