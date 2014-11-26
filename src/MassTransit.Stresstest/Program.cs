// Copyright 2007-2014 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

namespace MassTransit.Stresstest
{
	using System;
	using System.Collections.Generic;
	using System.Threading;
	using System.Threading.Tasks;
	using BusConfigurators;

	class Program
	{
		private const int Threads = 75;
		private static readonly TimeSpan Duration = TimeSpan.FromSeconds(600);
		internal static DateTime mStartTime;

		static void Main(string[] args)
		{
			int workerThreads, complete;
			ThreadPool.GetMinThreads(out workerThreads, out complete);
			ThreadPool.SetMinThreads(Threads + 20, complete);

			var responderBus = ServiceBusFactory.New(sbc =>
			{
				BasicBusConfiguration(sbc, "res");
				sbc.Subscribe(subs => subs.Handler<Request>((cxt, msg) => cxt.Respond(new Response { CorrelationId = msg.CorrelationId })));
			});

			var requestorBus =
				ServiceBusFactory.New(sbc => BasicBusConfiguration(sbc, "req"));

			var senders = new List<Task>();

			mStartTime = DateTime.Now;
			var endTime = mStartTime.Add(Duration);
			for (var i = 0; i < Threads; i++)
			{
				senders.Add(Task.Factory.StartNew(() => StartSender(endTime, requestorBus)));
				Console.WriteLine("Started: " + i);
			}

			Task.WaitAll(senders.ToArray());
			responderBus.Dispose();
			requestorBus.Dispose();
		}


		static void BasicBusConfiguration(ServiceBusConfigurator sbc, string queue)
		{
			var uriString = "rabbitmq://localhost:5672/" + queue + "?temporary=true";
			sbc.UseRabbitMq(r => r.ConfigureHost(new Uri(uriString), h =>
			{
				h.SetUsername("guest");
				h.SetPassword("guest");
			}));
			sbc.ReceiveFrom(uriString);
		}

		static void StartSender(DateTime endTime, IServiceBus requestorBus)
		{
			while (DateTime.Now.CompareTo(endTime) < 0)
			{
				var nextMessage = Request.Next();
				requestorBus.PublishRequest(nextMessage, x =>
				{
					x.Handle<Response>(msg =>
					{
						if (nextMessage.CorrelationId != msg.CorrelationId)
						{
							var errorMesssage = String.Format("\nMessage have different value. Expected: {0}, Actual: {1}\t{2}", nextMessage.CorrelationId,
								msg.CorrelationId, TimeSinceStart());
							Console.WriteLine(errorMesssage);
						}
					});
				});
			}
		}

		internal static TimeSpan TimeSinceStart()
		{
			return DateTime.Now.Subtract(mStartTime);
		}
	}

	[Serializable]
	public class Request : CorrelatedBy<int>
	{
		private const int DotFrequency = 2000;
		static int _current;
		public int CorrelationId { get; set; }

		public static Request Next()
		{
			var next = Interlocked.Increment(ref _current);
			if (next%DotFrequency == 0)
			{
				Console.Write(".");
				if (next%(50*DotFrequency) == 0)
				{
					Console.WriteLine(Program.TimeSinceStart());
				}
			}
			return new Request {CorrelationId = next};
		}
	}

	[Serializable]
	public class Response : CorrelatedBy<int>
	{
		public int CorrelationId { get; set; }
	}
}