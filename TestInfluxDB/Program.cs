using Newtonsoft.Json;
using RepositoryFramework.Interfaces;
using RepositoryFramework.Timeseries.InfluxDB;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Threading.Tasks;

namespace TestInfluxDB
{
    class Program
    {
        static void Main(string[] args)
        {
            dynamic ex = new ExpandoObject();
            ex.p1 = 1;
            (ex as IDictionary<string, Object>).Add("NewProp", string.Empty);

            Task.Run(() => MainAsync(args)).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine("Writing ...");
            var hostname = Environment.GetEnvironmentVariable("COMPUTERNAME");
            var start = DateTime.UtcNow.AddMinutes(-10);
            var ram = Process.GetCurrentProcess().WorkingSet64;
            var r = new Random();

            var repo = new InfluxDBRepository(new Uri("http://localhost:8086"), "datahub", "ComputerInfo");
            var sources = new List<string>() { "CUS", "WEU" };
            var tags = new List<string>() { "CPU", "RAM" };
            foreach (var tag in tags)
            {
                foreach (var source in sources)
                {
                    var timeseriesData = new Timeseries
                    {
                        Tag = tag,
                        Source = source,
                        DataPoints = new List<DataPoint>()
                    };
                    for (int i = 0; i < 3; i++)
                    {
                        timeseriesData.DataPoints.Add(new DataPoint
                        {
                            Timestamp = start.AddSeconds(i*30),
                            Value = tag.Contains("CPU") ? r.NextDouble() : ram
                        });
                    }
                    await repo.CreateAsync(timeseriesData);
                }
            }

            Console.WriteLine("Raw data filtered by tags [RAM, CPU], Source CUS, and time now - 10 mins until now");
            Console.WriteLine(JsonConvert.SerializeObject(
                await repo.FindAsync(new List<string> { "RAM", "CPU" }, "CUS", start, DateTime.UtcNow),
                new JsonSerializerSettings
                {
                    Formatting = Formatting.Indented,
                    NullValueHandling = NullValueHandling.Ignore
                }));

            Console.WriteLine("Aggregated data by 1 minute:");
            var findResult = await repo.FindAggregateAsync(
                new List<string> { "RAM", "CPU" }, 
                TimeInterval.Minute,
                new List<AggregationFunction>
                {
                    AggregationFunction.Count,
                    AggregationFunction.Mean,
                    AggregationFunction.Spread
                });
            Console.WriteLine(JsonConvert.SerializeObject(
                findResult,
                new JsonSerializerSettings
                {
                    Formatting = Formatting.Indented,
                    NullValueHandling = NullValueHandling.Ignore
                }));

            DeleteAll("ComputerInfo");
        }

        private static void DeleteAll(string tag)
        {
            var readRequest = new RestRequest("query", Method.POST)
                .AddQueryParameter("q", $"delete from {tag}")
                .AddQueryParameter("db", "datahub");
            var client = new RestClient("http://localhost:8086");
            var r = client.Execute(readRequest);
            if (!r.IsSuccessful)
            {
                Console.Error.WriteLine(r.ErrorMessage);
                return;
            }
        }
    }
}
