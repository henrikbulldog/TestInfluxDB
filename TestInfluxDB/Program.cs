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

            var repo = new InfluxDBRepository("http://localhost:8086", "datahub", "ComputerInfo");
            var sources = new List<string>() { "CUS", "WEU" };
            var tags = new List<string>() { "CPU", "RAM" };
            var time = DateTime.Now;
            foreach (var tag in tags)
            {
                foreach (var source in sources)
                {
                    var timeseriesData = new TimeseriesData
                    {
                        Tag = tag,
                        Source = source,
                        DataPoints = new List<DataPoint>()
                    };
                    for (int i = 0; i < 10; i++)
                    {
                        timeseriesData.DataPoints.Add(new DataPoint
                        {
                            Timestamp = start.AddSeconds(i*10),
                            Value = tag.Contains("CPU") ? r.NextDouble() : ram
                        });
                    }
                    repo.Create(timeseriesData);
                }
            }

            Console.WriteLine($"Time: {DateTime.Now - time}");
            repo.Flush();

            Console.WriteLine("Raw data filtered by tags [RAM, CPU], Source CUS and time");
            Console.WriteLine(JsonConvert.SerializeObject(
                await repo.FindAsync(new List<string> { "RAM", "CPU" }, "CUS", start, start.AddMinutes(2)),
                new JsonSerializerSettings
                {
                    Formatting = Formatting.Indented,
                    NullValueHandling = NullValueHandling.Ignore
                }));

            Console.WriteLine("Aggregated data filtered by tags [RAM, CPU], Source CUS and time, 1 minute interval:");
            var findResult = await repo.FindAggregateAsync(
                new List<string> { "RAM", "CPU" },
                TimeInterval.Minute,
                new List<AggregationFunction>
                {
                    AggregationFunction.Count,
                    AggregationFunction.Mean,
                    AggregationFunction.Spread
                },
                "CUS",
                start,
                start.AddMinutes(2));
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
