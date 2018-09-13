using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TestInfluxDB.Models;

namespace TestInfluxDB
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Run(() => MainAsync(args)).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            await WriteLineProtocolAsync();
            ReadRest();
            DeleteRest();
        }

        private static void DeleteRest()
        {
            var readRequest = new RestRequest("query", Method.POST)
                .AddQueryParameter("q", "delete from ComputerInfo")
                .AddQueryParameter("db", "datahub");
            var client = new RestClient("http://localhost:8086");
            var r = client.Execute(readRequest);
            if (!r.IsSuccessful)
            {
                Console.Error.WriteLine(r.ErrorMessage);
                return;
            }
        }

        private static async Task WriteLineProtocolAsync()
        {
            var payload = new LineProtocolPayload();
            payload.Add(new LineProtocolPoint(
                "ComputerInfo",
                new Dictionary<string, object>
                {
                    { "CPU", 12.345 },
                    { "RAM", Process.GetCurrentProcess().WorkingSet64 },
                },
                new Dictionary<string, string>
                {
                    { "Host", Environment.GetEnvironmentVariable("COMPUTERNAME") },
                    { "Region", "WEU" }
                },
                DateTime.UtcNow));
            Thread.Sleep(1);
            payload.Add(new LineProtocolPoint(
                "ComputerInfo",
                new Dictionary<string, object>
                {
                    { "CPU", 23.455 },
                    { "RAM", Process.GetCurrentProcess().WorkingSet64 },
                },
                new Dictionary<string, string>
                {
                    { "Host", Environment.GetEnvironmentVariable("COMPUTERNAME") },
                    { "Region", "WEU" }
                },
                DateTime.UtcNow));

            var client = new LineProtocolClient(new Uri("http://localhost:8086"), "datahub");
            var influxResult = await client.WriteAsync(payload);
            Console.WriteLine($"Success: {influxResult.Success} {influxResult.ErrorMessage}");
        }

        private static void ReadRest()
        {
            var list = ReadRest<ComputerInfo>("Timestamp");
            Console.WriteLine(JsonConvert.SerializeObject(list, Formatting.Indented));
        }

        private static IEnumerable<T> ReadRest<T>(string timeProperty)
            where T : class, new()
        {
            var readRequest = new RestRequest("query", Method.GET)
                .AddQueryParameter("q", $"select * from {typeof(T).Name}")
                .AddQueryParameter("db", "datahub");
            var client = new RestClient("http://localhost:8086");
            var r = client.Execute(readRequest);
            if (!r.IsSuccessful)
            {
                throw new Exception(r.ErrorMessage);
            }

            var qr = JsonConvert.DeserializeObject<QueryResult>(r.Content);
            var serie = qr.Results[0].Series[0];
            return Deserialize<T>(serie.Columns, serie.Values, timeProperty);
        }

        private static IEnumerable<T> Deserialize<T>(string[] columns, object[][] values, string timeProperty)
            where T : class, new()
        {
            var r = new List<T>();
            foreach(var value in values)
            {
                var t = new T();
                for (int i=0; i<columns.Length; i++)
                {
                    PropertyInfo propertyInfo = t.GetType().GetProperty(columns[i]);
                    if (columns[i] == "time")
                    {
                        propertyInfo = t.GetType().GetProperty(timeProperty);
                    }
                    if(propertyInfo != null)
                        propertyInfo.SetValue(t, Convert.ChangeType(value[i], propertyInfo.PropertyType), null);
                }
                r.Add(t);
            }
            return r;
        }
    }
}
