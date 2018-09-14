using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
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
            Console.WriteLine("Writing ...");
            var hostname = Environment.GetEnvironmentVariable("COMPUTERNAME");
            var start = DateTime.UtcNow.AddMinutes(-3);
            var ram = Process.GetCurrentProcess().WorkingSet64;
            var l = new List<ComputerInfo>();
            var r = new Random();
            for (int i=0; i<200;i++)
            {
                l.Add(new ComputerInfo
                {
                    Timestamp = start.AddSeconds(i),
                    CPU = r.NextDouble(),
                    RAM = ram,
                    Host = hostname,
                    Region = "WEU"
                });
            };

            var dataPointBuilder = new TimeseriesDataPointBuilder<ComputerInfo>(ci => ci.Timestamp)
                .AddField(ci => ci.Host)
                .AddField(ci => ci.Region)
                .AddTag(ci => ci.RAM)
                .AddTag(ci => ci.CPU);
            await Write(l, dataPointBuilder);

            var queryBuilder = new TimeseriesQueryBuilder<ComputerInfo>(ci => ci.Timestamp)
                .SetWhere($"Host = '{hostname}'")
                .SetFrom(DateTime.UtcNow.AddMinutes(-2))
                .SetTo(DateTime.UtcNow)
                .SetTimeInterval(TimeInterval.Minute)
                .SetAggregationFunction(AggregationFunction.Mean);
            Console.WriteLine(queryBuilder.GetQuery());
            var list = Read(queryBuilder);
            Console.WriteLine(JsonConvert.SerializeObject(
                list, 
                new JsonSerializerSettings
                {
                    Formatting =Formatting.Indented,
                    NullValueHandling =NullValueHandling.Ignore
                }));

            DeleteAll<ComputerInfo>();
        }

        private static void DeleteAll<T>()
        {
            var readRequest = new RestRequest("query", Method.POST)
                .AddQueryParameter("q", $"delete from {typeof(T).Name}")
                .AddQueryParameter("db", "datahub");
            var client = new RestClient("http://localhost:8086");
            var r = client.Execute(readRequest);
            if (!r.IsSuccessful)
            {
                Console.Error.WriteLine(r.ErrorMessage);
                return;
            }
        }

        private static async Task Write<T>(
            IEnumerable<T> l, 
            TimeseriesDataPointBuilder<T> builder)
        {
            var payload = new LineProtocolPayload();
            foreach (var t in l)
            {
                payload.Add(Object2LineProtocolPoint(t, builder));
            }
            var client = new LineProtocolClient(new Uri("http://localhost:8086"), "datahub");
            var influxResult = await client.WriteAsync(payload);
            Console.WriteLine($"Success: {influxResult.Success} {influxResult.ErrorMessage}");
        }

        private static LineProtocolPoint Object2LineProtocolPoint<T>(
            T t,
            TimeseriesDataPointBuilder<T> builder = null)
        {
            var fields = new Dictionary<string, object>();
            var tags = new Dictionary<string, string>();
            var ts = (DateTime)typeof(T).GetProperty(builder.Timestamp).GetValue(t);
            foreach (var field in builder.Fields)
            {
                fields.Add(field, typeof(T).GetProperty(field).GetValue(t));
            }
            foreach (var tag in builder.Tags)
            {
                fields.Add(tag, typeof(T).GetProperty(tag).GetValue(t));
            }

            var p = new LineProtocolPoint(typeof(T).Name, fields, tags, ts);
            return p;
        }

        private static IEnumerable<T> Read<T>(TimeseriesQueryBuilder<T> builder)
            where T : class, new()
        {
            var readRequest = new RestRequest("query", Method.GET)
                .AddQueryParameter("q", builder.GetQuery())
                .AddQueryParameter("db", "datahub");
            var client = new RestClient("http://localhost:8086");
            var r = client.Execute(readRequest);
            if (!r.IsSuccessful)
            {
                throw new Exception(r.ErrorMessage);
            }

            var qr = JsonConvert.DeserializeObject<QueryResult>(r.Content);

            if(!string.IsNullOrEmpty(qr.Results[0].Error))
            {
                throw new Exception(qr.Results[0].Error);
            }

            var serie = qr.Results[0].Series[0];
            return Deserialize<T>(serie.Columns, serie.Values, builder.Timestamp);
        }

        private static IEnumerable<T> Deserialize<T>(
            string[] columns, 
            object[][] values, 
            string timeProperty = null)
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
                    if(propertyInfo != null && value.Length > i && value[i] != null)
                        propertyInfo.SetValue(t, Convert.ChangeType(value[i], propertyInfo.PropertyType), null);
                }
                r.Add(t);
            }
            return r;
        }
    }
}
