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
            var l = new List<ComputerInfo>
            {
                new ComputerInfo
                {
                    Timestamp = DateTime.UtcNow,
                    CPU= 12.345,
                    RAM = Process.GetCurrentProcess().WorkingSet64,
                    Host= Environment.GetEnvironmentVariable("COMPUTERNAME"),
                    Region="WEU"
                },
                new ComputerInfo
                {
                    Timestamp = DateTime.UtcNow.AddMilliseconds(1),
                    CPU = 23.456,
                    RAM = Process.GetCurrentProcess().WorkingSet64,
                    Host = Environment.GetEnvironmentVariable("COMPUTERNAME"),
                    Region = "WEU"
                }
            };

            var builder = new TimeSeriesBuilder<ComputerInfo>()
                .SetTimestamp(ci => ci.Timestamp)
                .AddField(ci => ci.Host)
                .AddField(ci => ci.Region)
                .AddTag(ci => ci.RAM)
                .AddTag(ci => ci.CPU);
            await Write(l, builder);

            var list = Read<ComputerInfo>(builder.Timestamp);
            Console.WriteLine(JsonConvert.SerializeObject(list, Formatting.Indented));

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
            TimeSeriesBuilder<T> builder)
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
            TimeSeriesBuilder<T> builder = null)
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

        private static IEnumerable<T> Read<T>(string timeProperty)
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
                    if(propertyInfo != null)
                        propertyInfo.SetValue(t, Convert.ChangeType(value[i], propertyInfo.PropertyType), null);
                }
                r.Add(t);
            }
            return r;
        }
    }
}
