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

            await Write(l, ci => ci.Timestamp);

            var list = Read<ComputerInfo>(ci => ci.Timestamp);
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
            Expression<Func<T, object>> timeProperty = null)
        {
            var payload = new LineProtocolPayload();
            foreach (var t in l)
            {
                payload.Add(Object2LineProtocolPoint(t, timeProperty));
            }
            var client = new LineProtocolClient(new Uri("http://localhost:8086"), "datahub");
            var influxResult = await client.WriteAsync(payload);
            Console.WriteLine($"Success: {influxResult.Success} {influxResult.ErrorMessage}");
        }

        private static LineProtocolPoint Object2LineProtocolPoint<T>(
            T t, 
            Expression<Func<T, object>> timeProperty = null)
        {
            var fields = new Dictionary<string, object>();
            var tags = new Dictionary<string, string>();
            var ts = DateTime.Now;
            foreach (var property in typeof(T).GetProperties())
            {
                if (property.Name == GetPropertyName<T>(timeProperty))
                {
                    ts = (DateTime)property.GetValue(t);
                }
                else if (property.PropertyType.IsValueType)
                {
                    fields.Add(property.Name, property.GetValue(t));
                }
                else
                {
                    tags.Add(property.Name, property.GetValue(t).ToString());
                }
            }

            var p = new LineProtocolPoint(typeof(T).Name, fields, tags, ts);
            return p;
        }

        private static string GetPropertyName<T>(Expression<Func<T, object>> propertyExpression)
        {
            if(propertyExpression == null)
            {
                return null;
            }

            var body = propertyExpression.Body as MemberExpression;

            if (body != null)
            {
                return body.Member.Name;
            }

            var ubody = (UnaryExpression)propertyExpression.Body;
            body = ubody.Operand as MemberExpression;

            return body?.Member.Name ?? string.Empty;
        }

        private static IEnumerable<T> Read<T>(Expression<Func<T, object>> timeProperty = null)
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
            Expression<Func<T, object>> timeProperty = null)
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
                        propertyInfo = t.GetType().GetProperty(GetPropertyName<T>(timeProperty));
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
