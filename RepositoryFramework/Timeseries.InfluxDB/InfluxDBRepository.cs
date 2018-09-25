﻿using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;
using Newtonsoft.Json;
using RepositoryFramework.Interfaces;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;

namespace RepositoryFramework.Timeseries.InfluxDB
{
    public class InfluxDBRepository : ITimeseriesRepository
    {
        private Uri uri;
        private string database;
        private string measurement;

        public InfluxDBRepository(Uri uri, string database, string measurement)
        {
            this.uri = uri;
            this.database = database;
            this.measurement = measurement;
        }

        public void Create(Interfaces.Timeseries data)
        {
            CreateAsync(data).WaitSync();
        }

        public async Task CreateAsync(Interfaces.Timeseries timeseries)
        {
            if (timeseries == null || timeseries.DataPoints == null)
            {
                return;
            }
            var payload = new LineProtocolPayload();
            foreach (var point in timeseries.DataPoints)
            {
                var fields = new Dictionary<string, object>();
                fields.Add("Value", point.Value);
                var tags = new Dictionary<string, string>();
                tags.Add("Tag", timeseries.Tag);
                tags.Add("Source", timeseries.Source);
                payload.Add(new LineProtocolPoint(measurement, fields, tags, point.Timestamp));
            }
            var client = new LineProtocolClient(uri, database);
            var influxResult = await client.WriteAsync(payload);
            if (!influxResult.Success)
            {
                new Exception(influxResult.ErrorMessage);
            }
        }

        public IEnumerable<Interfaces.Timeseries> Find(IList<string> tags, string source = null, DateTime? from = null, DateTime? to = null)
        {
            var task = FindAsync(tags, source, from, to);
            task.WaitSync();
            return task.Result;
        }

        public async Task<IEnumerable<Interfaces.Timeseries>> FindAsync(IList<string> tags, string source = null, DateTime? from = null, DateTime? to = null)
        {
            var where = "";
            if (source != null)
            {
                where = $" where Source = '{source}'";
            }

            if (from != null)
            {
                where += string.IsNullOrEmpty(where) ? " where " : " and ";
                where += $"time >= '{from.Value.ToString("o")}'";
            }

            if (to != null)
            {
                where += string.IsNullOrEmpty(where) ? " where " : " and ";
                where += $"time <= '{to.Value.ToString("o")}'";
            }

            AddTagsFilter(tags, where);
            var readRequest = new RestRequest("query", Method.GET)
                .AddQueryParameter("q", $"select * from {measurement}{where}")
                .AddQueryParameter("db", database);
            var r = await CallApiAsync(readRequest);
            if (r.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception(r.StatusDescription);
            }

            var qr = JsonConvert.DeserializeObject<InfluxDBQueryResult>(r.Content);
            if (!string.IsNullOrEmpty(qr.Results[0].Error))
            {
                throw new Exception(qr.Results[0].Error);
            }

            return Deserialize(qr.Results[0]);
        }

        private void AddTagsFilter(IList<string> tags, string where)
        {
            if (tags.Count() > 0)
            {
                var tagsFilter = "";
                foreach (var tag in tags)
                {
                    tagsFilter += string.IsNullOrEmpty(tagsFilter) ? "" : " or ";
                    tagsFilter += $"\"Tag\" = '{tag}'";
                }
                where += string.IsNullOrEmpty(where) ? " where " : " and " + $"({tagsFilter})";
            }
        }

        public IEnumerable<AggregationTimeseriesData> FindAggregate(IList<string> tags, TimeInterval timeInterval, IList<AggregationFunction> aggregationFunctions = null, string source = null, DateTime? from = null, DateTime? to = null)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<AggregationTimeseriesData>> FindAggregateAsync(
            IList<string> tags,
            TimeInterval timeInterval, IList<AggregationFunction> aggregationFunctions = null,
            string source = null,
            DateTime? from = null,
            DateTime? to = null)
        {
            var where = "";
            if (source != null)
            {
                where = $" where \"Source\" = '{source}'";
            }

            if (from != null)
            {
                where += string.IsNullOrEmpty(where) ? " where " : " and ";
                where += $"time >= '{from.Value.ToString("o")}'";
            }

            if (to != null)
            {
                where += string.IsNullOrEmpty(where) ? " where " : " and ";
                where += $"time <= '{to.Value.ToString("o")}'";
            }

            AddTagsFilter(tags, where);

            string interval = GetTimeIntervalAsString(timeInterval);
            var isAggregate = !string.IsNullOrEmpty(interval);
            string groupBy = " group by \"Tag\", \"Source\"";
            if (!string.IsNullOrEmpty(interval))
            {
                groupBy += $", time(1{interval})";
            }

            var sql = $"select {AggregationFunctionsAsSQL(aggregationFunctions)} from {measurement}{where}{groupBy}";
            var readRequest = new RestRequest("query", Method.GET)
                .AddQueryParameter("q", sql)
                .AddQueryParameter("db", database);
            var r = await CallApiAsync(readRequest);
            if (r.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception(r.StatusDescription);
            }

            var qr = JsonConvert.DeserializeObject<InfluxDBQueryResult>(r.Content);
            if (!string.IsNullOrEmpty(qr.Results[0].Error))
            {
                throw new Exception(qr.Results[0].Error);
            }

            return AggregationDeserialize(qr.Results[0]);
        }

        private IEnumerable<AggregationTimeseriesData> AggregationDeserialize(InfluxDBResult result)
        {
            if (result == null || result.Series == null)
                return null;
            var r = new List<AggregationTimeseriesData>();
            foreach (var serie in result.Series)
            {
                r.Add(new AggregationTimeseriesData
                {
                    Tag = serie.Tags["Tag"],
                    Source = serie.Tags["Source"],
                    DataPoints = serie.Values
                        .Select(v =>
                        {
                            dynamic ex = new ExpandoObject();
                            for (var i=0;i<serie.Columns.Length;i++)
                            {
                                if (serie.Columns[i] == "time")
                                {
                                    (ex as IDictionary<string, Object>).Add("Timestamp", v[i]);
                                }
                                else
                                {
                                    (ex as IDictionary<string, Object>).Add(serie.Columns[i], v[i]);
                                }
                            }
                            return ex;
                        })
                });
            }

            return r;
        }

        private string AggregationFunctionsAsSQL(IEnumerable<AggregationFunction> aggregationFunctions)
        {
            if(aggregationFunctions == null || aggregationFunctions.Count() == 0)
            {
                aggregationFunctions = new List<AggregationFunction> { AggregationFunction.Mean };
            }

            return string.Join(" , ", aggregationFunctions
                .Select(f =>
                {
                    var s = AggregationFunctionAsString(f);
                    return $"{s}(Value) as {s}";
                }));
        }

        private object AggregationFunctionAsString(AggregationFunction aggregationFunction)
        {
            var function = "";
            switch (aggregationFunction)
            {
                case AggregationFunction.Count: function = "count"; break;
                case AggregationFunction.Distinct: function = "distinct"; break;
                case AggregationFunction.Integral: function = "integral"; break;
                case AggregationFunction.Mean: function = "mean"; break;
                case AggregationFunction.Median: function = "median"; break;
                case AggregationFunction.Mode: function = "mode"; break;
                case AggregationFunction.Spread: function = "spread"; break;
                case AggregationFunction.Stddev: function = "stddev"; break;
                case AggregationFunction.Sum: function = "sum"; break;
            }
            return function;
        }

        private string GetTimeIntervalAsString(TimeInterval timeinterval)
        {
            string interval = null;
            switch (timeinterval)
            {
                case TimeInterval.Nanoseconds: interval = "ns"; break;
                case TimeInterval.Microseconds: interval = "u"; break;
                case TimeInterval.Milliseconds: interval = "ms"; break;
                case TimeInterval.Second: interval = "s"; break;
                case TimeInterval.Minute: interval = "m"; break;
                case TimeInterval.Hour: interval = "h"; break;
                case TimeInterval.Day: interval = "d"; break;
                case TimeInterval.Week: interval = "w"; break;
            }
            return interval;
        }

        private Task<IRestResponse> CallApiAsync(IRestRequest request)
        {
            var client = new RestClient(uri);
            var taskCompletionSource = new TaskCompletionSource<IRestResponse>();
            client.ExecuteAsync(request, (r) => taskCompletionSource.SetResult(r));
            return taskCompletionSource.Task;
        }

        private static IEnumerable<Interfaces.Timeseries> Deserialize(
            InfluxDBResult result)
        {
            if (result == null || result.Series == null)
                return null;
            var r = new List<Interfaces.Timeseries>();
            foreach (var serie in result.Series)
            {
                var tagColumn = Array.FindIndex(serie.Columns, c => c == "Tag");
                var sourceColumn = Array.FindIndex(serie.Columns, c => c == "Source");
                var timeColumn = Array.FindIndex(serie.Columns, c => c == "time");
                var valueColumn = Array.FindIndex(serie.Columns, c => c == "Value");

                r.AddRange(serie.Values.GroupBy(v => new
                {
                    Source = v.ElementAt(sourceColumn).ToString(),
                    Tag = v.ElementAt(tagColumn).ToString()
                })
                .Select(g => new Interfaces.Timeseries
                {
                    Tag = g.Key.Tag,
                    Source = g.Key.Source,
                    DataPoints = new List<DataPoint>(g.Select(p => new DataPoint
                    {
                        Timestamp = (DateTime)p.ElementAt(timeColumn),
                        Value = p.ElementAt(valueColumn)
                    }))
                }));
            }

            return r;
        }
    }
}