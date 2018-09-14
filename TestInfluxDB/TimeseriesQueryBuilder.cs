using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace TestInfluxDB
{
    public class TimeseriesQueryBuilder<T> : TimeseriesBuilder<T>
    {
        public string Where { get; private set; } = "";
        public DateTime From { get; private set; } = DateTime.MinValue;
        public DateTime To { get; private set; } = DateTime.MaxValue;
        public TimeInterval TimeInterval { get; private set; } = TimeInterval.Raw;
        public AggregationFunction AggregationFunction { get; private set; } = AggregationFunction.Mean;

        public TimeseriesQueryBuilder(Expression<Func<T, object>> timestampExpression)
            : base(timestampExpression)
        {
        }

        public TimeseriesQueryBuilder<T> SetFrom(DateTime from)
        {
            From = from;
            return this;
        }

        public TimeseriesQueryBuilder<T> SetTo(DateTime to)
        {
            To = to;
            return this;
        }

        public TimeseriesQueryBuilder<T> SetTimeInterval(TimeInterval timeInterval)
        {
            TimeInterval = timeInterval;
            return this;
        }

        public TimeseriesQueryBuilder<T> SetAggregationFunction(AggregationFunction aggregationFunction)
        {
            AggregationFunction = aggregationFunction;
            return this;
        }

        public TimeseriesQueryBuilder<T> SetWhere(string where)
        {
            Where = $"{where}";
            return this;
        }

        public string GetQuery()
        {
            string interval = GetTimeIntervalAsString();
            var isAggregate = !string.IsNullOrEmpty(interval);
            string groupBy = "";
            if(!string.IsNullOrEmpty(interval))
            {
                groupBy = $" group by time(1{interval})";
            }

            return $"select {GetSelect(isAggregate)} from {typeof(T).Name}{GetWhere()}{groupBy}";
        }

        private object GetWhere()
        {
            string where = null;
            if (!string.IsNullOrEmpty(Where))
            {
                if (string.IsNullOrEmpty(where))
                {
                    where = " where ";
                }
                where += Where;
            }

            if (From != DateTime.MinValue)
            {
                if (string.IsNullOrEmpty(where))
                {
                    where = " where ";
                }
                else
                {
                    where += " and ";
                }
                where += $"time >= '{From.ToString("o")}'";
            }

            if (To != DateTime.MaxValue)
            {
                if (string.IsNullOrEmpty(where))
                {
                    where = " where ";
                }
                else
                {
                    where += " and ";
                }
                where += $"time <= '{To.ToString("o")}'";
            }

            return where;
        }

        private string GetSelect(bool isAggregate)
        {
            if(!isAggregate)
            {
                return "*";
            }
            string function = null;
            switch(AggregationFunction)
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
            return string.Join(" , ", typeof(T).GetProperties()
                .Where(p => p.PropertyType.IsValueType && p.Name != Timestamp)
                .Select(p => $"{function}({p.Name}) as {p.Name}"));
        }

        private string GetTimeIntervalAsString()
        {
            string interval = null;
            switch (TimeInterval)
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
    }
}
