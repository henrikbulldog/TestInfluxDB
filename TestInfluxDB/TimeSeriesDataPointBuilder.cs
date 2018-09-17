using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace TestInfluxDB
{
    public class TimeseriesDataPointBuilder<T> : TimeseriesBuilder<T>
    {
        public List<string> Fields { get; } = new List<string>();
        public List<string> Tags { get; } = new List<string>();

        public TimeseriesDataPointBuilder(Expression<Func<T, object>> timestampExpression)
            : base(timestampExpression)
        {
        }

        public TimeseriesDataPointBuilder<T> AddField(Expression<Func<T, object>> fieldExpressoin)
        {
            Fields.Add(ExpressionHelper.GetPropertyName(fieldExpressoin));
            return this;
        }

        public TimeseriesDataPointBuilder<T> AddTag(Expression<Func<T, object>> tagExpression)
        {
            Tags.Add(ExpressionHelper.GetPropertyName(tagExpression));
            return this;
        }
    }
}
