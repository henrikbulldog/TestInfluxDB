using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace TestInfluxDB
{
    public abstract class TimeseriesBuilder<T>
    {
        public string Timestamp { get; private set; }

        public TimeseriesBuilder(Expression<Func<T, object>> timestampExpression)
        {
            Timestamp = ExpressionHelper.GetPropertyName(timestampExpression);
        }
    }
}