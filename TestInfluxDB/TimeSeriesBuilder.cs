using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace TestInfluxDB
{
    public class TimeSeriesBuilder<T>
    {
        public string Timestamp { get; private set; } = null;
        public List<string> Fields { get; } = new List<string>();
        public List<string> Tags { get; } = new List<string>();

        public TimeSeriesBuilder<T> SetTimestamp(Expression<Func<T, object>> timestamp)
        {
            Timestamp = GetPropertyName(timestamp);
            return this;
        }

        public TimeSeriesBuilder<T> AddField(Expression<Func<T, object>> field)
        {
            Fields.Add(GetPropertyName(field));
            return this;
        }

        public TimeSeriesBuilder<T> AddTag(Expression<Func<T, object>> tag)
        {
            Tags.Add(GetPropertyName(tag));
            return this;
        }

        private static string GetPropertyName<T>(Expression<Func<T, object>> propertyExpression)
        {
            if (propertyExpression == null)
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
    }
}
