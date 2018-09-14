using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace TestInfluxDB
{
    public class ExpressionHelper
    {
        public static string GetPropertyName<T>(Expression<Func<T, object>> propertyExpression)
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
