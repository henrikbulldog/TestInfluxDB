using System;
using System.Collections.Generic;
using System.Text;

namespace TestInfluxDB
{
    public enum AggregationFunction
    {
        Count,
        Distinct,
        Integral,
        Mean,
        Median,
        Mode,
        Spread,
        Stddev,
        Sum
    }
}
