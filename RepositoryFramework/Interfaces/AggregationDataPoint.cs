using System;
using System.Collections.Generic;
using System.Text;

namespace RepositoryFramework.Interfaces
{
    public class AggregationDataPoint
    {
        public DateTime Timestamp { get; set; }
        public IList<AggregationValue> Values { get; set; }
    }
}
