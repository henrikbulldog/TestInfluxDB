using System;
using System.Collections.Generic;
using System.Text;

namespace RepositoryFramework.Interfaces
{
    public class AggregationTimeseriesData
    {
        public string Source { get; set; }

        public string Tag { get; set; }

        public IEnumerable<dynamic> DataPoints { get; set; }
    }
}
