using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RepositoryFramework.Interfaces
{
    public interface ITimeseriesRepository : ICreate<Timeseries>, ICreateAsync<Timeseries>
    {
        IEnumerable<Timeseries> Find(
            IList<string> tags,
            string source = null,
            DateTime? from = null,
            DateTime? to = null);

        Task<IEnumerable<Timeseries>> FindAsync(
            IList<string> tags,
            string source = null,
            DateTime? from = null,
            DateTime? to = null);

        IEnumerable<AggregationTimeseriesData> FindAggregate(
            IList<string> tags,
            TimeInterval timeInterval,
            IList<AggregationFunction> aggregationFunctions = null,
            string source = null,
            DateTime? from = null,
            DateTime? to = null);

        Task<IEnumerable<AggregationTimeseriesData>> FindAggregateAsync(
            IList<string> tags,
            TimeInterval timeInterval,
            IList<AggregationFunction> aggregationFunctions = null,
            string source = null,
            DateTime? from = null,
            DateTime? to = null);
    }
}
