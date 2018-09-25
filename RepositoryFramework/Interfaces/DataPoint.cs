using System;
using System.Collections.Generic;
using System.Text;

namespace RepositoryFramework.Interfaces
{
    public class DataPoint
    {
        public DateTime Timestamp { get; set; }
        public object Value { get; set; }
    }
}
