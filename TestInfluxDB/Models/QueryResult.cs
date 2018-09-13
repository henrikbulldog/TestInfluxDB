using System;
using System.Collections.Generic;
using System.Text;

namespace TestInfluxDB.Models
{
    public class QueryResult
    {
        public string Error { get; set; }
        public Result[] Results { get; set; }
    }
}
