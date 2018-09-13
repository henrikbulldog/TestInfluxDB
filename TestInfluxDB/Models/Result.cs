using System;
using System.Collections.Generic;
using System.Text;

namespace TestInfluxDB.Models
{
    public class Result
    {
        public string Error { get; set; }
        public Serie[] Series { get; set; }
    }
}
