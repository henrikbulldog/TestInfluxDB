using System;
using System.Collections.Generic;
using System.Text;

namespace TestInfluxDB
{
    public class ComputerInfo
    {
        public DateTime Timestamp { get; set; }

        public string Host { get; set; }

        public string Region { get; set; }

        public double CPU { get; set; }

        public long RAM { get; set; }
    }
}
