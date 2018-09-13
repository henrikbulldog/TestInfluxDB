﻿using System;
using System.Collections.Generic;
using System.Text;

namespace TestInfluxDB.Models
{
    public class Serie
    {
        public Serie()
        {
            Tags = new Dictionary<string, string>();
            Columns = new string[] { };
            Values = new object[][] { };
        }

        private Serie(string name)
        {
            Name = name;
        }

        public string Name { get; set; }
        public Dictionary<string, string> Tags { get; set; }
        public string[] Columns { get; set; }
        public object[][] Values { get; set; }
    }
}
