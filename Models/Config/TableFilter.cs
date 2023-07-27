using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataGenerator.Models.Config;

public class TableFilter
{
    [JsonProperty("mode")]
    public FilterMode FilterMode { get; set; }

    [JsonProperty("values")]
    public HashSet<string> Values { get; set; }

}
