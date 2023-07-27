using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataGenerator.Models.Config;

public class TableSettings
{
    [JsonProperty("filter")]
    public Filter Filter { get; set; }

    [JsonProperty("config")]
    public List<Config> Config { get; set; }
}
