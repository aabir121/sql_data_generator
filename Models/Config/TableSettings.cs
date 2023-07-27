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
    public TableFilter TableFilter { get; set; }

    [JsonProperty("config")]
    public List<TableConfig> Config { get; set; }
}
