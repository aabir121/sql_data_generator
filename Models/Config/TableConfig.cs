using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SQLDataGenerator.Helpers;

namespace SQLDataGenerator.Models.Config;

public class TableConfig
{
    [JsonProperty("name")]
    public string Name { get;set; }

    [JsonProperty("numberOfRows")]
    public int NumberOfRows { get; set; }

    [JsonConverter(typeof(ValidValuesConverter))]
    [JsonProperty("validValues")]
    public Dictionary<string, List<object>> ValidValues { get; set; }   
}
