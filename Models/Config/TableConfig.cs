using Newtonsoft.Json;
using SQLDataGenerator.Helpers;

namespace SQLDataGenerator.Models.Config;

public class TableConfig
{
    public TableConfig(string name, int numberOfRows, Dictionary<string, List<object>> validValues)
    {
        Name = name;
        NumberOfRows = numberOfRows;
        ValidValues = validValues;
    }

    [JsonProperty("name")]
    public string Name { get;set; }

    [JsonProperty("numberOfRows")]
    public int NumberOfRows { get; set; }

    [JsonConverter(typeof(ValidValuesConverter))]
    [JsonProperty("validValues")]
    public Dictionary<string, List<object>> ValidValues { get; set; }   
}
