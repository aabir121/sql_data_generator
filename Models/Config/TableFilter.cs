using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class TableFilter
{
    [JsonProperty("mode")]
    public FilterMode FilterMode { get; set; }

    [JsonProperty("values")]
    public HashSet<string> Values { get; set; }

}
