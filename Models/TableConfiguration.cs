using Newtonsoft.Json;
using SQLDataGenerator.Helpers;

namespace SQLDataGenerator.Models;

public record TableConfiguration(int NumberOfRows, [JsonConverter(typeof(ValidValuesConverter))]Dictionary<string, List<object>> ValidValues);