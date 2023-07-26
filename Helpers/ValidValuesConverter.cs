using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SQLDataGenerator.Helpers;

public class ValidValuesConverter : JsonConverter
{
    public override bool CanConvert(Type objectType)
    {
        return objectType == typeof(Dictionary<string, List<object>>);
    }

    public override object ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
    {
        var validValues = new Dictionary<string, List<object>?>();
        var jsonObject = JObject.Load(reader);

        foreach (var property in jsonObject.Properties())
        {
            var propertyName = property.Name;
            var propertyValue = property.Value;

            if (propertyValue.Type == JTokenType.String && (((string)propertyValue)!).Contains('-'))
            {
                // Parse the range if the value contains a hyphen
                var rangeValues = ((string)propertyValue!)?.Split('-');
                int minValue = int.Parse(rangeValues?[0] ?? string.Empty, CultureInfo.InvariantCulture);
                int maxValue = int.Parse(rangeValues?[1] ?? string.Empty, CultureInfo.InvariantCulture);
                validValues[propertyName] =  Enumerable.Range(minValue, maxValue - minValue + 1).Select(v => (object)v).ToList();;
            }
            else if (propertyValue.Type == JTokenType.Array)
            {
                // Directly add the array of strings to valid values
                validValues[propertyName] = propertyValue.ToObject<List<object>>();
            }
        }

        return validValues;
    }

    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}
