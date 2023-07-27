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

            switch (propertyValue.Type)
            {
                case JTokenType.String when (((string)propertyValue)!).Contains('-'):
                {
                    // Parse the range if the value contains a hyphen
                    var rangeValues = ((string)propertyValue!)?.Split('-');
                    var minValue = int.Parse(rangeValues?[0] ?? string.Empty, CultureInfo.InvariantCulture);
                    var maxValue = int.Parse(rangeValues?[1] ?? string.Empty, CultureInfo.InvariantCulture);
                    validValues[propertyName] =  Enumerable.Range(minValue, maxValue - minValue + 1).Select(v => (object)v).ToList();;
                    break;
                }
                case JTokenType.Array:
                    // Directly add the array of strings to valid values
                    validValues[propertyName] = propertyValue.ToObject<List<object>>();
                    break;
                case JTokenType.None:
                case JTokenType.Object:
                case JTokenType.Constructor:
                case JTokenType.Property:
                case JTokenType.Comment:
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.Boolean:
                case JTokenType.Null:
                case JTokenType.Undefined:
                case JTokenType.Date:
                case JTokenType.Raw:
                case JTokenType.Bytes:
                case JTokenType.Guid:
                case JTokenType.Uri:
                case JTokenType.TimeSpan:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        return validValues;
    }

    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}
