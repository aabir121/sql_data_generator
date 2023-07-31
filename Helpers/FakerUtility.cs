using System.Text.RegularExpressions;
using Bogus;

namespace SQLDataGenerator.Helpers;

public class FakerUtility
{
    private static readonly Lazy<Faker> LazyFaker = new Lazy<Faker>(() => new Faker());

    public static Faker Instance => LazyFaker.Value;

    private FakerUtility()
    {
        // Private constructor to prevent external instantiation.
    }

    public static int GetRandomInt()
    {
        return Instance.Random.Number(1, 100);
    }

    public static decimal GetRandomDecimal()
    {
        return Instance.Random.Decimal(1, 100);
    }

    public static bool GetRandomBool()
    {
        return Instance.Random.Bool();
    }

    public static DateTime GetRandomDate()
    {
        return Instance.Date.Past();
    }

public static string GenerateTextValue(string columnName, int? maxLength)
    {
        // Define the mapping of column name keywords to Faker methods.
        var keywordToMethodMap = new Dictionary<string, Func<string>>
        {
            { @"\b(?:name|fullname)\b", () => Instance.Name.FullName() },
            { @"\b(?:email)\b", () => Instance.Internet.Email() },
            { @"\b(?:address)\b", () => Instance.Address.FullAddress() },
            { @"\b(?:phone)\b", () => Instance.Phone.PhoneNumber() },
            { @"\b(?:password)\b", () => Instance.Internet.Password() },
            { @"\b(?:picture)\b", () => Instance.Image.LoremFlickrUrl() },
            { @"\b(?:url)\b", () => Instance.Internet.Url() },
            { @"\b(?:price)\b", () => Instance.Commerce.Price() },
            { @"\b(?:review)\b", () => Instance.Rant.Review() },
            { @"\b(?:country)\b", () => Instance.Address.Country() },
            { @"\b(?:city)\b", () => Instance.Address.City() },
            { @"\b(?:zipcode)\b", () => Instance.Address.ZipCode() },
            { @"\b(?:message)\b", () => Instance.Lorem.Text() },
            { @"\b(?:description)\b", () => Instance.Random.Words() },
        };

        foreach (var kvp in keywordToMethodMap)
        {
            if (!Regex.IsMatch(columnName, kvp.Key, RegexOptions.IgnoreCase)) continue;
            var generatedValue = kvp.Value();
            return TruncateTextIfNeeded(generatedValue, maxLength);
        }

        // If no specific keyword is matched, return the default Faker value.
        var defaultValue = Instance.Lorem.Word();
        return TruncateTextIfNeeded(defaultValue, maxLength);
    }

    private static string TruncateTextIfNeeded(string text, int? maxLength)
    {
        if (maxLength.HasValue && maxLength.Value < text.Length)
        {
            // If maxLength is smaller than the generated text length, truncate the text.
            return text[..maxLength.Value];
        }
        return text;
    }
}