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

    public static string GenerateTextValue(string columnName)
    {
        if (Regex.IsMatch(columnName, @"\b(?:name|fullname)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Name.FullName();
        }

        if (Regex.IsMatch(columnName, @"\b(?:email)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Internet.Email();
        }

        if (Regex.IsMatch(columnName, @"\b(?:address)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Address.FullAddress();
        }

        if (Regex.IsMatch(columnName, @"\b(?:phone)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Phone.PhoneNumber();
        }

        if (Regex.IsMatch(columnName, @"\b(?:password)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Internet.Password();
        }

        if (Regex.IsMatch(columnName, @"\b(?:picture)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Image.LoremFlickrUrl();
        }

        if (Regex.IsMatch(columnName, @"\b(?:url)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Internet.Url();
        }

        if (Regex.IsMatch(columnName, @"\b(?:price)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Commerce.Price();
        }

        if (Regex.IsMatch(columnName, @"\b(?:review)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Rant.Review();
        }

        if (Regex.IsMatch(columnName, @"\b(?:country)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Address.Country();
        }

        if (Regex.IsMatch(columnName, @"\b(?:city)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Address.City();
        }

        if (Regex.IsMatch(columnName, @"\b(?:zipcode)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Address.ZipCode();
        }

        if (Regex.IsMatch(columnName, @"\b(?:message)\b", RegexOptions.IgnoreCase))
        {
            return Instance.Lorem.Text();
        }

        return Regex.IsMatch(columnName, @"\b(?:description)\b", RegexOptions.IgnoreCase)
            ? Instance.Random.Words()
            : Instance.Lorem.Word();
    }
}