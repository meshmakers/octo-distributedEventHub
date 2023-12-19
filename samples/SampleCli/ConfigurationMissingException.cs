namespace SampleCli;

public class ConfigurationMissingException : Exception
{
    public ConfigurationMissingException()
    {
    }

    public ConfigurationMissingException(string message) : base(message)
    {
    }

    public ConfigurationMissingException(string message, Exception inner) : base(message, inner)
    {
    }
}