namespace Wolverine.SalesforcePubSub.Events;

/// <summary>
/// Declares the Salesforce event API name (the Avro record name, e.g. <c>"My_Event__e"</c>) on the .NET
/// event type itself, so registrations can use <c>MapEvent&lt;T&gt;()</c> without repeating the name at
/// every call site. An explicit name passed to <c>MapEvent</c> always wins over the attribute.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SalesforcePlatformEventAttribute : Attribute
{
    public SalesforcePlatformEventAttribute(string eventApiName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventApiName);
        EventApiName = eventApiName;
    }

    public string EventApiName { get; }
}
