using FluentValidation;
using Microsoft.Extensions.Options;

namespace WolverineFxContrib.SalesforcePubSub.External.Salesforce.Settings;

/// <summary>Salesforce REST client configuration (typically bound from a "salesforceSettings" section).</summary>
public sealed class SalesforceSettings
{
    /// <summary>REST data API base, e.g. https://your-org.my.salesforce.com/services/data/v64.0/ (trailing slash required).</summary>
    public Uri BaseUri { get; set; } = null!;
}

/// <summary>Validates <see cref="SalesforceSettings"/>.</summary>
internal sealed class SalesforceSettingsConfigurer : IValidateOptions<SalesforceSettings>
{
    public ValidateOptionsResult Validate(string? name, SalesforceSettings options)
    {
        var res = new SalesforceSettingsValidator().Validate(options);

        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }
}

internal sealed class SalesforceSettingsValidator : AbstractValidator<SalesforceSettings>
{
    public SalesforceSettingsValidator()
    {
        RuleFor(x => x.BaseUri).NotNull();
    }
}
