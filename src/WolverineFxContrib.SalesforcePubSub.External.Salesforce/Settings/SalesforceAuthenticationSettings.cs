using FluentValidation;
using Microsoft.Extensions.Options;

namespace WolverineFxContrib.SalesforcePubSub.External.Salesforce.Settings;

/// <summary>
/// Salesforce client-credentials auth for the REST publisher (typically bound from a
/// "salesforceAuthenticationSettings" section in user-secrets). Mirrors the internal client's settings.
/// </summary>
public sealed class SalesforceAuthenticationSettings
{
    public string ClientId { get; set; } = null!;
    public string ClientSecret { get; set; } = null!;
    public Uri LoginUri { get; set; } = null!;
    public int TokenCachingInMinutes { get; set; }
}

/// <summary>Applies defaults (PostConfigure) and validates <see cref="SalesforceAuthenticationSettings"/>.</summary>
internal sealed class SalesforceAuthenticationSettingsConfigurer
    : IPostConfigureOptions<SalesforceAuthenticationSettings>, IValidateOptions<SalesforceAuthenticationSettings>
{
    private const int DefaultTokenCachingInMinutes = 60;

    public void PostConfigure(string? name, SalesforceAuthenticationSettings options)
    {
        if (options.TokenCachingInMinutes <= 0)
            options.TokenCachingInMinutes = DefaultTokenCachingInMinutes;
    }

    public ValidateOptionsResult Validate(string? name, SalesforceAuthenticationSettings options)
    {
        var res = new SalesforceAuthenticationSettingsValidator().Validate(options);

        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }
}

internal sealed class SalesforceAuthenticationSettingsValidator : AbstractValidator<SalesforceAuthenticationSettings>
{
    public SalesforceAuthenticationSettingsValidator()
    {
        RuleFor(x => x.ClientId).NotEmpty();
        RuleFor(x => x.ClientSecret).NotEmpty();
        RuleFor(x => x.LoginUri).NotNull();
    }
}
