using FluentValidation;
using Microsoft.Extensions.Options;

namespace TestHost.Settings;

/// <summary>
/// Client-credentials auth for the transport's subscriber ECA, bound from the
/// "subscriberAuthenticationSettings" section (user secrets). Distinct from the publisher ECA
/// (Salesforce lib, "publisherAuthenticationSettings") so the two token lifecycles are
/// independent. No cache knob: the auth handler fetches fresh every call — the transport owns
/// token caching and invalidation.
/// </summary>
public sealed class SubscriberAuthenticationSettings
{
    public string ClientId { get; set; } = null!;
    public string ClientSecret { get; set; } = null!;
    public Uri LoginUri { get; set; } = null!;
}

/// <summary>Validates <see cref="SubscriberAuthenticationSettings"/>.</summary>
internal sealed class SubscriberAuthenticationSettingsConfigurer : IValidateOptions<SubscriberAuthenticationSettings>
{
    public ValidateOptionsResult Validate(string? name, SubscriberAuthenticationSettings options)
    {
        var res = new SubscriberAuthenticationSettingsValidator().Validate(options);

        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }
}

internal sealed class SubscriberAuthenticationSettingsValidator : AbstractValidator<SubscriberAuthenticationSettings>
{
    public SubscriberAuthenticationSettingsValidator()
    {
        RuleFor(x => x.ClientId).NotEmpty();
        RuleFor(x => x.ClientSecret).NotEmpty();
        RuleFor(x => x.LoginUri).NotNull();
    }
}
