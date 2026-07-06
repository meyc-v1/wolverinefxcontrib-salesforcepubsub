using Azure.Core;
using Azure.Identity;
using Microsoft.Data.SqlClient;

namespace MssqlReplay;

/// <summary>
/// Standard Azure SQL Entra auth pattern: a <see cref="ChainedTokenCredential"/> —
/// WorkloadIdentity in AKS, AzureCli locally — backing the <c>Active Directory Default</c> connection
/// string method, instead of the slower built-in DefaultAzureCredential probing. Call
/// <see cref="Register"/> once at startup.
/// </summary>
public static class MssqlAadAuthentication
{
    private static readonly TokenCredential Credential =
        new ChainedTokenCredential(new WorkloadIdentityCredential(), new AzureCliCredential());

    public static void Register()
        => SqlAuthenticationProvider.SetProvider(
            SqlAuthenticationMethod.ActiveDirectoryDefault,
            new CustomSqlAuthentication(Credential));
}

internal sealed class CustomSqlAuthentication(TokenCredential tokenCredential) : SqlAuthenticationProvider
{
    private const string DefaultScopeSuffix = "/.default";

    public override async Task<SqlAuthenticationToken> AcquireTokenAsync(SqlAuthenticationParameters parameters)
    {
        var scope = parameters.Resource.EndsWith(DefaultScopeSuffix, StringComparison.Ordinal)
            ? parameters.Resource
            : parameters.Resource + DefaultScopeSuffix;

        var accessToken = await tokenCredential
            .GetTokenAsync(new TokenRequestContext([scope]), CancellationToken.None)
            .ConfigureAwait(false);

        return new SqlAuthenticationToken(accessToken.Token, accessToken.ExpiresOn);
    }

    public override bool IsSupported(SqlAuthenticationMethod authenticationMethod)
        => authenticationMethod == SqlAuthenticationMethod.ActiveDirectoryDefault;
}
