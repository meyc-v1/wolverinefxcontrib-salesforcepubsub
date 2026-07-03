using Grpc.Core;

namespace Wolverine.SalesforcePubSub.Internals.Authentication;

/// <summary>
/// The single definition of "Salesforce rejected our credentials" for gRPC calls. Shared by the
/// listener's reconnect path and the serializer's recovery-decode path so the invalidate-and-retry
/// contract can never drift between them.
/// </summary>
internal static class SalesforceAuthErrors
{
    public static bool IsAuthRejection(RpcException exception)
        => exception.StatusCode is StatusCode.Unauthenticated or StatusCode.PermissionDenied;
}
