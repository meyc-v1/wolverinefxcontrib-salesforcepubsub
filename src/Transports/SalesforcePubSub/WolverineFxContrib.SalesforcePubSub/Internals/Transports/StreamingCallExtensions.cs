using Grpc.Core;

namespace Wolverine.SalesforcePubSub.Internals.Transports;

internal static class AsyncDuplexStreamingCallExtensions
{
    public static bool TryGetTrailers<TRequest, TResponse>(this AsyncDuplexStreamingCall<TRequest, TResponse> streamingCall, out Metadata? trailers)
    {
        try
        {
            trailers = streamingCall.GetTrailers();
            return true;
        }
        catch (Exception)
        {
            trailers = null;
            return false;
        }
    }

    public static bool TryGetStatus<TRequest, TResponse>(this AsyncDuplexStreamingCall<TRequest, TResponse> streamingCall, out Status? status)
    {
        try
        {
            status = streamingCall.GetStatus();
            return true;
        }
        catch (Exception)
        {
            status = null;
            return false;
        }
    }
}

internal static class RequestStreamExtensions
{
    /// <summary>
    /// Tries to complete the request stream. Returns any exception that occurs but does not throw.
    /// </summary>
    public static async Task<(bool Success, Exception? Exception)> TryCompleteAsync<T>(this IClientStreamWriter<T> requestStream)
    {
        try
        {
            await requestStream.CompleteAsync().ConfigureAwait(false);
            return new(true, null);
        }
        catch (Exception exception)
        {
            return new(false, exception);
        }
    }
}
