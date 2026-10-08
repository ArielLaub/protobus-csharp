using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Protobus.IntegrationTests;

/// <summary>
/// The real broker the integration suites run against. They never default to one: set
/// PROTOBUS_TEST_AMQP_URL (and PROTOBUS_TEST_MGMT_URL for the management API), for example to the
/// docker-compose broker.
/// </summary>
internal static class RealBroker
{
    private static readonly HttpClient Http = new();

    internal static string AmqpUrl()
    {
        var url = Environment.GetEnvironmentVariable("PROTOBUS_TEST_AMQP_URL");
        Assert.SkipWhen(string.IsNullOrEmpty(url), "PROTOBUS_TEST_AMQP_URL is not set");
        return url!;
    }

    private static Task<HttpResponseMessage> SendAsync(HttpMethod method, string path)
    {
        var @base = Environment.GetEnvironmentVariable("PROTOBUS_TEST_MGMT_URL");
        Assert.SkipWhen(string.IsNullOrEmpty(@base), "PROTOBUS_TEST_MGMT_URL is not set");
        var uri = new Uri(@base!.TrimEnd('/') + path);
        var info = string.IsNullOrEmpty(uri.UserInfo) ? "guest:guest" : Uri.UnescapeDataString(uri.UserInfo);
        var bare = new UriBuilder(uri) { UserName = "", Password = "" }.Uri;
        var request = new HttpRequestMessage(method, bare);
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(info)));
        return Http.SendAsync(request);
    }

    private static string Escape(string name) => Uri.EscapeDataString(name);

    internal static Task DeleteQueueAsync(string name) => SendAsync(HttpMethod.Delete, "/api/queues/%2f/" + Escape(name));

    internal static Task DeleteExchangeAsync(string name) => SendAsync(HttpMethod.Delete, "/api/exchanges/%2f/" + Escape(name));

    /// <summary>Delete a service's queues and exchanges: the main, retry, DLQ and event objects.</summary>
    internal static async Task DeleteServiceAsync(string name)
    {
        foreach (var q in new[] { name, name + ".Retry", name + ".DLQ", name + ".Events", name + ".Events.Retry", name + ".Events.DLQ" })
            await DeleteQueueAsync(q);
        foreach (var x in new[] { name + ".Retry.Exchange", name + ".Events.Retry.Exchange", name + ".Events.Redelivery" })
            await DeleteExchangeAsync(x);
    }

    /// <summary>Close every connection named protobus-csharp, as a broker restart or a network failure would.</summary>
    internal static async Task<int> CloseConnectionsAsync()
    {
        var r = await SendAsync(HttpMethod.Get, "/api/connections");
        using var doc = JsonDocument.Parse(await r.Content.ReadAsStringAsync());
        var names = new List<string>();
        foreach (var c in doc.RootElement.EnumerateArray())
        {
            if (c.TryGetProperty("client_properties", out var p) && p.TryGetProperty("connection_name", out var n)
                && n.GetString() == "protobus-csharp")
                names.Add(c.GetProperty("name").GetString()!);
        }
        foreach (var n in names) await SendAsync(HttpMethod.Delete, "/api/connections/" + Escape(n));
        return names.Count;
    }

    internal static async Task<int> QueueMessagesAsync(string queue)
    {
        var r = await SendAsync(HttpMethod.Get, "/api/queues/%2f/" + Escape(queue));
        if (!r.IsSuccessStatusCode) return 0;
        using var doc = JsonDocument.Parse(await r.Content.ReadAsStringAsync());
        return doc.RootElement.TryGetProperty("messages", out var m) && m.ValueKind == JsonValueKind.Number ? m.GetInt32() : 0;
    }

    internal static async Task<bool> WaitForAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await predicate())
        {
            if (DateTime.UtcNow >= deadline) return false;
            await Task.Delay(50);
        }
        return true;
    }
}
