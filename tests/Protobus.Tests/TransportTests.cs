using System;
using Protobus.Amqp;
using Xunit;

namespace Protobus.Tests;

public class TransportTests
{
    [Fact]
    public void ATrailingSlashIsTheDefaultVhostAsInEveryPort()
    {
        // The AMQP URI spec reads "amqp://host/" as the empty vhost; amqplib, pika, the Go client
        // and rabbitmq-c as protobus uses them all reach "/".
        Assert.Equal("/", RabbitTransport.Configure("amqp://guest:guest@127.0.0.1:25672/", 30).VirtualHost);
        Assert.Equal("/", RabbitTransport.Configure("amqp://127.0.0.1", 30).VirtualHost);
        Assert.Equal("/", RabbitTransport.Configure("amqp://h/%2f", 30).VirtualHost);
        Assert.Equal("prod", RabbitTransport.Configure("amqp://h/prod", 30).VirtualHost);
    }

    [Fact]
    public void CredentialsHostAndPortComeFromTheUrl()
    {
        var f = RabbitTransport.Configure("amqp://user:p%40ss@broker:5673/v", 30);
        Assert.Equal("user", f.UserName);
        Assert.Equal("p@ss", f.Password);
        Assert.Equal("broker", f.HostName);
        Assert.Equal(5673, f.Port);
    }

    [Fact]
    public void TheUrlsHeartbeatWinsAndZeroDisablesIt()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), RabbitTransport.Configure("amqp://h/", 30).RequestedHeartbeat);
        Assert.Equal(TimeSpan.FromSeconds(5), RabbitTransport.Configure("amqp://h/?heartbeat=5", 30).RequestedHeartbeat);
        Assert.Equal(TimeSpan.Zero, RabbitTransport.Configure("amqp://h/?heartbeat=0", 30).RequestedHeartbeat);
        Assert.Throws<AmqpException>(() => RabbitTransport.Configure("amqp://h/?heartbeat=x", 30));
    }

    [Fact]
    public void AmqpsVerifiesTheBrokerUnlessToldNotTo()
    {
        var tls = RabbitTransport.Configure("amqps://h/", 30);
        Assert.True(tls.Ssl.Enabled);
        Assert.Equal(5671, tls.Port);
        Assert.Equal(System.Net.Security.SslPolicyErrors.None, tls.Ssl.AcceptablePolicyErrors);
        Assert.Equal("h", tls.Ssl.ServerName);
        Assert.False(RabbitTransport.Configure("amqp://h/", 30).Ssl.Enabled);
        Assert.NotEqual(System.Net.Security.SslPolicyErrors.None,
            RabbitTransport.Configure("amqps://h/?verify=verify_none", 30).Ssl.AcceptablePolicyErrors);
    }

    [Fact]
    public void ProtobusDoesItsOwnRecovery()
    {
        Assert.False(RabbitTransport.Configure("amqp://h/", 30).AutomaticRecoveryEnabled);
        Assert.False(RabbitTransport.Configure("amqp://h/", 30).TopologyRecoveryEnabled);
    }

    [Fact]
    public void AnUnparseableUrlNeverEchoesItsPassword()
    {
        // .NET is lenient with stray escapes and spaces; a port that is not a number it refuses.
        var e = Assert.Throws<AmqpException>(() => RabbitTransport.Configure("amqp://svc:hunter2@host:port/", 30));
        Assert.DoesNotContain("hunter2", e.Message);
        Assert.DoesNotContain("hunter2", e.ToString());
    }
}
