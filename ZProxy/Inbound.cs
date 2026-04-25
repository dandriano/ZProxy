using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetMQ;

namespace ZProxy;

public record InboundConnection(NetworkStream Stream, NetMQMessage Msg);

public interface IInboundHandler : IAsyncDisposable
{
    /// <summary>
    /// Starts listening for incoming connections
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Event raised when a new connection is accepted with its destination
    /// </summary>
    event Func<InboundConnection, Task> OnNewConnectionAsync;
}

public record Socks5Result(bool Success, string Host = "", int Port = 0);

public class Socks5InboundHandler : IInboundHandler
{
    private readonly int _listenPort;
    private readonly ILogger<Socks5InboundHandler> _logger;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public event Func<InboundConnection, Task>? OnNewConnectionAsync;

    public Socks5InboundHandler(int listenPort, ILogger<Socks5InboundHandler> logger)
    {
        _listenPort = listenPort;
        _logger = logger;
        _listener = new TcpListener(System.Net.IPAddress.Loopback, listenPort);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        _logger.LogInformation("inbound handler started on port {_listenPort}", _listenPort);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);

        try
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await _listener.AcceptTcpClientAsync(linkedCts.Token);
                    _ = HandleConnectionAsync(tcpClient, linkedCts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                
            }
        }
        finally
        {
            _listener.Stop();
            _logger.LogInformation("inbound handler stopped");
        }
    }
    private async Task HandleConnectionAsync(TcpClient tcpClient, CancellationToken ct)
    {
        var connId = Guid.NewGuid().ToString("N");
        await using var stream = tcpClient.GetStream();

        try
        {
            var result = await PerformHandshakeAsync(stream, ct);

            if (!result.Success)
            {
                _logger.LogWarning("handshake failed for connection {connId}", connId);
                return;
            }

            _logger.LogDebug("SOCKS5 CONNECT: {Host}:{Port}", result.Host, result.Port);

            if (OnNewConnectionAsync == null)
            {
                _logger.LogWarning("something went wrong");
                await SendReplyAsync(stream, 0x01);
                return;
            }

            var msg = new NetMQMessage(4);
            msg.Append(connId);
            msg.AppendEmptyFrame();
            msg.Append(Traffic.CON);
            msg.Append($"{result.Host}:{result.Port}");

            await OnNewConnectionAsync(new InboundConnection(stream, msg));
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "unexpected error in connection {connId}", connId);
            await SendReplyAsync(stream, 0x01); // general failure
        }
    }
    private static async ValueTask<Socks5Result> PerformHandshakeAsync(NetworkStream stream, CancellationToken ct)
    {
        var buffer = new byte[512];

        // 1. socks5 greeting
        int read = await stream.ReadAsync(buffer.AsMemory(0, 2), ct);
        if (read < 2 || buffer[0] != 0x05)
            return new Socks5Result(false);

        int nMethods = buffer[1];
        if (nMethods > 0)
            await stream.ReadAsync(buffer.AsMemory(0, nMethods), ct);

        // 2. reply - no authentication required
        await stream.WriteAsync(new byte[] { 0x05, 0x00 }.AsMemory(), ct);

        // 3. version + CONNECT command
        read = await stream.ReadAsync(buffer.AsMemory(0, 4), ct);
        if (read < 4 || buffer[0] != 0x05 || buffer[1] != 0x01)
            return new Socks5Result(false);

        string host = buffer[3] switch
        {
            0x01 => await ReadIPv4Async(stream, buffer, ct),           // IPv4
            0x03 => await ReadDomainNameAsync(stream, buffer, ct),     // Domain name
            0x04 => throw new NotSupportedException("IPv6 not supported yet"),
            _ => throw new NotSupportedException($"Unsupported address type: {buffer[3]}")
        };

        await stream.ReadAsync(buffer.AsMemory(0, 2), ct);
        int port = (buffer[0] << 8) | buffer[1];

        return new Socks5Result(true, host, port);
    }
    private static async ValueTask<string> ReadIPv4Async(NetworkStream stream, byte[] buffer, CancellationToken ct)
    {
        await stream.ReadAsync(buffer.AsMemory(0, 4), ct);
        return $"{buffer[0]}.{buffer[1]}.{buffer[2]}.{buffer[3]}";
    }
    private static async ValueTask<string> ReadDomainNameAsync(NetworkStream stream, byte[] buffer, CancellationToken ct)
    {
        await stream.ReadAsync(buffer.AsMemory(0, 1), ct);
        int domainLength = buffer[0];

        await stream.ReadAsync(buffer.AsMemory(0, domainLength), ct);
        return Encoding.ASCII.GetString(buffer, 0, domainLength);
    }
    public static async ValueTask SendReplyAsync(NetworkStream stream, byte replyCode)
    {
        var reply = new byte[]
        {
            0x05, replyCode, 0x00, 0x01,   // VER, REP, RSV, ATYP (IPv4)
            0, 0, 0, 0,                    // Dummy IP
            0, 0                           // Dummy port
        };

        await stream.WriteAsync(reply.AsMemory());
    }
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
    }
}

