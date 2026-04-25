using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Sockets;

namespace ZProxy;

public class Client : IAsyncDisposable
{
    private readonly ILogger<Client> _logger;
    private readonly string _serverEndpoint;
    private readonly byte[] _serverPublicKey;
    private readonly NetMQCertificate _clientCert;
    private readonly DealerSocket _socket;
    private readonly IInboundHandler _inbound;
    private readonly Channel<NetMQMessage> _outbound;
    private readonly ConcurrentDictionary<string, Channel<NetMQMessage>> _inbounds 
        = new ConcurrentDictionary<string, Channel<NetMQMessage>>();
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    private Task? _sending;
    private Task? _receiving;

    public Client(string serverEndpoint, byte[] serverPublicKey, NetMQCertificate clientCert,
                  IInboundHandler inbound, ILogger<Client> logger, int maxConcurrency = 100)
    {
        _serverEndpoint = serverEndpoint;
        _serverPublicKey = serverPublicKey;
        _clientCert = clientCert;

        _socket = new DealerSocket();
        _socket.Options.CurveServerKey = _serverPublicKey;
        _socket.Options.CurveCertificate = _clientCert;

        _outbound = Channel.CreateBounded<NetMQMessage>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = true
        });

        _logger = logger;
        _inbound = inbound;
        _inbound.OnNewConnectionAsync += OnNewInboundConnectionAsync;
    }

    public Task RunAsync()
    {
        _socket.Connect(_serverEndpoint);

        _sending = RunSendingAsync(_cts.Token);
        _receiving = RunReceivingAsync(_cts.Token);

        return Task.WhenAll(_sending, _receiving, _inbound.StartAsync(_cts.Token));
    }

    private async Task OnNewInboundConnectionAsync(InboundConnection conn)
    {
        var connId = conn.Msg[0].ConvertToString();
        var inboundMsgs = Channel.CreateBounded<NetMQMessage>(
            new BoundedChannelOptions(128) { FullMode = BoundedChannelFullMode.Wait });

        _inbounds[connId] = inboundMsgs;

        try
        {
            await _outbound.Writer.WriteAsync(conn.Msg, _cts.Token);
            var response = await WaitForServerAsync(inboundMsgs.Reader, _cts.Token);

            if (response.FrameCount !=4 || response[2].ConvertToString() != Traffic.ACK)
            {
                // refused
                _logger.LogWarning("connection {connId} rejected", connId);
                await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x05);
                return;
            }

            _logger.LogInformation("connection {connId} established", connId);
            await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x00);

            // start relay
            var upstreamTask = RelayUpstreamAsync(connId, conn.Stream);
            var downstreamTask = RelayDownstreamAsync(inboundMsgs.Reader, conn.Stream);

            await Task.WhenAny(upstreamTask, downstreamTask);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "failed to handle connection {ConnId}", connId);
            await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x01);
        }
        finally
        {
            var eof = new NetMQMessage();
            eof.Append(connId);
            eof.AppendEmptyFrame();
            eof.Append(Traffic.EOF);
            eof.AppendEmptyFrame();

            await _outbound.Writer.WriteAsync(eof, _cts.Token);
            _inbounds.TryRemove(connId, out _);
            inboundMsgs.Writer.Complete();
        }
    }

    private static async Task<NetMQMessage> WaitForServerAsync(ChannelReader<NetMQMessage> reader, CancellationToken ct)
    {
        using var timeoutCts = new CancellationTokenSource(15000);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        try
        {
            return await reader.ReadAsync(linked.Token);
        }
        catch (OperationCanceledException)
        {
            var err = new NetMQMessage();
            err.AppendEmptyFrame();
            err.AppendEmptyFrame();
            err.Append(Traffic.ERR);
            err.Append("Timeout waiting for server ACK/ERR");

            return err;
        }
    }

    private async Task RelayUpstreamAsync(string connId, NetworkStream stream)
    {
        var buffer = new byte[32768];
        int bytesRead;

        try
        {
            while ((bytesRead = await stream.ReadAsync(buffer, _cts.Token)) > 0)
            {
                var dat = new NetMQMessage();
                dat.Append(connId);
                dat.AppendEmptyFrame();
                dat.Append(Traffic.DAT);
                dat.Append(new NetMQFrame(buffer, bytesRead));

                await _outbound.Writer.WriteAsync(dat, _cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task RelayDownstreamAsync(ChannelReader<NetMQMessage> reader, NetworkStream stream)
    {
        await foreach (var msg in reader.ReadAllAsync())
        {
            if (msg.FrameCount != 4)
                continue;

            switch (msg[2].ConvertToString())
            {
                case Traffic.DAT:
                    await stream.WriteAsync(msg[3].ToByteArray());
                    break;
                case Traffic.EOF:
                    return;
                case Traffic.ERR:
                    _logger.LogError("inbound error: {reason}", msg[3].ConvertToString());
                    return;
            }
        }
    }

    private async Task RunSendingAsync(CancellationToken ct)
    {
        await foreach (var msg in _outbound.Reader.ReadAllAsync(ct))
        {
            if (msg.FrameCount !=4)
                continue;

            _socket.SendMultipartMessage(msg);
        }
    }

    private async Task RunReceivingAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var msg = await _socket.ReceiveMultipartMessageAsync(4, ct);
                if (msg.FrameCount != 4)
                    continue;

                var connId = msg[0].ConvertToString();
                var cmd = msg[2].ConvertToString();
                
                if (!_inbounds.TryGetValue(connId, out var channel))
                    continue;

                switch (cmd)
                {
                    case Traffic.DAT:
                    case Traffic.ACK:
                    case Traffic.ERR:
                        await channel.Writer.WriteAsync(msg, ct);
                        break;
                    case Traffic.EOF:
                        await channel.Writer.WriteAsync(msg, ct);
                        channel.Writer.Complete();
                        break;
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError("rec error: {ex.Message}", ex.Message);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _outbound.Writer.Complete();

        if (_sending != null) 
            await _sending;
        if (_receiving != null) 
            await _receiving;

        if (_inbound is IAsyncDisposable d) 
            await d.DisposeAsync();

        _socket.Dispose();
        _cts.Dispose();
    }
}