using System;
using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, (Channel<NetMQMessage> Channel, CancellationTokenSource Cts)> _inbounds
        = new ConcurrentDictionary<string, (Channel<NetMQMessage>, CancellationTokenSource)>();
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
        if (string.IsNullOrWhiteSpace(connId))
        {
            _logger.LogWarning("Invalid connId in connection request");
            await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x01);
            conn.Stream.Dispose();
            return;
        }

        var inboundMsgs = Channel.CreateBounded<NetMQMessage>(
            new BoundedChannelOptions(128) { FullMode = BoundedChannelFullMode.Wait });

        var connCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var existing = _inbounds.GetOrAdd(connId, (inboundMsgs, connCts));

        if (existing.Channel != inboundMsgs)
        {
            _logger.LogError("Duplicate connId {ConnId}, rejecting", connId);
            await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x01);
            connCts.Dispose();
            conn.Stream.Dispose();
            return;
        }

        bool established = false;

        try
        {
            await _outbound.Writer.WriteAsync(conn.Msg, _cts.Token);
            var response = await WaitForServerAsync(inboundMsgs.Reader, _cts.Token);

            if (response.FrameCount != 4 || response[2].ConvertToString() != Traffic.ACK)
            {
                _logger.LogWarning("Connection {ConnId} rejected by server", connId);
                await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x05);
                return;
            }

            established = true;
            _logger.LogInformation("Connection {ConnId} established", connId);
            await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x00);

            var upstreamTask = Task.Run(async () =>
            {
                try
                {
                    await Relay.RelayToZmqAsync(connId, conn.Stream, _outbound.Writer, _logger, connCts.Token);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Upstream failed for {ConnId}", connId);
                }
            });

            var downstreamTask = Task.Run(async () =>
            {
                try
                {
                    await Relay.RelayFromZmqAsync(connId, inboundMsgs.Reader, conn.Stream, _logger, connCts.Token);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Downstream failed for {ConnId}", connId);
                }
            });

            await Task.WhenAny(upstreamTask, downstreamTask);
            await Task.WhenAll(upstreamTask, downstreamTask);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to handle connection {ConnId}", connId);
            if (!established)
            {
                try { await Socks5InboundHandler.SendReplyAsync(conn.Stream, 0x01); }
                catch { }
            }
        }
        finally
        {
            if (established)
            {
                var eof = new NetMQMessage();
                eof.Append(connId);
                eof.AppendEmptyFrame();
                eof.Append(Traffic.EOF);
                eof.AppendEmptyFrame();

                try { await _outbound.Writer.WriteAsync(eof, _cts.Token); }
                catch { }
            }

            if (_inbounds.TryRemove(connId, out var entry))
            {
                try { entry.Cts.Cancel(); } catch { }
                entry.Cts.Dispose();
                entry.Channel.Writer.Complete();
            }

            try { conn.Stream.Dispose(); } catch { }
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

    private async Task RunSendingAsync(CancellationToken ct)
    {
        await foreach (var msg in _outbound.Reader.ReadAllAsync(ct))
        {
            if (msg.FrameCount != 4)
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

                if (!_inbounds.TryGetValue(connId, out var entry))
                    continue;

                switch (cmd)
                {
                    case Traffic.DAT:
                    case Traffic.ACK:
                    case Traffic.ERR:
                        await entry.Channel.Writer.WriteAsync(msg, ct);
                        break;
                    case Traffic.EOF:
                        await entry.Channel.Writer.WriteAsync(msg, ct);
                        entry.Channel.Writer.Complete();
                        break;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _logger.LogError(ex, "Receiving error, closing socket");
                break;
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

        foreach (var entry in _inbounds.Values)
        {
            try { entry.Cts.Cancel(); } catch { }
            entry.Cts.Dispose();
        }
        _inbounds.Clear();
        _cts.Dispose();
    }
}