using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ZProxy;

public class Server : IAsyncDisposable
{
    private readonly NetMQCertificate _cert;
    private readonly string _bindEndpoint;
    private readonly DealerSocket _socket;
    private readonly ILogger<Server> _logger;
    private readonly Channel<NetMQMessage> _upstreamMsgs 
        = Channel.CreateUnbounded<NetMQMessage>(new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = true
        });
    private readonly Channel<NetMQMessage> _downstreamMsgs 
        = Channel.CreateUnbounded<NetMQMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true
        });

    private readonly ConcurrentDictionary<string, (TcpClient tcp, Channel<byte[]> data, CancellationTokenSource cts)> _proxy
        = new ConcurrentDictionary<string, (TcpClient tcp, Channel<byte[]> data, CancellationTokenSource cts)>();

    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    private Task? _router;
    private Task? _processor;
    private Task? _sender;
    private readonly SemaphoreSlim _concurrency;

    public Server(string bindEndpoint, NetMQCertificate serverCert, ILogger<Server> logger, int maxConcurrency = 200)
    {
        _cert = serverCert;
        _bindEndpoint = bindEndpoint;

        _socket = new DealerSocket();
        _socket.Options.CurveServer = true;
        _socket.Options.CurveCertificate = _cert;

        _logger = logger;
        _concurrency = new SemaphoreSlim(maxConcurrency, maxConcurrency);
    }

    public Task RunAsync()
    {
        _socket.Bind(_bindEndpoint);
        _logger.LogInformation("listening on {_bindEndpoint}", _bindEndpoint);

        _sender = RunSenderAsync(_cts.Token);
        _router = RunRouterAsync(_cts.Token);
        _processor = RunMsgProcessorAsync(_cts.Token);

        return Task.WhenAll(_sender, _router, _processor);
    }
    private async Task RunSenderAsync(CancellationToken ct)
    {
        await foreach (var msg in _downstreamMsgs.Reader.ReadAllAsync(ct))
        {
            if (msg.FrameCount != 4)
                continue;

            _socket.SendMultipartMessage(msg);
        }
    }
    private async Task RunRouterAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var msg = await _socket.ReceiveMultipartMessageAsync(4, ct);
                if (msg.FrameCount != 4)
                    continue;
                await _upstreamMsgs.Writer.WriteAsync(msg, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError("router error: {ex.Message}", ex.Message);
            }
        }
    }
    private async Task RunMsgProcessorAsync(CancellationToken ct)
    {
        await foreach (var msg in _upstreamMsgs.Reader.ReadAllAsync(ct))
        {
            await _concurrency.WaitAsync(ct);
            try
            {
                await HandleClientMessageAsync(msg);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "unexpected error processing message");
            }
            finally
            {
                _concurrency.Release();
            }
        }
    }

    private async Task HandleClientMessageAsync(NetMQMessage msg)
    {
        var connId = msg[0].ConvertToString();
        var cmd = msg[2].ConvertToString();
        
        try
        {
            switch (cmd)
            {
                case Traffic.CON:
                    var hostPort = msg[3].ConvertToString();
                    var lastColon = hostPort.LastIndexOf(':');
                    if (lastColon < 0) 
                        break;
                    var host = hostPort[..lastColon];
                    var portStr = hostPort[(lastColon + 1)..];
                    if (!int.TryParse(portStr, out var port)) 
                        break;

                    await HandleNewConnectionAsync(connId, host, port);
                    break;

                case Traffic.DAT:
                    if (_proxy.TryGetValue(connId, out var proxy))
                    {
                        await proxy.data.Writer.WriteAsync(msg[3].ToByteArray());
                    }
                    break;

                case Traffic.EOF:
                    CloseConnection(connId);
                    break;

                default:
                    break;
            }
        }
        catch (Exception ex)
        {
            var err = new NetMQMessage();
            err.Append(connId);
            err.AppendEmptyFrame();
            err.Append(Traffic.ERR);
            err.Append($"Error handling {connId}: {ex.Message}");
            await _downstreamMsgs.Writer.WriteAsync(err);

            CloseConnection(connId);
        }
    }

    private async Task HandleNewConnectionAsync(string connId, string host, int port)
    {
        var tcp = new TcpClient();
        
        try
        {
            await tcp.ConnectAsync(host, port);
        }
        catch (Exception ex)
        {
            var err = new NetMQMessage();
            err.Append(connId);
            err.AppendEmptyFrame();
            err.Append(Traffic.ERR);
            err.Append($"Error handling {connId}: {ex.Message}");
            await _downstreamMsgs.Writer.WriteAsync(err);

            return;
        }

        var stream = tcp.GetStream();
        var data = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(128)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
        var connCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        
        _proxy[connId] = (tcp, data, connCts);

        // send ACK
        var ack = new NetMQMessage();
        ack.Append(connId);
        ack.AppendEmptyFrame();
        ack.Append(Traffic.ACK);
        ack.AppendEmptyFrame();

        await _downstreamMsgs.Writer.WriteAsync(ack);

        // relay to client
        var upstream = Relay.RelayFromChannelAsync(connId, data.Reader, stream, _logger, connCts.Token);
        var downstream = Relay.RelayToZmqAsync(connId, stream, _downstreamMsgs.Writer, _logger, connCts.Token);

        // wait both directions to finish naturally
        // EOF from one side = close that direction gracefully
        // let the other direction finish
        await Task.WhenAll(upstream, downstream);
    }

    private void CloseConnection(string connId)
    {
        if (!_proxy.TryRemove(connId, out var proxy))
            return;

        try 
        { 
            proxy.cts.Cancel(); 
        } 
        catch { }

        proxy.cts.Dispose();
        proxy.tcp.Dispose();
        proxy.data.Writer.Complete();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        foreach (var key in _proxy.Keys)
            CloseConnection(key);

        _downstreamMsgs.Writer.Complete();
        _upstreamMsgs.Writer.Complete();

        if (_sender != null) 
            await _sender;
        if (_router != null) 
            await _router;
        if (_processor != null) 
            await _processor;

        _socket.Dispose();
        _concurrency.Dispose();
            
        _cts.Dispose();
    }
}