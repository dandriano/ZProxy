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

    private readonly ConcurrentDictionary<string, (TcpClient tcp, Channel<byte[]> data)> _proxy 
        = new ConcurrentDictionary<string, (TcpClient tcp, Channel<byte[]> data)>();

    private readonly ConcurrentDictionary<string, CancellationTokenSource> _connectionCts 
        = new ConcurrentDictionary<string, CancellationTokenSource>();

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
            _ = Task.Run(async () =>
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
            });
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
            await _downstreamMsgs.Writer.WriteAsync(msg);

            CloseConnection(connId);
        }
    }

    private async Task HandleNewConnectionAsync(string connId, string host, int port)
    {
        var tcp = new TcpClient();
        await tcp.ConnectAsync(host, port);

        var stream = tcp.GetStream();
        var data = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(128)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
        _proxy[connId] = (tcp, data);

        var connCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        _connectionCts[connId] = connCts;

        // send ACK
        var ack = new NetMQMessage();
        ack.Append(connId);
        ack.AppendEmptyFrame();
        ack.Append(Traffic.ACK);
        ack.AppendEmptyFrame();

        await _downstreamMsgs.Writer.WriteAsync(ack);

        // relay to client
        _ = RelayRemoteToClientAsync(connId, stream, connCts.Token);
        _ = RelayClientToRemoteAsync(connId, data.Reader, stream, connCts.Token);
    }

    private async Task RelayRemoteToClientAsync(string connId, NetworkStream stream, CancellationToken ct)
    {
        var buffer = new byte[32768];

        try
        {
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer, ct)) > 0)
            {
                var dat = new NetMQMessage();
                dat.Append(connId);
                dat.AppendEmptyFrame();
                dat.Append(Traffic.DAT);
                dat.Append(new NetMQFrame(buffer, bytesRead));

                await _downstreamMsgs.Writer.WriteAsync(dat, ct);
            }

            var eof = new NetMQMessage();
            eof.Append(connId);
            eof.AppendEmptyFrame();
            eof.Append(Traffic.EOF);
            eof.AppendEmptyFrame();

            await _downstreamMsgs.Writer.WriteAsync(eof, ct);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            var err = new NetMQMessage();
            err.Append(connId);
            err.AppendEmptyFrame();
            err.Append(Traffic.ERR);
            err.Append($"Error handling {connId}: {ex.Message}");
            await _downstreamMsgs.Writer.WriteAsync(err, ct);
        }
        finally
        {
            CloseConnection(connId);
        }
    }

    private async Task RelayClientToRemoteAsync(string connId, ChannelReader<byte[]> reader, NetworkStream stream, CancellationToken ct)
    {
        try
        {
            await foreach (var data in reader.ReadAllAsync(ct))
            {
                await stream.WriteAsync(data, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError("error writing to remote {connId}: {ex.Message}", connId, ex.Message);
        }
        finally
        {
            CloseConnection(connId);
        }
    }

    private void CloseConnection(string connId)
    {
        if (_connectionCts.TryRemove(connId, out var cts))
        {
            cts.Cancel();
            cts.Dispose();
        }

        if (_proxy.TryRemove(connId, out var proxy))
        {
            proxy.tcp.Dispose();
            proxy.data.Writer.Complete();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        foreach (var cts in _connectionCts.Values) 
        { 
            cts.Cancel(); 
            cts.Dispose(); 
        }
        _connectionCts.Clear();

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

        foreach (var (tcp, data) in _proxy.Values)
        {
            tcp.Dispose();
            data.Writer.Complete();
        }
            
        _cts.Dispose();
    }
}