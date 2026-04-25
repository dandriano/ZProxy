using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetMQ;

namespace ZProxy;

public static class Relay
{
    private const int DefaultBufferSize = 32768;

    /// <summary>
    /// Relays data from NetworkStream → Channel (used for upstream on both sides)
    /// </summary>
    public static async Task RelayToChannelAsync(string connId,
        NetworkStream stream,
        ChannelWriter<byte[]> writer,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        var buffer = new byte[DefaultBufferSize];

        try
        {
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                var data = new byte[bytesRead];           // copy because buffer is reused
                Buffer.BlockCopy(buffer, 0, data, 0, bytesRead);
                await writer.WriteAsync(data, cancellationToken);
            }

            // Graceful EOF from the stream
            writer.Complete();
        }
        catch (OperationCanceledException)
        {
            writer.TryComplete();
        }
        catch (Exception ex) when (ex is not ObjectDisposedException)
        {
            logger.LogError(ex, "RelayToChannel failed for {ConnId}", connId);
            writer.TryComplete(ex);
        }
    }

    /// <summary>
    /// Relays data from Channel → NetworkStream (used for downstream to remote)
    /// </summary>
    public static async Task RelayFromChannelAsync(string connId,
        ChannelReader<byte[]> reader,
        NetworkStream stream,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var data in reader.ReadAllAsync(cancellationToken))
            {
                await stream.WriteAsync(data, cancellationToken);
            }

            await stream.FlushAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex) when (ex is not ObjectDisposedException)
        {
            logger.LogError(ex, "RelayFromChannel failed for {ConnId}", connId);
        }
    }

    /// <summary>
    /// Relays data from ZMQ messages → NetworkStream (Client downstream)
    /// </summary>
    public static async Task RelayFromZmqAsync(string connId, 
        ChannelReader<NetMQMessage> reader,
        NetworkStream stream,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var msg in reader.ReadAllAsync(cancellationToken))
            {
                if (msg.FrameCount != 4) continue;

                var cmd = msg[2].ConvertToString();

                switch (cmd)
                {
                    case Traffic.DAT:
                        await stream.WriteAsync(msg[3].ToByteArray(), cancellationToken);
                        break;

                    case Traffic.EOF:
                        return;

                    case Traffic.ERR:
                        logger.LogError("Remote error for {ConnId}: {Reason}", 
                            connId, msg[3].ConvertToString());
                        return;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RelayFromZmq failed for {ConnId}", connId);
        }
    }

    /// <summary>
    /// Relays data from NetworkStream → ZMQ (Client upstream)
    /// </summary>
    public static async Task RelayToZmqAsync(string connId, 
        NetworkStream stream,
        ChannelWriter<NetMQMessage> outboundWriter,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        var buffer = new byte[DefaultBufferSize];

        try
        {
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                var dat = new NetMQMessage(4);
                dat.Append(connId);
                dat.AppendEmptyFrame();
                dat.Append(Traffic.DAT);
                dat.Append(new NetMQFrame(buffer, bytesRead));

                await outboundWriter.WriteAsync(dat, cancellationToken);
            }

            // send EOF when local client closes write side
            var eof = new NetMQMessage(4);
            eof.Append(connId);
            eof.AppendEmptyFrame();
            eof.Append(Traffic.EOF);
            eof.AppendEmptyFrame();
            await outboundWriter.WriteAsync(eof, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex) when (ex is not ObjectDisposedException)
        {
            logger.LogError(ex, "RelayToZmq failed for {connId}", connId);
        }
    }
}