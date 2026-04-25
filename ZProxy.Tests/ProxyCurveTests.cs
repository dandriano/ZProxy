using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NetMQ;
using NUnit.Framework.Internal;

namespace ZProxy.Tests;

[TestFixture]
public class ProxyCurveTests
{
    private readonly ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;

    [Test]
    public async Task CanProxyTraffic()
    {
                // ------- 1. Curve certificates -------
        var serverCert = new NetMQCertificate();
        byte[] serverPublicKey = serverCert.PublicKey;
        var clientCert = new NetMQCertificate();

        // ------- 2. Free TCP port for ZMQ transport -------
        int zmqPort = GetFreeTcpPort();
        string serverEndpoint = $"tcp://127.0.0.1:{zmqPort}";

        // ------- 3. Start a minimal HTTP server (the target) -------
        var httpCts = new CancellationTokenSource();
        (var httpListener, int httpPort, string expectedBody) =
            await StartHttpServerAsync(httpCts.Token);

        // ------- 4. Create ZProxy server & client -------
        int socksPort = GetFreeTcpPort();
        var socksHandler = new Socks5InboundHandler(socksPort, _loggerFactory.CreateLogger<Socks5InboundHandler>());
        var server = new Server(serverEndpoint, serverCert, _loggerFactory.CreateLogger<Server>());
        var client = new Client(serverEndpoint, serverPublicKey, clientCert,
                                socksHandler, _loggerFactory.CreateLogger<Client>());

        // ------- 5. Run everything inside NetMQRuntime (required for async sockets) -------
        Exception? runtimeException = null;
        var runtimeReady = new ManualResetEventSlim(false);

        var runtimeThread = new Thread(() =>
        {
            try
            {
                using var runtime = new NetMQRuntime();
                    runtimeReady.Set();
                    runtime.Run(server.RunAsync(), client.RunAsync());
                }
            catch (Exception ex)
            {
                runtimeException = ex;
            }
        });
        runtimeThread.Start();
        runtimeReady.Wait();

        await Task.Delay(300); // let sockets bind / connect

        try
        {
            // ------- 6. SOCKS5 client connects through the proxy -------
            using var socksClient = new TcpClient();
            await socksClient.ConnectAsync(IPAddress.Loopback, socksPort);
            var socksStream = socksClient.GetStream();

            bool handshakeOk = await Socks5ConnectAsync(socksStream, "127.0.0.1", httpPort);
            Assert.That(handshakeOk, Is.True, "SOCKS5 handshake failed");

            // ------- 7. Send real HTTP GET and read response -------
            string request = $"GET / HTTP/1.1\r\nHost: 127.0.0.1:{httpPort}\r\nConnection: close\r\n\r\n";
            byte[] requestBytes = Encoding.ASCII.GetBytes(request);
            await socksStream.WriteAsync(requestBytes, 0, requestBytes.Length);
            await socksStream.FlushAsync();

            // Close the sending side to signal EOF and let the proxy complete the data relay
            socksClient.Client.Shutdown(SocketShutdown.Send);

            using var reader = new StreamReader(socksStream, Encoding.ASCII);
            string fullResponse = await reader.ReadToEndAsync();

            // ------- 8. Verify HTTP response -------
            Assert.That(fullResponse, Does.Contain("200 OK"), "Expected 200 status");
            Assert.That(fullResponse, Does.Contain(expectedBody), "Response body should be echoed");
        }
        finally
        {
            // ------- 9. Graceful shutdown -------
            await client.DisposeAsync();
            await server.DisposeAsync();
            httpCts.Cancel();
        }

        runtimeThread.Join(TimeSpan.FromSeconds(5));
        if (runtimeException != null)
            throw new AggregateException("NetMQRuntime thread crashed", runtimeException);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int GetFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>Minimal HTTP/1.1 server that returns a fixed response and closes the connection.</summary>
    private static async Task<(TcpListener Listener, int Port, string Body)> StartHttpServerAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        string body = "Hello from ZProxy HTTP test!";
        string response = $"HTTP/1.1 200 OK\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n{body}";

        _ = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try { client = await listener.AcceptTcpClientAsync(); }
                catch { break; }

                _ = HandleHttpConnection(client, response, ct);
            }
        }, ct);

        return (listener, port, body);
    }

    private static async Task HandleHttpConnection(TcpClient client, string response, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            // Read until \r\n\r\n (end of request headers)
            byte[] buf = new byte[4096];
            int total = 0;
            while (total < buf.Length)
            {
                int read = await stream.ReadAsync(buf, total, 1, ct);
                if (read == 0) break;
                total++;
                if (total >= 4 &&
                    buf[total - 4] == '\r' && buf[total - 3] == '\n' &&
                    buf[total - 2] == '\r' && buf[total - 1] == '\n')
                    break;
            }

            byte[] respBytes = Encoding.ASCII.GetBytes(response);
            await stream.WriteAsync(respBytes, ct);
            await stream.FlushAsync(ct);
        }
    }

    /// <summary>Minimal SOCKS5 CONNECT (no authentication).</summary>
    private static async Task<bool> Socks5ConnectAsync(NetworkStream stream, string host, int port)
    {
        // Greeting
        await stream.WriteAsync(new byte[] { 0x05, 0x01, 0x00 }, 0, 3);
        await stream.FlushAsync();
        var resp = new byte[2];
        if (await stream.ReadAsync(resp, 0, 2) < 2) return false;
        if (resp[0] != 0x05 || resp[1] != 0x00) return false;

        // CONNECT request (domain name ATYP)
        byte[] hostBytes = Encoding.ASCII.GetBytes(host);
        using var ms = new MemoryStream();
        ms.WriteByte(0x05);                 // VER
        ms.WriteByte(0x01);                 // CMD CONNECT
        ms.WriteByte(0x00);                 // RSV
        ms.WriteByte(0x03);                 // ATYP = domain
        ms.WriteByte((byte)hostBytes.Length);
        ms.Write(hostBytes, 0, hostBytes.Length);
        ms.WriteByte((byte)(port >> 8));
        ms.WriteByte((byte)(port & 0xFF));

        await stream.WriteAsync(ms.ToArray(), 0, (int)ms.Length);
        await stream.FlushAsync();

        var reply = new byte[10];
        if (await stream.ReadAsync(reply, 0, 10) < 10) return false;
        return reply[0] == 0x05 && reply[1] == 0x00;
    }
}