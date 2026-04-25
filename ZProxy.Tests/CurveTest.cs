using NetMQ;
using NetMQ.Sockets;

namespace ZProxy.Tests;

[TestFixture]
public class CurveTest
{
    [Test]
    public void CanSendReciveCurveMessages()
    {
        var serverKeys = new NetMQCertificate();
        using var server = new DealerSocket();
        server.Options.CurveServer = true;
        server.Options.CurveCertificate = serverKeys;
        server.Bind($"tcp://localhost:55367");
        
        var clientKeys = new NetMQCertificate();
        using var client = new DealerSocket();
        client.Options.CurveServerKey = serverKeys.PublicKey;
        client.Options.CurveCertificate = clientKeys;
        client.Connect("tcp://localhost:55367");

        for (var i = 0; i < 25; i++)
        {
            var clientSendedFrame = $"ClientSecretMessage{i}";
            client.SendFrame(clientSendedFrame);

            var serverReceivedFrame = server.ReceiveFrameString();
            Assert.That(serverReceivedFrame, Is.EqualTo(clientSendedFrame));
            
            var serverSendedFrame = $"ServerSecretMessage{i}";
            server.SendFrame(serverSendedFrame);

            var clientReceivedFrame = client.ReceiveFrameString();
            Assert.That(clientReceivedFrame, Is.EqualTo(serverSendedFrame));
        }
    }
}