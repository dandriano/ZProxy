using System.IO;
using NetMQ;

namespace ZProxy;

public static class Cert
{
    public static void SaveCert(NetMQCertificate cert, string publicKeyPath, string secretKeyPath)
    {
        File.WriteAllText(publicKeyPath, cert.PublicKeyZ85);
        File.WriteAllText(secretKeyPath, cert.SecretKeyZ85);
    }
    public static NetMQCertificate LoadCert(string publicKeyPath)
    {
        var z85 = File.ReadAllText(publicKeyPath).Trim();
        return NetMQCertificate.FromPublicKey(z85);
    }
    public static NetMQCertificate LoadCert(string publicKeyPath, string secretKeyPath)
    {
        var pub = File.ReadAllText(publicKeyPath).Trim();
        var sec = File.ReadAllText(secretKeyPath).Trim();
        return new NetMQCertificate(sec, pub);
    }

    public static NetMQCertificate GenerateCert(string publicKeyPath = "", string secretKeyPath = "")
    {
        var cert = new NetMQCertificate();
    if (!string.IsNullOrEmpty(secretKeyPath) && !string.IsNullOrEmpty(publicKeyPath))
            SaveCert(cert, publicKeyPath, secretKeyPath);
        return cert;
    }
}