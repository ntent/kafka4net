using System;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Protocols;

namespace kafka4net
{
    class Connection
    {
        readonly string _host;
        readonly int _port;
        private readonly Protocol _protocol;
        TcpClient _client;
        public ConnState State;
        static readonly ILogger _log = Logger.GetLogger();

        public Connection(string host, int port, Protocol protocol)
        {
            _host = host;
            _port = port;
            _protocol = protocol;
        }


        public static Tuple<string, int>[] ParseAddress(string seedConnections)
        {
            return seedConnections.Split(',').
                Select(_ => _.Trim()).
                Where(_ => _ != null).
                Select(s =>
                {
                    int port = 9092;
                    string host = null;
                    if (s.Contains(':'))
                    {
                        var parts = s.Split(new[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length == 2)
                        {
                            host = parts[0];
                            port = int.Parse(parts[1]);
                        }
                    }
                    else
                    {
                        host = s;
                    }
                    return Tuple.Create(host, port);
                }).ToArray();
        }

        public async Task<TcpClient> GetClient()
        {
            if (_client != null && !_client.Connected)
            {
                _log.Debug("Replacing closed connection {0} with a new one", _client.Client.RemoteEndPoint);
                _client = null;
            }

            if (_client == null)
            {
                _log.Debug("Opening new connection {0}:{1}", _host, _port);

                State = ConnState.Connecting;
                _client = new TcpClient();
                await _client.ConnectAsync(_host, _port);
                // TODO: Who and when is going to cancell reading?
                _protocol.CorrelateResponseLoop(_client, CancellationToken.None);
                State = ConnState.Connected;
                return _client;
            }

            return _client;
        }

        public override string ToString()
        {
            return string.Format("Connection: {0}:{1} connected:{2}", _host, _port, _client.Connected);
        }
    }
}
