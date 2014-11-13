using System;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Protocols;

namespace kafka4net
{
    internal class Connection
    {
        private static readonly ILogger _log = Logger.GetLogger();

        public ConnState State;

        private readonly string _host;
        private readonly int _port;
        private readonly Protocol _protocol;
        private readonly Action<Exception> _onError;
        private TcpClient _client;
        readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);

        internal Connection(string host, int port, Protocol protocol, Action<Exception> onError = null)
        {
            _host = host;
            _port = port;
            _protocol = protocol;
            _onError = onError;
        }


        internal static Tuple<string, int>[] ParseAddress(string seedConnections)
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

        internal async Task<TcpClient> GetClientAsync()
        {
            await _connectionLock.WaitAsync();
            try
            {
                if (_client != null && !_client.Connected)
                {
                    _log.Debug("Replacing closed connection {0}:{1} with a new one", _host, _port);
                    _client = null;
                }

                if (_client == null)
                {
                    _log.Debug("Opening new connection {0}:{1}", _host, _port);

                    State = ConnState.Connecting;
                    _client = new TcpClient();
                    try
                    {
                        await _client.ConnectAsync(_host, _port);
                    }
                    catch (Exception e)
                    {

                    }
                    // TODO: Who and when is going to cancel reading?
                    var loopTask = _protocol.CorrelateResponseLoop(_client, CancellationToken.None);

                    // Close connection in case of any exception. It is important, because in case of deserialization exception,
                    // we are out of sync and can't continue.
                    loopTask.ContinueWith(async t => 
                    {
                        _log.Debug("CorrelationLoop errored. Closing connection because of error. {0}", t.Exception.Message);
                        await _connectionLock.WaitAsync();
                        try
                        {
                            if (_client != null)
                                _client.Close();
                        }
                        finally 
                        { 
                            _client = null;
                            _connectionLock.Release();
                        }
                    }, TaskContinuationOptions.OnlyOnFaulted);

                    State = ConnState.Connected;
                    return _client;
                }
            }
            catch(Exception e)
            {
                // some exception getting a connection, clear what we have for next time.
                _client = null;
                if (_onError != null)
                    _onError(e);
                throw;
            }
            finally
            {
                _connectionLock.Release();
            }

            return _client;
        }

        public override string ToString()
        {
            return string.Format("Connection: {0}:{1}", _host, _port);
        }

        internal bool OwnsClient(TcpClient tcp)
        {
            return _client == tcp;
        }

        public async Task MarkSocketAsFailed(TcpClient tcp)
        {
            await _connectionLock.WaitAsync();
            try
            {
                if (_client != tcp)
                    return;

                _log.Debug("Marking connection as failed. {0}", this);

                if (_client != null)
                    try { _client.Close(); }
                    catch { /*empty*/ }
                _client = null;
            }
            finally
            {
                _connectionLock.Release();
            }
        }
    }
}
