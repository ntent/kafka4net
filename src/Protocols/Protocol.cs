using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Metadata;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    internal class Protocol
    {
        private static readonly ILogger _log = Logger.GetLogger();

        private readonly Cluster _cluster;
        private readonly Action<Exception, TcpClient> _onError;
        private readonly Tuple<string, int>[] _seedAddresses;

        // It is possible to have id per connection, but it's just simpler code and debug/tracing when it's global
        private static int _correlationId;
        private readonly ConcurrentDictionary<int,Action<byte[],Exception>> _corelationTable = new ConcurrentDictionary<int, Action<byte[],Exception>>();

        internal Protocol(Cluster cluster, string seedConnections, Action<Exception,TcpClient> onError = null)
        {
            _cluster = cluster;
            _onError = onError;
            _seedAddresses = Connection.ParseAddress(seedConnections);
        }

        internal async Task<MetadataResponse> ConnectAsync()
        {
            var meta = await StartInitialConnectAsync();
            if (meta == null)
            {
                //_log.FatalFormat("Could not connect to any of seed addresses");
                //return null;
                throw new BrokerException("Could not connect to any of seed addresses");
            }
            return meta;
        }

        /// <summary>
        /// Parallel query all seed brokers and listen to the first successful response with all
        /// topics configuration.
        /// </summary>
        /// <returns></returns>
        private async Task<MetadataResponse> StartInitialConnectAsync()
        {
            return await _seedAddresses.
                Select(addr => Observable.StartAsync(async cancel =>
                {
                    var client = new TcpClient { NoDelay = true };
                    //cancel.Register(client.Close);
                    await client.ConnectAsync(addr.Item1, addr.Item2);
                    //if (cancel.IsCancellationRequested)
                    //    return null;
                    _log.Debug("Connected to {0}:{1}", addr.Item1, addr.Item2);
                    CorrelateResponseLoop(client, cancel);
                    var meta = await LoadAllTopicsMeta(client);
                    return meta;
                })).
                Merge().
                FirstOrDefaultAsync().
                ToTask();
        }

        internal async Task CorrelateResponseLoop(TcpClient client, CancellationToken cancel)
        {
            try
            {
                _log.Debug("Starting reading loop from socket");
                // TODO: if corrup message, dump bin log of 100 bytes for further investigation
                while (client.Connected)
                {
                    try
                    {
                        // read message size
                        var buff = new byte[4];
                        var read = await client.GetStream().ReadAsync(buff, 0, 4, cancel);
                        if (cancel.IsCancellationRequested)
                        {
                            _log.Debug("Stopped reading from {0} because socket cancelled", client.Client.RemoteEndPoint);
                            return;
                        }

                        // TODO: what to do if read<4 ? Recycle connection?
                        if (read == 0)
                        {
                            _log.Info("Server closed connection");
                            client.Close();
                            return;
                        }
                        var size = BigEndianConverter.ToInt32(buff);
                        // read message body
                        var body = new byte[size];
                        var pos = 0;
                        var left = size;
                        do
                        {
                            read = await client.GetStream().ReadAsync(body, pos, left);
                            if (read == 0)
                            {
                                _log.Info("Server closed connection");
                                client.Close();
                                return;
                            }
                            //_log.Debug("Read/expected/total {0}/{1}/{2}", read, left, size);
                            pos += read;
                            left -= read;
                        } while (left > 0);

                        if (client.Available > 0)
                            _log.Debug("Still available {0} bytes", client.Available);

                        try
                        {
                            int correlationId = -1;
                            // TODO: check read==size && read > 4
                            correlationId = BigEndianConverter.ToInt32(body);
                            //_log.Debug("<-{0}\n {1}", correlationId, FormatBytes(body));
                            //_log.Debug("<-{0}", correlationId);

                            // find correlated action
                            Action<byte[], Exception> handler;
                            // TODO: if correlation id is not found, there is a chance of corrupt 
                            // connection. Maybe recycle the connection?
                            if (!_corelationTable.TryRemove(correlationId, out handler))
                            {
                                _log.Error("Unknown correlationId: " + correlationId);
                                continue;
                            }
                            handler(body, null);
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex, "Error with handling message. Message bytes:\r\n{0}", FormatBytes(body));
                            throw;
                        }
                    }
                    catch (SocketException e)
                    {
                        // shorter version of socket exception, without stack trace dump
                        _log.Info("CorrelationLoop socket exception. {0}", e.Message);
                        throw;
                    }
                    catch (Exception e)
                    {
                        _log.Error(e, "CorrelateResponseLoop error");
                        if (_onError != null)
                            _onError(e, client);
                        throw;
                    }
                }

                try
                {
                    _log.Debug("Finished reading loop from socket");
                }
                catch (ObjectDisposedException)
                {
                }
            }
            catch(Exception e)
            {
                _corelationTable.Values.ForEach(c => c(null, e));
                if (_onError != null)
                    _onError(e, client);
                throw;
            }
            finally
            {
                _corelationTable.Values.ForEach(c => c(null, new TaskCanceledException("Correlation loop closed")));
            }
        }

        private async Task<T> SendAndCorrelateAsync<T>(Func<int, byte[]> serialize, Func<byte[], T> deserialize, TcpClient tcp, CancellationToken cancel)
        {
            var correlationId = Interlocked.Increment(ref _correlationId);

            var callback = new TaskCompletionSource<T>();
            // TODO: configurable timeout
            // TODO: coordinate with Message's timeout
            // TODO: set exception in case of tcp socket exception and end of correlation loop
            //var timeout = new CancellationTokenSource(5 * 1000);
            try
            {
                var res = _corelationTable.TryAdd(correlationId, (body, ex) =>
                {
                    if(ex == null)
                        callback.TrySetResult(deserialize(body));
                    else
                        callback.TrySetException(ex);
                    //timeout.Dispose();
                });
                if (!res)
                    throw new ApplicationException("Failed to add correlationId: " + correlationId);
                var buff = serialize(correlationId);
                //_log.Debug("{0}->\n{1}", correlationId, FormatBytes(buff));
                //_log.Debug("{0}->", correlationId);
                await tcp.GetStream().WriteAsync(buff, 0, buff.Length, cancel);
            }
            catch (Exception e)
            {
                callback.TrySetException(e);
                if (_onError != null)
                    _onError(e, tcp);
            }

            // TODO: is stream safe to use after timeout?
            cancel.Register(() => callback.TrySetCanceled(), useSynchronizationContext: false);
            return await callback.Task;
        }

        internal async Task<ProducerResponse> ProduceRaw(ProduceRequest request, CancellationToken cancel)
        {
            var client = await request.Broker.Conn.GetClientAsync();
            var response = await SendAndCorrelateAsync(
                id => Serializer.Serialize(request, id),
                Serializer.GetProducerResponse,
                client, cancel);
            
            if(response.Topics.Any(t => t.Partitions.Any(p => p.ErrorCode != ErrorCode.NoError)))
                _log.Debug("_");

            return response;
        }

        internal async Task<ProducerResponse> Produce(ProduceRequest request)
        {
            var conn = request.Broker.Conn;
            var client = await conn.GetClientAsync();
            _log.Debug("Sending ProduceRequest to {0}, Request: {1}", conn, request);
            var response = await SendAndCorrelateAsync(
                id => Serializer.Serialize(request, id),
                Serializer.GetProducerResponse,
                client,
                CancellationToken.None
            );
            _log.Debug("Got ProduceResponse: {0}", response);

            return response;
        }

        internal async Task<MetadataResponse> MetadataRequest(TopicRequest request, BrokerMeta broker = null)
        {
            var tcp = await (broker != null ? broker.Conn.GetClientAsync() : _cluster.GetAnyClientAsync());
            _log.Debug("Sending MetadataRequest to {0}", tcp.Client.RemoteEndPoint);

            // TODO: handle result
            return await SendAndCorrelateAsync(
                id => Serializer.Serialize(request, id),
                Serializer.DeserializeMetadataResponse,
                tcp, CancellationToken.None);
        }

        internal async Task<OffsetResponse> GetOffsets(OffsetRequest req, Connection conn)
        {
            var tcp = await conn.GetClientAsync();
            if(_log.IsDebugEnabled)
                _log.Debug("Sending OffsetRequest to {0}. request: {1}", tcp.Client.RemoteEndPoint, req);
            return await SendAndCorrelateAsync(
                id => Serializer.Serialize(req, id),
                Serializer.DeserializeOffsetResponse,
                tcp, CancellationToken.None);
        }

        internal async Task<FetchResponse> Fetch(FetchRequest req, Connection conn)
        {
            _log.Debug("Sending FetchRequest {0}", req);
            
            // Detect disconnected server. Wait no less than 5sec. 
            // If wait time exceed wait time + 3sec, consider it a timeout too
            var timeout = Math.Max(5000, req.MaxWaitTime + 3000);
            var cancel = new CancellationTokenSource(timeout);

            var tcp = await conn.GetClientAsync();
            var response = await SendAndCorrelateAsync(
                id => Serializer.Serialize(req, id),
                Serializer.DeserializeFetchResponse,
                tcp, cancel.Token);

            if(response.Topics.Length > 0 && _log.IsDebugEnabled)
                _log.Debug("Got fetch response from {0} Response: {1}", conn, response);

            return response;
        }

        // TODO: move to Cluster
        Task<MetadataResponse> LoadAllTopicsMeta(TcpClient client)
        {
            _log.Debug("Loading all metadata...");
            var request = new TopicRequest();
            return SendAndCorrelateAsync(c => Serializer.Serialize(request, c), Serializer.DeserializeMetadataResponse, client, CancellationToken.None);
        }

        private static string FormatBytes(byte[] buff)
        {
            return buff.Aggregate(new StringBuilder(), (builder, b) => builder.Append(b.ToString("x2")),
                str => str.ToString());
        }
    }
}
