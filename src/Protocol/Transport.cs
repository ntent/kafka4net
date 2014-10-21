using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Metadata;
using kafka4net.Protocol.Requests;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net.Protocol
{
    class Transport
    {
        private readonly Router _router;
        static readonly ILogger _log = Logger.GetLogger();
        readonly Tuple<string, int>[] _seedAddresses;
        // It is possible to have id per connection, but it's just simpler code and debug/tracing when it's global
        static int _correlationId;
        readonly ConcurrentDictionary<int,Action<byte[]>> _corelationTable = new ConcurrentDictionary<int, Action<byte[]>>();

        public Transport(Router router, string seedConnections)
        {
            _router = router;
            _seedAddresses = Connection.ParseAddress(seedConnections);
        }

        public async Task<MetadataResponse> ConnectAsync()
        {
            var meta = await StartInitialConnect();
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
        async Task<MetadataResponse> StartInitialConnect()
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

        internal async void CorrelateResponseLoop(TcpClient client, CancellationToken cancel)
        {
            _log.Debug("Starting reading loop from socket {0}", client.Client.RemoteEndPoint);
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
                        }
                        //_log.Debug("Read/expected/total {0}/{1}/{2}", read, left, size);
                        pos += read;
                        left -= read;
                    } while (left > 0);

                    if (client.Available > 0)
                        _log.Debug("Still available {0} bytes", client.Available);

                    // TODO: check read==size && read > 4
                    var correlationId = BigEndianConverter.ToInt32(body);
                    //_log.Debug("<-{0}\n {1}", correlationId, FormatBytes(body));
                    //_log.Debug("<-{0}", correlationId);

                    // find correlated action
                    Action<byte[]> handler;
                    // TODO: if correlation id is not found, there is a chance of corrupt 
                    // connection. Maybe recycle the connection?
                    if (!_corelationTable.TryGetValue(correlationId, out handler))
                    {
                        _log.Error("Unknown correlationId: " + correlationId);
                        continue;
                    }
                    handler(body);
                    // TODO: remove id from correlation table
                }
                catch (Exception e)
                {
                    // TODO: it seems exception here is not propagated all the way back to subscriber
                    _log.Error(e, "CorrelateResponseLoop error");
                }
            }
            _log.Debug("Finished reading loop from socket {0}", client.Client.RemoteEndPoint);
        }

        async Task<T> SendAndCorrelate<T>(Func<int, byte[]> serialize, Func<byte[], T> deserialize, TcpClient tcp, CancellationToken cancel)
        {
            var correlationId = Interlocked.Increment(ref _correlationId);

            var callback = new TaskCompletionSource<T>();
            // TODO: configurable timeout
            // TODO: coordinate with Message's timeout
            //var timeout = new CancellationTokenSource(5 * 1000);
            try
            {
                var res = _corelationTable.TryAdd(correlationId, body =>
                {
                    callback.TrySetResult(deserialize(body));
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
            }

            // TODO: is stream safe to use after timeout?
            cancel.Register(() => callback.TrySetCanceled(), useSynchronizationContext: false);
            return await callback.Task;
        }

        internal async Task<ProducerResponse> ProduceRaw(ProduceRequest request, CancellationToken cancel)
        {
            var client = await request.Broker.Conn.GetClient();
            var response = await SendAndCorrelate(
                id => Serializer.Serialize(request, id),
                Serializer.GetProducerResponse,
                client, cancel);
            
            if(response.Topics.Any(t => t.Partitions.Any(p => p.ErrorCode != ErrorCode.NoError)))
                _log.Debug("_");

            return response;
        }

        internal Task Produce(IEnumerable<ProduceRequest> requests)
        {
            // Avoid multiple enumeration of "requests"
            var materializedRequest = requests.ToArray();

            // TODO: handle result
            var tasks = materializedRequest.Select(async request =>
            {
                ProducerResponse response;
                try
                {
                    var conn = request.Broker.Conn;
                    var client = await conn.GetClient();
                    _log.Debug("Sending ProduceRequest to {0}, Request: {1}", conn, request);
                    response = await SendAndCorrelate(
                        id => Serializer.Serialize(request, id),
                        Serializer.GetProducerResponse,
                        client,
                        CancellationToken.None
                    );
                    _log.Debug("Got ProduceResponse: {0}", response);
                    if (response.Topics.Any(t => t.Partitions.Any(p => p.ErrorCode != ErrorCode.NoError)))
                        _log.Debug("_");
                }
                catch (SocketException e)
                {
                    _router.OnTransportError(request, e);
                    return;
                }

                (
                    from req in materializedRequest
                    from topic in req.TopicData
                    from part in topic.PartitionsData
                    // correlate per-partition result errors to source per-partition messages
                    join err in (
                        from topic in response.Topics
                        from part in topic.Partitions
                        select new {topic.TopicName, part.Partition, part.ErrorCode}
                    ) on new {topic.TopicName, part.Partition} equals new {err.TopicName, err.Partition}
                    select new {topic.TopicName, part.Pub, part.OriginalMessages, err.ErrorCode, err.Partition }
                 ).ForEach(p => {
                     if (p.ErrorCode == ErrorCode.NoError)
                     {
                         if (p.Pub.OnSuccess != null)
                             SafeCallback(() => p.Pub.OnSuccess(p.OriginalMessages));
                     }
                     else
                     {
                         // TODO: if its a temp error, route messages to delay queue
                         // TODO: what if exception? What if recoverable exception (connection lost?)
                         if(p.Pub.OnPermError != null) 
                         {
                             // TODO: create dedicated PartitionException
                             var error = string.Format("Error sending data. Error code: {0}; topic: '{1}', partition: {2}", p.ErrorCode, p.TopicName, p.Partition);
                             SafeCallback(() => p.Pub.OnPermError(new BrokerException(error), p.OriginalMessages));
                         }
                     }
                 });

            });

            return Task.WhenAll(tasks);
        }

        internal async Task<MetadataResponse> MetadataRequest(TopicRequest request, BrokerMeta broker = null)
        {
            var tcp = await (broker != null ? broker.Conn.GetClient() : _router.GetAnyClient());
            _log.Debug("Sending MetadataRequest to {0}", tcp.Client.RemoteEndPoint);

            // TODO: handle result
            return await SendAndCorrelate(
                id => Serializer.Serialize(request, id),
                Serializer.DeserializeMetadataResponse,
                tcp, CancellationToken.None);
        }

        internal async Task<OffsetResponse> GetOffsets(OffsetRequest req, Connection conn)
        {
            var tcp = await conn.GetClient();
            if(_log.IsDebugEnabled)
                _log.Debug("Sending OffsetRequest to {0}. request: {1}", tcp.Client.RemoteEndPoint, req);
            return await SendAndCorrelate(
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

            var tcp = await conn.GetClient();
            var response = await SendAndCorrelate(
                id => Serializer.Serialize(req, id),
                Serializer.DeserializeFetchResponse,
                tcp, cancel.Token);

            if(response.Topics.Length > 0 && _log.IsDebugEnabled)
                _log.Debug("Got fetch response from {0} Response: {1}", conn, response);

            return response;
        }

        // TODO: move to router
        Task<MetadataResponse> LoadAllTopicsMeta(TcpClient client)
        {
            _log.Debug("Loading all metadata...");
            var request = new TopicRequest();
            return SendAndCorrelate(c => Serializer.Serialize(request, c), Serializer.DeserializeMetadataResponse, client, CancellationToken.None);
        }

        static void SafeCallback(Action action)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                _log.Error("Error when executing callback. {0}\n{1}", e.Message, e.StackTrace);
            }
        }

        //static string FormatBytes(byte[] buff)
        //{
        //    return buff.Aggregate(new StringBuilder(), (builder, b) => builder.Append(b.ToString("x2")), 
        //        str => str.ToString());
        //}

    }
}
