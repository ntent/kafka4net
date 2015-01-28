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
using kafka4net.Tracing;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    // TODO: can this class be made static?
    internal class Protocol
    {
        private static readonly ILogger _log = Logger.GetLogger();
        private readonly Cluster _cluster;
        static readonly EtwTrace _etw = EtwTrace.Log;

        internal Protocol(Cluster cluster)
        {
            _cluster = cluster;
        }

        internal async Task<ProducerResponse> Produce(ProduceRequest request)
        {
            var conn = request.Broker.Conn;
            var client = await conn.GetClientAsync();
            _log.Debug("Sending ProduceRequest to {0}, Request: {1}", conn, request);
            if(_etw.IsEnabled())
                _etw.ProtocolProduceRequest(request.ToString(), request.Broker.NodeId);

            var response = await conn.Correlation.SendAndCorrelateAsync(
                id => Serializer.Serialize(request, id),
                Serializer.GetProducerResponse,
                client,
                CancellationToken.None
            );
            _log.Debug("Got ProduceResponse: {0}", response);
            if (_etw.IsEnabled())
                _etw.ProtocolProduceResponse(response.ToString(), request.Broker.NodeId);

            return response;
        }

        internal async Task<MetadataResponse> MetadataRequest(TopicRequest request, BrokerMeta broker = null)
        {
            TcpClient tcp;
            Connection conn;

            if (broker != null)
            {
                conn = broker.Conn;
                tcp = await conn.GetClientAsync();
            }
            else
            {
                var clientAndConnection = await _cluster.GetAnyClientAsync();
                conn = clientAndConnection.Item1;
                tcp = clientAndConnection.Item2;
            }

            //var tcp = await (broker != null ? broker.Conn.GetClientAsync() : _cluster.GetAnyClientAsync());
            _log.Debug("Sending MetadataRequest to {0}", tcp.Client.RemoteEndPoint);
            if (_etw.IsEnabled())
            {
                _etw.ProtocolMetadataRequest(request.ToString());
            }

            var response = await conn.Correlation.SendAndCorrelateAsync(
                id => Serializer.Serialize(request, id),
                Serializer.DeserializeMetadataResponse,
                tcp, CancellationToken.None);

            if (_etw.IsEnabled())
            {
                _etw.ProtocolMetadataResponse(response.ToString(), 
                    broker != null ? broker.Host : "", 
                    broker != null ? broker.Port : -1,
                    broker != null ? broker.NodeId : -1);
            }


            return response;
        }

        internal async Task<OffsetResponse> GetOffsets(OffsetRequest req, Connection conn)
        {
            var tcp = await conn.GetClientAsync();
            
            if(_etw.IsEnabled())
                _etw.ProtocolOffsetRequest(req.ToString());

            var response = await conn.Correlation.SendAndCorrelateAsync(
                id => Serializer.Serialize(req, id),
                Serializer.DeserializeOffsetResponse,
                tcp, CancellationToken.None);
            
            _log.Debug("Got OffsetResponse {0}", response);
            if (_etw.IsEnabled())
                _etw.ProtocolOffsetResponse(response.ToString());
            
            return response;
        }

        internal async Task<FetchResponse> Fetch(FetchRequest req, Connection conn)
        {
            _log.Debug("Sending FetchRequest to broker {1}. Request: {0}", req, conn);
            if (_etw.IsEnabled())
                _etw.ProtocolFetchRequest(req.ToString());
            
            // Detect disconnected server. Wait no less than 5sec. 
            // If wait time exceed wait time + 3sec, consider it a timeout too
            //var timeout = Math.Max(5000, req.MaxWaitTime + 3000);
            //var cancel = new CancellationTokenSource(timeout);

            var tcp = await conn.GetClientAsync();
            var response = await conn.Correlation.SendAndCorrelateAsync(
                id => Serializer.Serialize(req, id),
                Serializer.DeserializeFetchResponse,
                tcp, /*cancel.Token*/ CancellationToken.None);

            if (_etw.IsEnabled())
                _etw.ProtocolFetchResponse(response.ToString());

            return response;
        }
    }
}
