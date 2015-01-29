using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Runtime.InteropServices;
using kafka4net.Metadata;
using kafka4net.Protocols.Requests;

namespace kafka4net.Tracing
{
    [EventSource(Name = "kafka4net")]
    public sealed class EtwTrace : EventSource
    {
        public static EtwTrace Log = new EtwTrace();

        public class Opcodes
        {
            // Shared
            public const EventOpcode Create = (EventOpcode)200;
            public const EventOpcode Starting = (EventOpcode)201;
            public const EventOpcode Started = (EventOpcode)202;
            public const EventOpcode Stopping = (EventOpcode)203;
            public const EventOpcode Stopped = (EventOpcode)204;

            // Fetcher
            public const EventOpcode CancelSentWakeup = (EventOpcode)12;
            public const EventOpcode PartitionSubscribed = (EventOpcode)13;
            public const EventOpcode FetchResponse = (EventOpcode)14;
            public const EventOpcode Message = (EventOpcode)15;
            public const EventOpcode Sleep = (EventOpcode)16;
            public const EventOpcode Wakeup = (EventOpcode)17;
            public const EventOpcode FetchRequest = (EventOpcode)18;

            // Connection
            public const EventOpcode Connecting = (EventOpcode)19;
            public const EventOpcode Connected = (EventOpcode)20;
            public const EventOpcode Errored = (EventOpcode)21;
            public const EventOpcode Disconnected = (EventOpcode)22;
            public const EventOpcode ReplaceClosedClient = (EventOpcode)23;
            public const EventOpcode MarkSocketAsFailedCorrelationLoopCancelling = (EventOpcode)24;
            public const EventOpcode MarkSocketAsFailedTcpClosing = (EventOpcode)25;

            // Correlation
            public const EventOpcode ReadingMessageSize = (EventOpcode)26;
            public const EventOpcode ServerClosedConnection = (EventOpcode)27;
            public const EventOpcode ReadMessageSize = (EventOpcode)28;
            public const EventOpcode ReadingBodyChunk = (EventOpcode)29;
            public const EventOpcode ReadBodyChunk = (EventOpcode)30;
            public const EventOpcode ReadBody = (EventOpcode)31;
            public const EventOpcode ReceivedCorrelationId = (EventOpcode)32;
            public const EventOpcode ExecutingHandler = (EventOpcode)33;
            public const EventOpcode ExecutedHandler = (EventOpcode)34;
            public const EventOpcode Error = (EventOpcode)35;
            public const EventOpcode Complete = (EventOpcode)36;
            public const EventOpcode WritingMessage = (EventOpcode)37;

            // RecoveryMonitor
            public const EventOpcode PartitionRecovered = (EventOpcode)38;
            public const EventOpcode PartitionFailed = (EventOpcode)39;
            public const EventOpcode PartitionFailedAgain = (EventOpcode)40;
            public const EventOpcode RecoveryLoopStarted = (EventOpcode)41;
            public const EventOpcode SendingPing = (EventOpcode)42;
            public const EventOpcode PingResponse = (EventOpcode)43;
            public const EventOpcode PingFailed = (EventOpcode)44;
            public const EventOpcode PossiblyHealedPartitions = (EventOpcode)45;
            public const EventOpcode NoHealedPartitions = (EventOpcode)46;
            public const EventOpcode CheckingBrokerAccessibility = (EventOpcode)47;
            public const EventOpcode BrokerIsAccessible = (EventOpcode)48;
            public const EventOpcode HealedPartitions = (EventOpcode)49;
            public const EventOpcode RecoveryLoopStop = (EventOpcode)50;
            
            // Protcol
            public const EventOpcode MetadataRequest = (EventOpcode)51;
            public const EventOpcode MetadataResponse = (EventOpcode)52;
            public const EventOpcode ProduceRequest = (EventOpcode)53;
            public const EventOpcode ProduceResponse = (EventOpcode)54;
            public const EventOpcode OffsetRequest = (EventOpcode)55;
            public const EventOpcode OffsetResponse = (EventOpcode)56;

            // Producer
            public const EventOpcode PermanentFailure = (EventOpcode)57;
            public const EventOpcode PermanentFailureDetails = (EventOpcode)58;
            public const EventOpcode RecoverableErrors = (EventOpcode)59;

            // Metadata
            public const EventOpcode NewTopic = (EventOpcode)60;
            public const EventOpcode PartitionErrorChange = (EventOpcode)61;
            public const EventOpcode PartitionIsrChange = (EventOpcode)62;
            public const EventOpcode PartitionLeaderChange = (EventOpcode)63;
            public const EventOpcode PartitionReplicasChange = (EventOpcode)64;
            public const EventOpcode PartitionTransportError = (EventOpcode)65;
        }

        public class Tasks
        {
            public const EventTask Fetcher = (EventTask)1;
            public const EventTask Connection = (EventTask)2;
            public const EventTask Correlation = (EventTask)3;
            public const EventTask RecoveryMonitor = (EventTask)4;
            public const EventTask Protocol = (EventTask)5;
            public const EventTask Producer = (EventTask)6;
            public const EventTask Cluster = (EventTask)7;
            public const EventTask Metadata = (EventTask)8;
            public const EventTask Consumer = (EventTask)9;
        }

        public class Keywords
        {
            public const EventKeywords DataDump = (EventKeywords)0x1;
        }

        #region Fetcher
        [Event(1, Task = Tasks.Fetcher, Opcode = Opcodes.Create)]
        public void FetcherStart(int fetcherId, string topic)
        {
            if(IsEnabled())
                Log.WriteEvent(1, fetcherId, topic);
        }

        [Event(2, Task = Tasks.Fetcher, Opcode = Opcodes.CancelSentWakeup)]
        public void FetcherCancelSentWakeup(int id)
        {
            if (IsEnabled())
                Log.WriteEvent(2, id);
        }

        [Event(3, Task = Tasks.Fetcher, Opcode = Opcodes.PartitionSubscribed)]
        public void FetcherPartitionSubscribed(int id, int partitionId)
        {
            if (IsEnabled())
                Log.WriteEvent(3, id, partitionId);
        }

        [Event(4, Task = Tasks.Fetcher, Opcode = Opcodes.FetchResponse)]
        public void FetcherFetchResponse(int id)
        {
            if (IsEnabled())
                Log.WriteEvent(4, id);
        }

        [Event(5, Task = Tasks.Fetcher, Opcode = Opcodes.Message)]
        public void FetcherMessage(int id, int keyLen, int valueLen, long offset, int partition)
        {
            if (IsEnabled())
                Log.WriteEvent(5, id, keyLen, valueLen, offset, partition);
        }

        [Event(6, Task = Tasks.Fetcher, Opcode = Opcodes.Sleep)]
        public void FetcherSleep(int id)
        {
            if (IsEnabled())
                Log.WriteEvent(6, id);
        }

        [Event(7, Task = Tasks.Fetcher, Opcode = Opcodes.Wakeup)]
        public void FetcherWakeup(int id)
        {
            if (IsEnabled())
                Log.WriteEvent(7, id);
        }

        [Event(8, Task = Tasks.Fetcher, Opcode = Opcodes.FetchRequest)]
        public void FetcherFetchRequest(int id, int topicCount, int partsCount, string host, int port, int brokerId)
        {
            if (IsEnabled())
                Log.WriteEvent(8, id, topicCount, partsCount, host, port, brokerId);
        }

        #endregion

        #region Connection

        [Event(101, Task = Tasks.Connection, Opcode = Opcodes.Connecting)]
        public void ConnectionConnecting(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(101, host, port);
        }

        [Event(102, Task = Tasks.Connection, Opcode = Opcodes.Connected)]
        public void ConnectionConnected(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(102, host, port);
        }

        [Event(103, Task = Tasks.Connection, Opcode = Opcodes.Errored)]
        public void ConnectionErrored(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(103, host, port);
        }

        [Event(104, Task = Tasks.Connection, Opcode = Opcodes.Disconnected)]
        public void ConnectionDisconnected(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(104, host, port);
        }

        //[Event]
        //public void ConnectionWaitingForLock(string host, int port)
        //{
        //    Log.WriteEvent(105, host, port);
        //}

        //[Event]
        //public void ConnectionGotLock(string host, int port)
        //{
        //    Log.WriteEvent(106, host, port);
        //}

        //[Event]
        //public void ConnectionLockRelease(string host, int port)
        //{
        //    Log.WriteEvent(108, host, port);
        //}

        [Event(107, Task = Tasks.Connection, Opcode = Opcodes.ReplaceClosedClient)]
        public void ConnectionReplaceClosedClient(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(107, host, port);
        }

        [Event(108, Task = Tasks.Connection, Opcode = Opcodes.MarkSocketAsFailedCorrelationLoopCancelling)]
        public void Connection_MarkSocketAsFailed_CorrelationLoopCancelling(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(108, host, port);
        }

        [Event(109, Task = Tasks.Connection, Opcode = Opcodes.MarkSocketAsFailedTcpClosing)]
        public void Connection_MarkSocketAsFailed_TcpClosing(string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(109, host, port);
        }
        #endregion

        #region Correlation
        [Event(201, Task = Tasks.Correlation, Opcode = Opcodes.Create)]
        public void CorrelationCreate()
        {
            if (IsEnabled())
                Log.WriteEvent(201);
        }

        [Event(202, Task = Tasks.Correlation, Opcode = EventOpcode.Start)]
        public void CorrelationStart()
        {
            if (IsEnabled())
                Log.WriteEvent(202);
        }

        [Event(203, Task = Tasks.Correlation, Opcode = Opcodes.ReadingMessageSize)]
        public void CorrelationReadingMessageSize()
        {
            if (IsEnabled())
                Log.WriteEvent(203);
        }

        [Event(204, Task = Tasks.Correlation, Opcode = Opcodes.ServerClosedConnection)]
        public void CorrelationServerClosedConnection()
        {
            if (IsEnabled())
                Log.WriteEvent(204);
        }

        [Event(205, Task = Tasks.Correlation, Opcode = Opcodes.ReadMessageSize)]
        public void CorrelationReadMessageSize(int size)
        {
            if (IsEnabled())
                Log.WriteEvent(205, size);
        }

        [Event(206, Task = Tasks.Correlation, Opcode = Opcodes.ReadingBodyChunk)]
        public void Correlation_ReadingBodyChunk(int left)
        {
            if (IsEnabled())
                Log.WriteEvent(206, left);
        }

        [Event(207, Task = Tasks.Correlation, Opcode = Opcodes.ReadBodyChunk)]
        public void CorrelationReadBodyChunk(int read, int left)
        {
            if (IsEnabled())
                Log.WriteEvent(207, read, left);
        }

        [Event(208, Task = Tasks.Correlation, Opcode = Opcodes.ReadBody)]
        public void CorrelationReadBody(int size)
        {
            if (IsEnabled())
                Log.WriteEvent(208, size);
        }

        [Event(209, Task = Tasks.Correlation, Opcode = Opcodes.ReceivedCorrelationId)]
        public void CorrelationReceivedCorrelationId(int correlationId)
        {
            if (IsEnabled())
                Log.WriteEvent(209, correlationId);
        }

        [Event(210, Task = Tasks.Correlation, Opcode = Opcodes.ExecutingHandler)]
        public void CorrelationExecutingHandler()
        {
            if (IsEnabled())
                Log.WriteEvent(210);
        }

        [Event(211, Task = Tasks.Correlation, Opcode = Opcodes.ExecutedHandler)]
        public void CorrelationExecutedHandler()
        {
            if (IsEnabled())
                Log.WriteEvent(211);
        }

        [Event(212, Task = Tasks.Correlation, Opcode = Opcodes.Error)]
        public void CorrelationError(string message)
        {
            if (IsEnabled())
                Log.WriteEvent(212, message);
        }

        [Event(213, Task = Tasks.Correlation, Opcode = Opcodes.Complete)]
        public void CorrelationComplete()
        {
            if (IsEnabled())
                Log.WriteEvent(213);
        }

        [Event(214, Task = Tasks.Correlation, Opcode = Opcodes.WritingMessage)]
        public void CorrelationWritingMessage(int correlationId, int length)
        {
            if (IsEnabled())
                Log.WriteEvent(214, correlationId, length);
        }
        #endregion

        #region Recovery Monitor
        [Event(301, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.Create)]
        public void RecoveryMonitor_Create(int monitorId)
        {
            if (IsEnabled())
                Log.WriteEvent(301, monitorId);
        }

        [Event(302, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PartitionRecovered)]
        public void RecoveryMonitor_PartitionRecovered(int monitorId, string topic, int partitionId)
        {
            if (IsEnabled())
                Log.WriteEvent(302, monitorId, topic, partitionId);
        }

        [Event(303, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PartitionFailed)]
        public void RecoveryMonitor_PartitionFailed(int monitorId, string topic, int partitionId, int errorCode)
        {
            // TODO: if use ErrorCode enum, manifest is failed to generate for some reason
            if (IsEnabled())
                Log.WriteEvent(303, monitorId, topic, partitionId, errorCode);
        }

        [Event(304, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PartitionFailedAgain)]
        public void RecoveryMonitor_PartitionFailedAgain(int monitorId, string topic, int partitionId, int errorCode)
        {
            if (IsEnabled())
                Log.WriteEvent(304, monitorId, topic, partitionId, errorCode);
        }

        [Event(305, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.RecoveryLoopStarted)]
        public void RecoveryMonitor_RecoveryLoopStarted(int monitorId, string host, int port, int nodeId)
        {
            if (IsEnabled())
                Log.WriteEvent(305, monitorId, host, port, nodeId);
        }

        [Event(306, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.SendingPing)]
        public void RecoveryMonitor_SendingPing(int monitorId, string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(306, monitorId, host, port);
        }

        [Event(307, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PingResponse)]
        public void RecoveryMonitor_PingResponse(int monitorId, string host, int port)
        {
            if (IsEnabled())
                Log.WriteEvent(307, monitorId, host, port);
        }

        [Event(308, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PingFailed)]
        public void RecoveryMonitor_PingFailed(int monitorId, string host, int port, string message)
        {
            if (IsEnabled())
                Log.WriteEvent(308, monitorId, host, port, message);
        }

        [Event(309, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.PossiblyHealedPartitions)]
        public void RecoveryMonitor_PossiblyHealedPartitions(int monitorId, int count)
        {
            if (IsEnabled())
                Log.WriteEvent(309, monitorId, count);
        }

        [Event(310, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.NoHealedPartitions)]
        public void RecoveryMonitor_NoHealedPartitions(int monitorId)
        {
            if (IsEnabled())
                Log.WriteEvent(310, monitorId);
        }

        [Event(311, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.CheckingBrokerAccessibility)]
        public void RecoveryMonitor_CheckingBrokerAccessibility(int monitorId, string host, int port, int nodeId)
        {
            if (IsEnabled())
                Log.WriteEvent(311, monitorId, host, port, nodeId);
        }

        [Event(312, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.BrokerIsAccessible)]
        public void RecoveryMonitor_BrokerIsAccessible(int monitorId, string host, int port, int nodeId)
        {
            if (IsEnabled())
                Log.WriteEvent(312, monitorId, host, port, nodeId);
        }

        [Event(313, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.HealedPartitions)]
        public void RecoveryMonitor_HealedPartitions(int monitorId, string host, int port, int nodeId, string topicName, string partitions)
        {
            if (IsEnabled())
                Log.WriteEvent(313, monitorId, host, port, nodeId, topicName, partitions);
        }

        [Event(314, Task = Tasks.RecoveryMonitor, Opcode = Opcodes.RecoveryLoopStop)]
        public void RecoveryMonitor_RecoveryLoopStop(int monitorId)
        {
            if (IsEnabled())
                Log.WriteEvent(314, monitorId);
        }
        #endregion

        #region Protocol
        [Event(400, Task = Tasks.Protocol, Opcode = Opcodes.MetadataRequest, Keywords = Keywords.DataDump)]
        public void ProtocolMetadataRequest(string request)
        {
            if(IsEnabled())
                Log.WriteEvent(400, request);
        }

        [Event(401, Task = Tasks.Protocol, Opcode = Opcodes.MetadataResponse, Keywords = Keywords.DataDump)]
        public void ProtocolMetadataResponse(string response, string host, int port, int nodeId)
        {
            if (IsEnabled())
                Log.WriteEvent(401, response, host, port, nodeId);
        }

        [Event(402, Task = Tasks.Protocol, Opcode = Opcodes.ProduceRequest, Keywords = Keywords.DataDump)]
        public void ProtocolProduceRequest(string request, int nodeId)
        {
            if(IsEnabled())
                Log.WriteEvent(402, request, nodeId);
        }

        [Event(403, Task = Tasks.Protocol, Opcode = Opcodes.ProduceResponse, Keywords = Keywords.DataDump)]
        public void ProtocolProduceResponse(string response, int nodeId)
        {
            if(IsEnabled())
                Log.WriteEvent(403, response, nodeId);
        }

        [Event(404, Task = Tasks.Protocol, Opcode = Opcodes.OffsetRequest, Keywords = Keywords.DataDump)]
        public void ProtocolOffsetRequest(string request)
        {
            if(IsEnabled())
                Log.WriteEvent(404, request);
        }

        [Event(405, Task = Tasks.Protocol, Opcode = Opcodes.OffsetResponse, Keywords = Keywords.DataDump)]
        public void ProtocolOffsetResponse(string response)
        {
            if(IsEnabled())
                Log.WriteEvent(405, response);
        }

        [Event(406, Task = Tasks.Protocol, Opcode = Opcodes.FetchRequest, Keywords = Keywords.DataDump)]
        public void ProtocolFetchRequest(string request)
        {
            if(IsEnabled())
                Log.WriteEvent(406, request);
        }

        [Event(407, Task = Tasks.Protocol, Opcode = Opcodes.FetchResponse, Keywords = Keywords.DataDump)]
        public void ProtocolFetchResponse(string response)
        {
            if(IsEnabled())
                Log.WriteEvent(407, response);
        }
        #endregion

        #region Producer 
        [Event(500, Task = Tasks.Producer, Opcode = Opcodes.PermanentFailure)]
        public void ProducerPermanentFailure(int producerId, int partitionCount)
        {
            if(IsEnabled())
                Log.WriteEvent(500, producerId, partitionCount);
        }

        [Event(501, Task = Tasks.Producer, Opcode = Opcodes.PermanentFailureDetails, Keywords = Keywords.DataDump)]
        public void ProducerPermanentFailureDetails(int producerId, string error)
        {
            if(IsEnabled())
                Log.WriteEvent(501, producerId, error);
        }

        [Event(502, Task = Tasks.Producer, Opcode = Opcodes.RecoverableErrors, Level = EventLevel.Error)]
        public void ProducerRecoverableErrors(int producerId, int partitionCount)
        {
            if(IsEnabled())
                Log.WriteEvent(502, producerId, partitionCount);
        }

        [Event(503, Task = Tasks.Producer, Opcode = Opcodes.Starting)]
        public void ProducerStarting(string topic, int producerId)
        {
            if (IsEnabled())
                Log.WriteEvent(503, topic, producerId);
        }

        [Event(504, Task = Tasks.Producer, Opcode = Opcodes.Started)]
        public void ProducerStarted(string topic, int producerId)
        {
            if (IsEnabled())
                Log.WriteEvent(504, topic, producerId);
        }

        [Event(505, Task = Tasks.Producer, Opcode = Opcodes.Error)]
        public void ProducerError(string error, int producerId)
        {
            if (IsEnabled())
                Log.WriteEvent(505, error, producerId);
        }

        [Event(506, Task = Tasks.Producer, Opcode = Opcodes.Stopping)]
        public void ProducerStopping(string topic, int producerId)
        {
            if (IsEnabled())
                Log.WriteEvent(506, topic, producerId);
        }

        [Event(507, Task = Tasks.Producer, Opcode = Opcodes.Stopped)]
        public void ProducerStoped(string topic, int producerId)
        {
            if (IsEnabled())
                Log.WriteEvent(507, topic, producerId);
        }
        #endregion

        #region Cluster
        [Event(600, Task = Tasks.Cluster, Opcode = Opcodes.Starting)]
        public void ClusterStarting(int clusterId)
        {
            if(IsEnabled())
                Log.WriteEvent(600, clusterId);
        }

        [Event(601, Task = Tasks.Cluster, Opcode = Opcodes.Started)]
        public void ClusterStarted(int clusterId)
        {
            if(IsEnabled())
                Log.WriteEvent(601, clusterId);
        }

        [Event(602, Task = Tasks.Cluster, Opcode = Opcodes.Stopping)]
        public void ClusterStopping(int clusterId)
        {
            if(IsEnabled())
                Log.WriteEvent(602, clusterId);
        }

        [Event(603, Task = Tasks.Cluster, Opcode = Opcodes.Error)]
        public void ClusterError(int clusterId, string error)
        {
            if(IsEnabled())
                Log.WriteEvent(603, clusterId, error);
        }

        [Event(604, Task = Tasks.Cluster, Opcode = Opcodes.Stopped)]
        public void ClusterStopped(int clusterId)
        {
            if(IsEnabled())
                Log.WriteEvent(604, clusterId);
        }
        #endregion

        #region Metadata
        [Event(700, Task = Tasks.Metadata, Opcode = Opcodes.NewTopic)]
        public void MetadataNewTopic(int clusterId, string topic)
        {
            if(IsEnabled())
                Log.WriteEvent(700, clusterId, topic);
        }

        [Event(702, Task = Tasks.Metadata, Opcode = Opcodes.PartitionErrorChange)]
        public void MetadataPartitionErrorChange(int clusterId, string topic, int partId, int oldCode, int newCode)
        {
            if(IsEnabled())
                Log.WriteEvent(702, clusterId, topic, partId,oldCode, newCode);
        }

        [Event(703, Task = Tasks.Metadata, Opcode = Opcodes.PartitionIsrChange)]
        public void MetadataPartitionIsrChange(int clusterId, string topic, int partId, string oldIsrs, string newIsrs)
        {
            if(IsEnabled())
                Log.WriteEvent(703, clusterId, topic, partId, oldIsrs, newIsrs);
        }

        [Event(704, Task = Tasks.Metadata, Opcode = Opcodes.PartitionLeaderChange)]
        public void MetadataPartitionLeaderChange(int clusterId, string topic, int partId, int oldLeader, int newLeader)
        {
            if(IsEnabled())
                Log.WriteEvent(704, clusterId, topic, partId, oldLeader, newLeader);
        }

        [Event(705, Task = Tasks.Metadata, Opcode = Opcodes.PartitionReplicasChange)]
        public void MetadataPartitionReplicasChange(int clusterId, string topic, int partId, string oldReplicas, string newReplicas)
        {
            if(IsEnabled())
                Log.WriteEvent(705, clusterId, topic, partId, oldReplicas, newReplicas);
        }

        [Event(706, Task = Tasks.Metadata, Opcode = Opcodes.PartitionTransportError)]
        public void MetadataTransportError(string topicName, int clusterId, int part, int leader)
        {
            Log.WriteEvent(706, topicName, clusterId, part, leader);
        }

        #endregion

        #region Consumer
        [Event(800, Task = Tasks.Consumer, Opcode = Opcodes.Started)]
        public void ConsumerStarted(int consumerId, string topic)
        {
            if (IsEnabled())
                Log.WriteEvent(800, consumerId, topic);
        }

        [Event(801, Task = Tasks.Consumer, Opcode = Opcodes.Stopped)]
        public void ConsumerStopped(int consumerId, string topic)
        {
            if (IsEnabled())
                Log.WriteEvent(801, consumerId, topic);
        }
        #endregion

        #region Custom Marker
        [Event(900)]
        public void Marker2(string marker) 
        {
            Log.WriteEvent(900, marker);
        }

        public static void Marker(string marker) 
        {
            Log.Marker2(marker);
        }
        #endregion
    }
}
