namespace kafka4net
{
    public enum ErrorCode : short
    {
        ///<summary>No error--it worked!</summary>
        NoError = 0,
        ///<summary>An unexpected server error</summary>
        Unknown = -1,
        ///<summary>The requested offset is outside the range of offsets maintained by the server for the given 
        /// topic/partition.</summary>
        OffsetOutOfRange = 1,
        ///<summary>This indicates that a message contents does not match its CRC</summary>
        InvalidMessage = 2,
        ///<summary>This request is for a topic or partition that does not exist on this broker.</summary>
        UnknownTopicOrPartition = 3,
        ///<summary>The message has a negative size</summary>
        InvalidMessageSize = 4,
        ///<summary>This error is thrown if we are in the middle of a leadership election and there is currently 
        /// no leader for this partition and hence it is unavailable for writes.</summary>
        LeaderNotAvailable = 5,
        ///<summary>This error is thrown if the client attempts to send messages to a replica that is not 
        /// the leader for some partition. It indicates that the clients metadata is out of date.</summary>
        NotLeaderForPartition = 6,
        ///<summary>This error is thrown if the request exceeds the user-specified time limit in the request</summary>
        RequestTimedOut = 7,
        ///<summary>This is not a client facing error and is used only internally by intra-cluster 
        /// broker communication</summary>
        BrokerNotAvailable = 8,
        ///<summary>Unused</summary>
        Unused = 9,
        ///<summary>The server has a configurable maximum message size to avoid unbounded memory allocation. 
        /// This error is thrown if the client attempt to produce a message larger than this maximum.</summary>
        MessageSizeTooLarge = 10,
        ///<summary>Internal error code for broker-to-broker communication</summary>
        StaleControllerEpochCode = 11,
        ///<summary>If you specify a string larger than configured maximum for offset metadata</summary>
        OffsetMetadataTooLargeCode = 12,
        /// <summary> The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition) </summary>
        OffsetsLoadInProgressCode = 14,
        /// <summary> The broker returns this error code for consumer metadata requests or offset commit requests 
        /// if the offsets topic has not yet been created </summary>
        ConsumerCoordinatorNotAvailableCode = 15,
        /// <summary>The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for</summary>
        NotCoordinatorForConsumerCode = 16,

        /// <summary>This state is not Kafka error code but driver-set state, meaning that there are messages in the waiting queue for this partition and sending routine needs to drain
        /// waiting queue first, to prevent possible message reordering, when fresh messages will go out before waiting ones.</summary>
        Draining = 1000
    }
}
