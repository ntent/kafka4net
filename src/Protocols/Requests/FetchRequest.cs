using System.Linq;

namespace kafka4net.Protocols.Requests
{
    class FetchRequest
    {
        /// <summary>
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if 
        /// insufficient data is available at the time the request is issued.
        /// </summary>
        public int MaxWaitTime;

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response. 
        /// If the client sets this to 0 the server will always respond immediately, however if there is 
        /// no new data since their last request they will just get back empty message sets. 
        /// If this is set to 1, the server will respond as soon as at least one partition has 
        /// at least 1 byte of data or the specified timeout occurs. By setting higher values in combination 
        /// with the timeout the consumer can tune for throughput and trade a little additional latency 
        /// for reading only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting 
        /// MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k 
        /// of data before responding).
        /// </summary>
        public int MinBytes;

        public TopicData[] Topics;

        public class TopicData
        {
            public string Topic;
            public PartitionData[] Partitions;

            public override string ToString()
            {
                return string.Format("Topic: {0} Parts: [{1}]", Topic, string.Join(", ", Partitions.AsEnumerable()));
            }
        }

        public class PartitionData
        {
            public int Partition;
            public long FetchOffset;
            public int MaxBytes;

            public override string ToString()
            {
                return string.Format("Partition: {0} Offset: {1} MaxBytes: {2}", Partition, FetchOffset, MaxBytes);
            }
        }

        public override string ToString()
        {
            return string.Format("MaxTime: {0} MinBytes: {1} [{2}]", MaxWaitTime, MinBytes, string.Join(",", Topics.AsEnumerable()));
        }
    }
}
