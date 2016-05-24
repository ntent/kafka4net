using kafka4net.Metadata;

namespace kafka4net
{
    /// <summary>
    /// Assign message to certain partition.
    /// WARNING: this interface is called in caller (client) thread. Thus, if caller is multithreaded,
    /// IMessagePartitioner implementation must be thread-safe.
    /// 
    /// It is possible to move call to IMessagePartitioner at later stage in the driver, where it would be thread-safe, but it would mean that 
    /// message will get its partition at unpredictable time from client point of view, which could be confusing.
    /// </summary>
    public interface IMessagePartitioner
    {
        PartitionMeta GetMessagePartition(Message message, PartitionMeta[] allPartitions);
    }
}
