using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using kafka4net.Metadata;

namespace kafka4net
{
    public interface IMessagePartitioner
    {
        PartitionMeta GetMessagePartition(Message message, PartitionMeta[] allPartitions);
    }
}
