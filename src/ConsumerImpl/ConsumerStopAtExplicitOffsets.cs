using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.ConsumerImpl
{
    public class ConsumerStopAtExplicitOffsets
    {
        private readonly TopicPartitionOffsets _stopOffsets;

        public ConsumerStopAtExplicitOffsets(TopicPartitionOffsets stopOffsets)
        {
            _stopOffsets = stopOffsets;
        }

        public bool IsPartitionComplete(ReceivedMessage message)
        {
            // "NextOffset" is the "Next" offset, so subtract one to get the current offset.
            var stopOffset = _stopOffsets.NextOffset(message.Partition) - 1;
            
            return message.Offset == stopOffset;
        } 
    }
}
