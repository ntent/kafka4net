using System.Collections.Generic;
using System.Linq;

namespace kafka4net.Protocols.Requests
{
    class PartitionData
    {
        public int Partition;
        // is calculated at serizlization time
        //public int MessageSetSize;
        public IEnumerable<MessageData> Messages;

        /// <summary>Is not serialized. Is carried through to send error/success notifications
        /// if herror happen</summary>
        public Producer Pub;

        /// <summary>
        /// Not serialized.
        /// Copy of origianl, application provided messages. Is needed when error happen
        /// and driver notifys app that those messages have failed.
        /// </summary>
        public Message[] OriginalMessages;

        public override string ToString()
        {
            return string.Format("Part: {0} Messages: {1}", Partition, Messages.Count());
        }
    }
}
