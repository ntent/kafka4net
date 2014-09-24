using System.Collections.Generic;

namespace kafka4net.Metadata
{
    class BrokerMeta
    {
        public int NodeId;
        public string Host;
        public int Port;

        // Not serialized, just a link to connection associated with this broker
        internal Connection Conn;

        public override string ToString()
        {
            return string.Format("{0}:{1} Id:{2}", Host, Port, NodeId);
        }

        #region comparer
        public static readonly IEqualityComparer<BrokerMeta> NodeIdComparer = new ComparerImpl();
        class ComparerImpl : IEqualityComparer<BrokerMeta>
        {
            public bool Equals(BrokerMeta x, BrokerMeta y)
            {
                return x.NodeId == y.NodeId;
            }

            public int GetHashCode(BrokerMeta obj)
            {
                return obj.NodeId;
            }
        }
        #endregion
    }
}
