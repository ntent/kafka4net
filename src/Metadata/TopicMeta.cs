using System.Collections.Generic;
using System.Linq;

namespace kafka4net.Metadata
{
    public class TopicMeta
    {
        public ErrorCode TopicErrorCode;
        public string TopicName;
        public PartitionMeta[] Partitions;

        public override string ToString()
        {
            return string.Format("Topic '{0}' Partitions [{1}]", TopicName, string.Join(",", Partitions.AsEnumerable()));
        }

        #region name comparer
        static readonly NameCompareImpl NameComparerInstance = new NameCompareImpl();
        public static IEqualityComparer<TopicMeta> NameComparer { get { return NameComparerInstance; } }
        class NameCompareImpl : IEqualityComparer<TopicMeta>
        {
            public bool Equals(TopicMeta x, TopicMeta y)
            {
                return x.TopicName == y.TopicName;
            }

            public int GetHashCode(TopicMeta obj)
            {
                return obj.TopicName.GetHashCode();
            }
        }
        #endregion
    }
}
