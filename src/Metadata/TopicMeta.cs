using System.Collections.Generic;

namespace kafka4net.Metadata
{
    public class TopicMeta
    {
        public ErrorCode TopicErrorCode;
        public string TopicName;
        public PartitionMeta[] Partitions;

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
