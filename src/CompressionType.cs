namespace kafka4net
{
    public enum CompressionType
    {
        None = 0,
        Gzip = 1,
        Snappy = 2,

        // Beaware of https://issues.apache.org/jira/browse/KAFKA-3160
        Lz4 = 3,
    }
}
