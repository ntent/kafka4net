namespace kafka4net.Protocols.Requests
{
    class TopicRequest
    {
        public string[] Topics;

        public override string ToString()
        {
            return string.Format("[{0}]", string.Join(",", Topics));
        }
    }
}
