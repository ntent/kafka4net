using System.Threading.Tasks;

namespace kafka4net
{
    public sealed class ConsumerState
    {
        public Task Connected { get { return _connected.Task; } }
        public Task Closed { get { return _closed.Task; } }

        internal readonly TaskCompletionSource<bool> _connected = new TaskCompletionSource<bool>();
        internal readonly TaskCompletionSource<bool> _closed = new TaskCompletionSource<bool>();
    }
}
