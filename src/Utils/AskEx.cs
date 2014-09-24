using System;
using System.Reactive.Concurrency;
using System.Threading.Tasks;

namespace kafka4net.Utils
{
    static class AskEx
    {
        public static Task<T> Ask<T>(this IScheduler scheduler, Func<T> action)
        {
            var src = new TaskCompletionSource<T>();
            scheduler.Schedule(() =>
            {
                try
                {
                    var res = action();
                    src.SetResult(res);
                }
                catch (Exception e)
                {
                    src.SetException(e);
                }
            });

            return src.Task;
        }
    }
}
