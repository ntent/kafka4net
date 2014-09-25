using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.Utils
{
    static class TaskEx
    {
        /// <summary>
        /// Not very performant, but good enough to use in shutdown routine.
        /// http://blogs.msdn.com/b/pfxteam/archive/2011/11/10/10235834.aspx
        /// </summary>
        public async static Task<bool> TimeoutAfter(this Task task, TimeSpan timeout)
        {
            var delay = Task.Delay(timeout);
            var res = await Task.WhenAny(task, delay);
            return res == task;
        }
    }
}
