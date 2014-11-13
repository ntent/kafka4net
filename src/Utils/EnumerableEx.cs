using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace kafka4net.Utils
{
    static class EnumerableEx
    {
        [DebuggerStepThrough]
        public static void ForEach<T>(this IEnumerable<T> items, Action<T> action) 
        {
            foreach (T item in items)
                action(item);
        }
    }
}
