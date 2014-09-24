using System;
using System.Collections.Generic;

namespace kafka4net.Utils
{
    static class EnumerableEx
    {
        public static void ForEach<T>(this IEnumerable<T> items, Action<T> action) 
        {
            foreach (T item in items)
                action(item);
        }
    }
}
