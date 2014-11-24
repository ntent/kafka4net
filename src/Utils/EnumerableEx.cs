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

        /// <summary>
        /// Wraps this object instance into an IEnumerable&lt;T&gt;
        /// consisting of a single item.
        /// </summary>
        /// <typeparam name="T"> Type of the object. </typeparam>
        /// <param name="item"> The instance that will be wrapped. </param>
        /// <returns> An IEnumerable&lt;T&gt; consisting of a single item. </returns>
        public static IEnumerable<T> YieldEnumerable<T>(this T item)
        {
            yield return item;
        }
    }
}
