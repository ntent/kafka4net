using System;

namespace kafka4net.Utils
{
    public static class DateTimeExtensions
    {

        /// <summary>
        /// Unix epoch time from a datetime
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static int DateTimeToEpochSeconds(this DateTimeOffset date)
        {
            var t = (date - new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero));
            return (int)t.TotalSeconds;
        }

        /// <summary>
        /// Datetime from an int representing a unix epoch time
        /// </summary>
        /// <param name="secondsSinceEpoch"></param>
        /// <returns></returns>
        public static DateTimeOffset EpochSecondsToDateTime(this int secondsSinceEpoch)
        {
            var date = new DateTimeOffset(1970, 1, 1,0,0,0,TimeSpan.Zero);
            return date.AddSeconds(secondsSinceEpoch);
        }

        /// <summary>
        /// Unix epoch time from a datetime
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static long DateTimeToEpochMilliseconds(this DateTimeOffset date)
        {
            var t = (date - new DateTimeOffset(1970, 1, 1,0,0,0,TimeSpan.Zero));
            return (long)t.TotalMilliseconds;
        }

        /// <summary>
        /// Datetime from an int representing a unix epoch time
        /// </summary>
        /// <param name="msSinceEpoch"></param>
        /// <returns></returns>
        public static DateTimeOffset EpochMillisecondsToDateTime(this long msSinceEpoch)
        {
            var date = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
            return date.AddMilliseconds(msSinceEpoch);
        }

    }
}
