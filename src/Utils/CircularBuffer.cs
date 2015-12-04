using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace kafka4net.Utils
{
    internal class CircularBuffer<T> : ICollection<T>, ICollection 
        where T: class
    {
        private static readonly ILogger _log = Logger.GetLogger();
        private int capacity;
        private int size;
        private int head;
        private int tail;
        private T[] buffer;

        [NonSerialized]
        private object syncRoot;

        public CircularBuffer(int capacity)
            : this(capacity, false)
        {
        }

        public CircularBuffer(int capacity, bool allowOverflow)
        {
            if (capacity < 0)
                throw new ArgumentException("The buffer capacity must be greater than or equal to zero.", "capacity");

            this.capacity = capacity;
            size = 0;
            head = 0;
            tail = 0;
            buffer = new T[capacity];
            AllowOverflow = allowOverflow;
        }

        public bool AllowOverflow
        {
            get;
            set;
        }

        public int Capacity
        {
            get { return capacity; }
            set
            {
                if (value == capacity)
                    return;

                if (value < size)
                    throw new ArgumentOutOfRangeException("value", "The new capacity must be greater than or equal to the buffer size.");

                AssertDense();

                var dst = new T[value];

                if (size > 0)
                    CopyTo(dst);
                buffer = dst;

                head = 0;
                tail = size;

                capacity = value;

                AssertDense();
            }
        }

        private void AssertDense()
        {
            // assert we have no nulls in our current buffer.
            var notDense = Peek(size).Any(m => m == null);
            if (notDense)
                _log.Error("Null messages in buffer! {0}", this);
            //else
                //_log.Debug("Buffer is dense {0}", this);
        }

        public int Size
        {
            get { return size; }
        }

        public bool Contains(T item)
        {
            int bufferIndex = head;
            var comparer = EqualityComparer<T>.Default;
            for (int i = 0; i < size; i++, bufferIndex++)
            {
                if (bufferIndex == capacity)
                    bufferIndex = 0;

                if (item == null && buffer[bufferIndex] == null)
                    return true;
                else if ((buffer[bufferIndex] != null) &&
                    comparer.Equals(buffer[bufferIndex], item))
                    return true;
            }

            return false;
        }

        public void Clear()
        {
            size = 0;
            head = 0;
            tail = 0;
            Array.Clear(buffer, 0, buffer.Length);
        }

        public T Peek()
        {
            if (size == 0)
                throw new InvalidOperationException("The buffer is empty.");
            return buffer[head];
        }

        public T[] Peek(int count)
        {
            var dst = new T[count];
            Peek(dst);
            return dst;
        }

        public int Peek(T[] dst)
        {
            return Peek(dst, 0, dst.Length);
        }

        public int Peek(T[] dst, int offset, int count)
        {
            int realCount = Math.Min(count, size);
            int dstIndex = offset;
            int readHead = head;
            for (int i = 0; i < realCount; i++, readHead++, dstIndex++)
            {
                if (readHead == capacity)
                    readHead = 0;
                dst[dstIndex] = buffer[readHead];
            }
            return realCount;
        }

        public IEnumerable<T> PeekEnum(int count) 
        {
            if(count > size)
                throw new ArgumentOutOfRangeException("count", string.Format("Count {0} > size {1}", count, size));
            var readHead = head;
            for (int i = 0; i < count; i++, readHead++)
            {
                if (readHead == capacity)
                    readHead = 0;
                yield return buffer[readHead];
            }
        }

        public T PeekSingle(int offset)
        {
            return buffer[(head + offset) % capacity];
        }

        public int Put(T[] src)
        {
            return Put(src, 0, src.Length);
        }

        public int Put(T[] src, int offset, int count)
        {
            AssertDense();
            if (!AllowOverflow && count > capacity - size)
                throw new InvalidOperationException("The buffer does not have sufficient capacity to put new items.");

            int srcIndex = offset;
            for (int i = 0; i < count; i++, srcIndex++)
            {
                buffer[tail] = src[srcIndex];

                // advance the tail
                if (++tail == capacity)
                    tail = 0;
            }
            size = Math.Min(size + count, capacity);
            AssertDense();
            return count;
        }

        public void Put(T item)
        {
            if (!AllowOverflow && size == capacity)
                throw new InvalidOperationException("The buffer does not have sufficient capacity to put new items.");

            buffer[tail] = item;
            if (++tail == capacity)
                tail = 0;
            size++;
        }

        public T[] Get(int count)
        {
            var dst = new T[count];
            Get(dst);
            return dst;
        }

        public int Get(T[] dst)
        {
            return Get(dst, 0, dst.Length);
        }

        public int Get(T[] dst, int offset, int count)
        {
            AssertDense();
            int realCount = Math.Min(count, size);
            int dstIndex = offset;
            for (int i = 0; i < realCount; i++, dstIndex++)
            {
                dst[dstIndex] = buffer[head];
                buffer[head] = null;

                // advance the head
                if (++head == capacity)
                    head = 0;
            }
            size -= realCount;
            AssertDense();
            return realCount;
        }

        public T Get()
        {
            if (size == 0)
                throw new InvalidOperationException("The buffer is empty.");

            var item = buffer[head];
            buffer[head] = null;
            if (++head == capacity)
                head = 0;
            size--;
            return item;
        }

        public IEnumerable<T> GetEnum(int count)
        {
            if (count > size)
                throw new ArgumentOutOfRangeException("count", string.Format("Count is {0} but actual size {1}", count, size));

            for (int i = 0; i < count; i++)
            {
                yield return buffer[head];
                buffer[head] = null;

                if (++head == capacity)
                    head = 0;
                size--;
            }
        }

        public void CopyTo(T[] array)
        {
            CopyTo(array, 0);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            CopyTo(0, array, arrayIndex, size);
        }

        public void CopyTo(int index, T[] array, int arrayIndex, int count)
        {
            if (count > size)
                throw new ArgumentOutOfRangeException("count", "The read count cannot be greater than the buffer size.");

            int bufferIndex = head;
            for (int i = 0; i < count; i++, arrayIndex++)
            {
                array[arrayIndex] = buffer[bufferIndex];
                if (++bufferIndex == capacity)
                    bufferIndex = 0;
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            int bufferIndex = head;
            for (int i = 0; i < size; i++, bufferIndex++)
            {
                if (bufferIndex == capacity)
                    bufferIndex = 0;

                yield return buffer[bufferIndex];
            }
        }

        public T[] GetBuffer()
        {
            return buffer;
        }

        public T[] ToArray()
        {
            var dst = new T[size];
            CopyTo(dst);
            return dst;
        }

        #region ICollection<T> Members

        int ICollection<T>.Count
        {
            get { return Size; }
        }

        bool ICollection<T>.IsReadOnly
        {
            get { return false; }
        }

        void ICollection<T>.Add(T item)
        {
            Put(item);
        }

        bool ICollection<T>.Remove(T item)
        {
            if (size == 0)
                return false;

            Get();
            return true;
        }

        #endregion

        #region IEnumerable<T> Members

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region ICollection Members

        int ICollection.Count
        {
            get { return Size; }
        }

        bool ICollection.IsSynchronized
        {
            get { return false; }
        }

        object ICollection.SyncRoot
        {
            get
            {
                if (syncRoot == null)
                    Interlocked.CompareExchange(ref syncRoot, new object(), null);
                return syncRoot;
            }
        }

        void ICollection.CopyTo(Array array, int arrayIndex)
        {
            CopyTo((T[])array, arrayIndex);
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public override string ToString()
        {
            return string.Format("{0}/{1} H:{2},T:{3}", size, capacity, head, tail);
        }
    }
}
