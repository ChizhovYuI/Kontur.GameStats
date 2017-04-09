using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kontur.GameStats.Server.Utils
{
    public class ReportCache<T>
    {
        public ReportCache(int cacheTime)
        {
            this.cacheTime = cacheTime;
        }

        public async Task<List<T>> GetValueAsync(int count, Func<Task<List<T>>> getValueFunc)
        {
            List<T> resultList;
            locker.EnterReadLock();
            try
            {
                if (TryGetValue(count, out resultList))
                    return resultList;
            }
            finally
            {
                locker.ExitReadLock();
            }

            locker.EnterWriteLock();
            try
            {
                if (TryGetValue(count, out resultList))
                    return resultList;

                resultList = await getValueFunc();

                Update(resultList);
                return resultList.Take(count).ToList();
            }
            finally
            {
                locker.ExitWriteLock();
            }
        }

        public bool TryGetValue(int count, out List<T> result)
        {
            if (!((DateTime.Now - lastUpdateDateTime).TotalSeconds < cacheTime))
            {
                result = new List<T>();
                return false;
            }

            result = list.Take(count).ToList();
            return true;
        }

        public void Update(List<T> newResult)
        {
            list = newResult;
            lastUpdateDateTime = DateTime.Now;
        }

        private List<T> list;

        private DateTime lastUpdateDateTime;

        private readonly int cacheTime;

        private readonly ReaderWriterLockSlim locker = new ReaderWriterLockSlim();
    }
}
