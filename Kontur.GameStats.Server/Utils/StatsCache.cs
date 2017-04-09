using System;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Inerfaces;

namespace Kontur.GameStats.Server.Utils
{
    public class StatsCache<T> : IDisposable where T : class, ICacheable
    {
        public StatsCache(int cacheTime)
        {
            this.cacheTime = cacheTime;
        }

        public async Task<T> GetValueAsync(string key, Func<string, Task<T>> getValueFunc)
        {
            var newValue = new Lazy<Task<T>>(() => getValueFunc(key));
            var value = (Lazy<Task<T>>)memoryCache.AddOrGetExisting(key, newValue, DateTime.Now.AddSeconds(cacheTime));
            return await (value ?? newValue).Value;
        }

        public void Dispose()
        {
            memoryCache.Dispose();
        }


        private T GetValue(string key)
        {
            return memoryCache.Get(key) as T;
        }

        private void Add(T value)
        {
            memoryCache.Add(value.Key, value, DateTime.Now.AddSeconds(cacheTime));
        }

        /// <summary>
        /// Время жизни значения в кэше в секундах
        /// </summary>
        private readonly int cacheTime;

        private readonly MemoryCache memoryCache = new MemoryCache(typeof(T).FullName);

        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
    }
}
