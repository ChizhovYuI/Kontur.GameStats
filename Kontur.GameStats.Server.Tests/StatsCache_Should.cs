using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Domains;
using Kontur.GameStats.Server.Tests.Utils;
using Kontur.GameStats.Server.Utils;
using NUnit.Framework;

namespace Kontur.GameStats.Server.Tests
{
    [TestFixture]
    public class StatsCache_Should
    {
        [SetUp]
        public void SetUp()
        {
            cache = new StatsCache<ServerStat>(cacheTime);
        }
        
        [Test]
        public void Equals_WhenGetValueAsync_getValueFuncReturnThisValue()
        {
            var serverStat = ExampleDomains.ServerStat;

            var actualServerStat = cache
                .GetValueAsync(serverStat.Endpoint, s => GetServerStatAsync(serverStat)).Result;

            Assert.AreEqual(serverStat, actualServerStat);
        }

        [Test]
        public void FirstValue_WhenGetServerStatAsync_secondGetValueFuncReturnOtherValue()
        {
            var serverStat = ExampleDomains.ServerStat;

            var expectedServerStat = cache
                .GetValueAsync(serverStat.Endpoint, s => GetServerStatAsync(serverStat)).Result;
            var actualServerStat = cache
                .GetValueAsync(serverStat.Endpoint, s => GetServerStatAsync(new ServerStat())).Result;

            Assert.AreEqual(expectedServerStat, actualServerStat);
        }

        [Test]
        public void NotEqual_WhenTryGetItem_AfterCacheTime()
        {
            var serverStat = ExampleDomains.ServerStat;

            var oldSeverStat = cache
                .GetValueAsync(serverStat.Endpoint, s => GetServerStatAsync(serverStat)).Result;
            Thread.Sleep(cacheTime * millisecondInSecond);
            var actualServerStat = cache.GetValueAsync(ExampleDomains.ServerStat.Endpoint,
                s =>
                    GetServerStatAsync(new ServerStat("newEndpoint"))).Result;
            Assert.AreNotEqual(oldSeverStat, actualServerStat);
        }

        private static Task<ServerStat> GetServerStatAsync(ServerStat serverStat)
        {
            var task = new Task<ServerStat>(() => serverStat);
            task.Start();
            return task;
        }

        private const int cacheTime = 2;

        private const int millisecondInSecond = 1000;

        private StatsCache<ServerStat> cache;
    }
}
