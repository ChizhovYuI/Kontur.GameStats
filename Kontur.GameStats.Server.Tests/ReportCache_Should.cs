using System.Collections.Generic;
using System.Threading;
using Kontur.GameStats.Server.Domains;
using Kontur.GameStats.Server.Tests.Utils;
using Kontur.GameStats.Server.Utils;
using NUnit.Framework;

namespace Kontur.GameStats.Server.Tests
{
    [TestFixture]
    public class ReportCache_Should
    {
        private const int cacheTime = 1;

        private const int millisecondInSecond = 1000;

        private const int maxItemsCount = 50;

        private ReportCache<PopularServer> cache;

        [SetUp]
        public void SetUp()
        {
            cache = new ReportCache<PopularServer>(cacheTime);
        }

        [Test]
        public void Count0_WhenTryGetItems_ForEmptyCache()
        {
            List<PopularServer> result;
            cache.TryGetValue(maxItemsCount, out result);

            Assert.Zero(result.Count);
        }

        [Test]
        public void Count1_WhenTryGetItems_AfterUpdate()
        {
            List<PopularServer> result;

            cache.Update(new List<PopularServer> {ExampleDomains.PopularServer});
            cache.TryGetValue(maxItemsCount, out result);

            Assert.AreEqual(1, result.Count);
        }

        [Test]
        public void Count0_WhenTryGetItems_AfterUpdateWaitCacheTime()
        {
            List<PopularServer> result;

            cache.Update(new List<PopularServer> {ExampleDomains.PopularServer});
            Thread.Sleep(cacheTime*millisecondInSecond);
            cache.TryGetValue(maxItemsCount, out result);

            Assert.Zero(result.Count);
        }
    }
}
