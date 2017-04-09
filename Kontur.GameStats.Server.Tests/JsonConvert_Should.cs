using Kontur.GameStats.Server.Domains;
using Kontur.GameStats.Server.Tests.Utils;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Kontur.GameStats.Server.Tests
{
    [TestFixture]
    public class JsonConvert_Should
    {
        [SetUp]
        public void Tests_Init()
        {
            EntryPoint.JsonConvert_Init();
        }

        [Test]
        public void Serialize_ServerInfo()
        {
            var actualSerialaizedServerInfo = JsonConvert.SerializeObject(ExampleDomains.ServerInfo);

            Assert.AreEqual(ExampleDomains.SerilizedServerInfo, actualSerialaizedServerInfo);
        }

        [Test]
        public void Serialize_Server()
        {
            var actualSerializedServer = JsonConvert.SerializeObject(ExampleDomains.Server);

            Assert.AreEqual(ExampleDomains.SerializedServer, actualSerializedServer);
        }

        [Test]
        public void Serialize_MatchResult()
        {
            var actualSerialaizedMatchResult = JsonConvert.SerializeObject(ExampleDomains.MatchResult);

            Assert.AreEqual(ExampleDomains.SerialaizedMatchResult, actualSerialaizedMatchResult);
        }

        [Test]
        public void Serialize_Match()
        {
            var actualSerializedMatch = JsonConvert.SerializeObject(ExampleDomains.Match);

            Assert.AreEqual(ExampleDomains.SerialaizedMatch, actualSerializedMatch);
        }

        [Test]
        public void Deserialize_ServerInfo()
        {
            var actualServerInfo = JsonConvert.DeserializeObject<ServerInfo>(ExampleDomains.SerilizedServerInfo);

            Assert.AreEqual(ExampleDomains.ServerInfo, actualServerInfo);
        }

        [Test]
        public void Deserialize_MatchResult()
        {
            var actualMatchResult = JsonConvert.DeserializeObject<MatchResult>(ExampleDomains.SerialaizedMatchResult);

            Assert.AreEqual(ExampleDomains.MatchResult, actualMatchResult);
        }

        [Test]
        public void Serialize_ServerStat()
        {
            var actualSerializedServerStat = JsonConvert.SerializeObject(ExampleDomains.ServerStat);

            Assert.AreEqual(ExampleDomains.SerialaizedServerStat, actualSerializedServerStat);
        }

        [Test]
        public void Serialize_PlayerStat()
        {
            var actualSerializedPlayerStat = JsonConvert.SerializeObject(ExampleDomains.PlayerStat);

            Assert.AreEqual(ExampleDomains.SerialaizedPlayerStat, actualSerializedPlayerStat);
        }

        [Test]
        public void Serialize_PopularServer()
        {
            var actualSerializedPopularServer = JsonConvert.SerializeObject(ExampleDomains.PopularServer);

            Assert.AreEqual(ExampleDomains.SerialaizedPopularServer, actualSerializedPopularServer);
        }
    }
}
