using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Tests.Utils;
using Kontur.GameStats.Server.Utils;
using Moq;
using NUnit.Framework;

namespace Kontur.GameStats.Server.Tests
{
    [TestFixture]
    public class RequestHandler_Should
    {
        [SetUp]
        public void CreateMoq()
        {
            EntryPoint.JsonConvert_Init();
            databaseMock = new Mock<Database>();
            requestHandler = new RequestHandler(databaseMock.Object);
        }

        [Test]
        public void Ok_WhenProccess_PUT_ServerInfo()
        {
            var server = ExampleDomains.Server;
            databaseMock.Setup(d => d.InsertOrUpdateServerAsync(server)).Returns(Task.CompletedTask);

            var response = requestHandler.GetResponseAsync($"/servers/{server.Endpoint}/info",
                PUT,
                ExampleDomains.SerializedServer).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
        }

        [Test]
        public void Ok_WhenProccess_PUT_MatchResult()
        {
            var match = ExampleDomains.Match;
            databaseMock.Setup(d => d.TryInsertOrIgnoreMatchAsync(match)).Returns(Task.Run(() => true));
            var url = $"/servers/{match.Server}/matches/{match.Timestamp.ToUniversalTime():yyyy-MM-ddTHH:mm:ssZ}";
            var response = requestHandler.GetResponseAsync(url,
                PUT,
                ExampleDomains.SerialaizedMatchResult).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
        }

        [Test]
        public void BadRequest_WhenProccess_PUT_MatchResult()
        {
            var match = ExampleDomains.Match;
            databaseMock.Setup(d => d.TryInsertOrIgnoreMatchAsync(match)).Returns(Task.Run(() => false));
            var url = $"/servers/{match.Server}/matches/{match.Timestamp:yyyy-MM-ddTHH:mm:ssZ}";
            var response = requestHandler.GetResponseAsync(url,
                PUT,
                ExampleDomains.SerialaizedMatchResult).Result;

            Assert.AreEqual(HttpStatusCode.BadRequest, response.HttpStatusCode);
        }

        [Test]
        public void OK_WhenProccess_GET_ServerInfo()
        {
            var server = ExampleDomains.Server;
            databaseMock.Setup(d => d.GetServerInfoAsync(server.Endpoint)).Returns(Task.Run(() => server.Info));
            var url = $"/servers/{server.Endpoint}/info";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual(ExampleDomains.SerilizedServerInfo, response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_MatchResult()
        {
            var match = ExampleDomains.Match;
            databaseMock.Setup(d => d.GetMatchResultAsync(match.Server, match.Timestamp)).Returns(Task.Run(() => match.Results));
            var url = $"/servers/{match.Server}/matches/{match.Timestamp:yyyy-MM-ddTHH:mm:ssZ}";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual(ExampleDomains.SerialaizedMatchResult, response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_AllServers()
        {
            var server = ExampleDomains.Server;
            databaseMock.Setup(d => d.GetAllServersAsync()).Returns(Task.Run(() => new List<Domains.Server> {server}));
            var url = $"/servers/info";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual($"[{ExampleDomains.SerializedServer}]", response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_ServerStat()
        {
            var serverStat = ExampleDomains.ServerStat;
            databaseMock.Setup(d => d.GetServerStatAsync(serverStat.Endpoint)).Returns(Task.Run(() => serverStat));
            var url = $"/servers/{serverStat.Endpoint}/stats";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual(ExampleDomains.SerialaizedServerStat, response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_PlayerStat()
        {
            var playerName = "player";
            var playerStat = ExampleDomains.PlayerStat;
            databaseMock.Setup(d => d.GetPlayerStatAsync(playerName)).Returns(Task.Run(() => playerStat));
            var url = $"/players/{playerName}/stats";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual(ExampleDomains.SerialaizedPlayerStat, response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_RecentMatches_WithoutCount()
        {
            var match =  ExampleDomains.Match;
            databaseMock.Setup(d => d.GetRecentMatchesAsync(5)).Returns(Task.Run(() => new List<Domains.Match> {match}));
            var url = "/reports/recent-matches";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual($"[{ExampleDomains.SerialaizedMatch}]", response.Content);
        }

        [Test]
        public void OK_WhenProccess_GET_RecentMatches_WithCount()
        {
            var count = 10;
            var match =  ExampleDomains.Match;
            databaseMock.Setup(d => d.GetRecentMatchesAsync(count)).Returns(Task.Run(() => new List<Domains.Match> {match}));
            var url = $"/reports/recent-matches/{count}";
            var response = requestHandler.GetResponseAsync(url,
                GET,
                null).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.AreEqual($"[{ExampleDomains.SerialaizedMatch}]", response.Content);
        }

        private Mock<Database> databaseMock;

        private RequestHandler requestHandler;

        private const string PUT = "PUT";

        private const string GET = "GET";
    }
}
