using System;
using System.Linq;
using Kontur.GameStats.Server.Domains;
using Kontur.GameStats.Server.Tests.Utils;
using Kontur.GameStats.Server.Utils;
using NUnit.Framework;

namespace Kontur.GameStats.Server.Tests
{
    [TestFixture]
    public class Database_Should
    {
        private Database database;

        private int cacheTime = 10;

        private int maxValuesInReport = 50;

        [SetUp]
        public void DatabaseInit()
        {
            database = new Database("Test.sqlite", cacheTime, maxValuesInReport);
            database.DropTables();
            database.Init();
        }

        [TearDown]
        public void DatabaseDrop()
        {
            database.DropTables();
            database.Dispose();
        }

        [Test]
        public void Success_WhenInsertServer()
        {
            Assert.DoesNotThrowAsync(() => database.InsertOrUpdateServerAsync(ExampleDomains.Server));
        }

        [Test]
        public void Success_InsertTwice_OneServer()
        {
            Success_WhenInsertServer();
            Success_WhenInsertServer();
        }
[Test]
        public void False_WhenInsertMatch_ForNotAdvertiseServer()
        {
            var isInsertMatch = database.TryInsertOrIgnoreMatchAsync(ExampleDomains.Match).Result;

            Assert.False(isInsertMatch);
        }

        [Test]
        public void True_WhenInsertMatch_ForAdvertiseServer()
        {
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();

            var isInsertMatch = database.TryInsertOrIgnoreMatchAsync(ExampleDomains.Match).Result;

            Assert.True(isInsertMatch);
        }

        [Test]
        public void Null_WhenGetServerInfo_ForNotAdvertiseServer()
        {
            var serverInfo = database.GetServerInfoAsync("example.com-1234").Result;

            Assert.Null(serverInfo);
        }

        [Test]
        public void Success_WhenGetServerInfo_ForAdvertiseServer()
        {
            var server = ExampleDomains.Server;
            database.InsertOrUpdateServerAsync(server).Wait();

            var actualServerInfo = database.GetServerInfoAsync(server.Endpoint).Result;

            Assert.AreEqual(server.Info, actualServerInfo);
        }

        [Test]
        public void EmptyList_WhenGetAllServers_ForEmptyDatabase()
        {
            var servers = database.GetAllServersAsync().Result;

            Assert.AreEqual(0, servers.Count);
        }

        [Test]
        public void ServersCount1_WhenGetAllServers_For1ServerInDatabase()
        {
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();

            var servers = database.GetAllServersAsync().Result;

            Assert.AreEqual(1, servers.Count);
        }

        [Test]
        public void ServersCount2_WhenGetAllServers_For2ServersInDatabase()
        {
            var data = new RandomData(2);
            var servers = data.GetServers();

            servers.ForEach(s => database.InsertOrUpdateServerAsync(s).Wait());
            var actualServers = database.GetAllServersAsync().Result;

            Assert.AreEqual(servers.Count, actualServers.Count);
        }

        [Test]
        public void Null_WhenGetMatchResult_ForEmptyDatabse()
        {
            var matchResult = database.GetMatchResultAsync("example-1234", DateTime.Now).Result;

            Assert.Null(matchResult);
        }

        [Test]
        public void Success_WhenGetMatchResult_For1MatchInDatabase()
        {
            var server = ExampleDomains.Server;
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var actualMatchResult = database.GetMatchResultAsync(match.Server, match.Timestamp).Result;

            Assert.AreEqual(match.Results, actualMatchResult);
        }

        [Test]
        public void EmptyServerStat_WhenGetServerStat_ForEmptyDatabase()
        {
            var actualServerStat = database.GetServerStatAsync(ExampleDomains.Server.Endpoint).Result;

            Assert.AreEqual(new ServerStat(), actualServerStat);
        }

        [Test]
        public void Success_WhenGetServerStat_For1MatchInDatabase()
        {
            var server = ExampleDomains.Server;
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            Assert.DoesNotThrowAsync(() => database.GetServerStatAsync(server.Endpoint));
        }

        [Test]
        public void TotalMatchesPlayed10_WhenGetServerStat_For10MatchesInDatabase()
        {
            var countMatches = 10;
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var matches = randomData.GetUniqueRandomMatchesForServer(server, countMatches);
            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(m => database.TryInsertOrIgnoreMatchAsync(m).Wait());

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;

            Assert.AreEqual(countMatches, actualServerStat.TotalMatchesPlayed);
        }

        [Test]
        public void MaximumMatchesPerDay_WhenGetServerStat_For10Matches()
        {
            var countMatches = 10;
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var matches = randomData.GetUniqueRandomMatchesForServer(server, countMatches);
            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(m => database.TryInsertOrIgnoreMatchAsync(m).Wait());

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;
            var expectedMaximumMatchesPerDay = matches.GroupBy(i => i.Timestamp.Date).Max(i => i.Count());

            Assert.AreEqual(expectedMaximumMatchesPerDay, actualServerStat.MaximumMatchesPerDay);
        }

        [Test]
        public void AverageMatchesPerDay_WhenGetServerStat_For10Matches()
        {
            var countMatches = 10;
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var matches = randomData.GetUniqueRandomMatchesForServer(server, countMatches);
            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(m => database.TryInsertOrIgnoreMatchAsync(m).Wait());

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;
            var expectedAverageMatchesPerDay = (decimal)matches.Count /
                                               ((matches.Max(i => i.Timestamp).Date - matches.Min(i => i.Timestamp).Date)
                                                .Days + 1);

            Assert.AreEqual(expectedAverageMatchesPerDay, actualServerStat.AverageMatchesPerDay);
        }

        [Test]
        public void MaximumPopulation_WhenGetServerStat_For10Matches()
        {
            var countMatches = 10;
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var matches = randomData.GetUniqueRandomMatchesForServer(server, countMatches);
            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(m => database.TryInsertOrIgnoreMatchAsync(m).Wait());

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;
            var expectedMaximumPopulation = matches.Max(i => i.Results.Scoreboard.Count);

            Assert.AreEqual(expectedMaximumPopulation, actualServerStat.MaximumPopulation);
        }

        [Test]
        public void AveragePopulation_WhenGetServerStat_For10Matches()
        {
            var countMatches = 10;
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var matches = randomData.GetUniqueRandomMatchesForServer(server, countMatches);
            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(m => database.TryInsertOrIgnoreMatchAsync(m).Wait());

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;

            var expectedAveragePopulation = (decimal)matches.Sum(i => i.Results.Scoreboard.Count) / matches.Count;
            Assert.AreEqual(expectedAveragePopulation, actualServerStat.AveragePopulation);
        }

        [Test]
        public void MaximumMatchesPerDay1_WhenGetServerStat_For2MAtchesOnDaysBorder()
        {
            var countServers = 1;
            var randomData = new RandomData(countServers);
            var server = randomData.GetRandomServer();
            var match1 = randomData.GetRandomMatchForServer(server, DateTime.Parse("2017-01-22T23:59:59Z"));
            var match2 = randomData.GetRandomMatchForServer(server, DateTime.Parse("2017-01-23T00:00:00Z"));
            database.InsertOrUpdateServerAsync(server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match1).Wait();
            database.TryInsertOrIgnoreMatchAsync(match2).Wait();

            var actualServerStat = database.GetServerStatAsync(server.Endpoint).Result;

            Assert.AreEqual(1, actualServerStat.MaximumMatchesPerDay);
        }

        [Test]
        public void AverageMatchesPerDay_WhenGetServerStat_For2ServersWith1MatchOnDifferentDays()
        {
            var countServers = 2;
            var randomData = new RandomData(countServers);
            var servers = randomData.GetServers();
            var match1 = randomData.GetRandomMatchForServer(servers[0], DateTime.Parse("2017-01-22T12:00:00Z"));
            var match2 = randomData.GetRandomMatchForServer(servers[1], DateTime.Parse("2017-01-23T12:00:00Z"));
            servers.ForEach(s => database.InsertOrUpdateServerAsync(s).Wait());
            database.TryInsertOrIgnoreMatchAsync(match1).Wait();
            database.TryInsertOrIgnoreMatchAsync(match2).Wait();

            var actualServerStat1 = database.GetServerStatAsync(servers[0].Endpoint).Result;
            var actualServerStat2 = database.GetServerStatAsync(servers[1].Endpoint).Result;

            Assert.AreEqual(0.5, actualServerStat1.AverageMatchesPerDay);
            Assert.AreEqual(1, actualServerStat2.AverageMatchesPerDay);
        }

        [Test]
        public void EmptyPlayerStat_WhenGetPlayerStat_ForEmptyDatabase()
        {
            var playerStat = database.GetPlayerStatAsync("player").Result;

            Assert.AreEqual(new PlayerStat(), playerStat);
        }

        [Test]
        public void TotalMatchesPlayer1_WhenGetPlayerStat_ForOneMatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard.First().Name).Result;

            Assert.AreEqual(1, playerStat.TotalMatchesPlayed);
        }

        [Test]
        public void TotalMatchesWon1_WhenGetPlayerStat_ForWinnerInOneMatch()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard.First().Name).Result;

            Assert.AreEqual(1, playerStat.TotalMatchesWon);
        }

        [Test]
        public void TotalMatchesWon0_WhenGetPlayerStat_ForLoserInOneMatch()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(0, playerStat.TotalMatchesWon);
        }

        [Test]
        public void AverageScoreboardPercent100_WhenGetPlayerStat_ForWinnerInOneMatch()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard.First().Name).Result;

            Assert.AreEqual(100, playerStat.AverageScoreboardPercent);
        }

        [Test]
        public void AverageScoreboardPercent0_WhenGetPlayerStat_ForLoserInOneMatch()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(0, playerStat.AverageScoreboardPercent);
        }

        [Test]
        public void FavoriteGameMode_WhenGetPlayerStat_ForOneMatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(match.Results.GameMode, playerStat.FavoriteGameMode);
        }

        [Test]
        public void FavoriteServer_WhenGetPlayerStat_ForOneMatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(match.Server, playerStat.FavoriteServer);
        }

        [Test]
        public void UniqueServers_WhenGetPlayerStat_ForOneMatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(1, playerStat.UniqueServers);
        }

        [Test]
        public void LastMatchPlayed_WhenGetPlayerStat_ForOneMatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[1].Name).Result;

            Assert.AreEqual(match.Timestamp.ToUniversalTime(), playerStat.LastMatchPlayed);
        }

        [Test]
        public void Success_WhenGetPlayerStat_ForRandomMatches()
        {
            var data = new RandomData(10);
            var servers = data.GetServers();
            var matches = servers.SelectMany(i => data.GetUniqueRandomMatchesForServer(i, 10, 10)).ToList();
            servers.ForEach(i => database.InsertOrUpdateServerAsync(i).Wait());
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());

            Assert.DoesNotThrowAsync(() => database.GetPlayerStatAsync(matches[0].Results.Scoreboard[0].Name));
        }

        [Test]
        public void AverageScoreboardPercent100_WhenGetPlayerStat_ForOnePlayerInMatch()
        {
            var data = new RandomData(1, 1, 1, 1);
            var server = data.GetRandomServer();
            var match = data.GetRandomMatchForServer(server, DateTime.Now);

           database.InsertOrUpdateServerAsync(server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();
            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[0].Name).Result;

            Assert.AreEqual(100, playerStat.AverageScoreboardPercent);
        }

        [Test]
        public void IgnoreCase_WhenGetPlayerStat_ForDifferentCasePlayerName()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var playerStat = database.GetPlayerStatAsync(match.Results.Scoreboard[0].Name.ToUpper()).Result;

            Assert.AreEqual(1, playerStat.TotalMatchesPlayed);
        }

        [Test]
        public void Success_WhenGetRecentMatches_For1MatchInDatabase()
        {
            var match = ExampleDomains.Match;
            database.InsertOrUpdateServerAsync(ExampleDomains.Server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match).Wait();

            var matches = database.GetRecentMatchesAsync(5).Result;

            Assert.AreEqual(1, matches.Count);
            Assert.AreEqual(match, matches[0]);
        }

        [Test]
        public void MatchesCount5_WhenGet5RecentMatches_For10MatchesInDatabase()
        {
            var data = new RandomData(1);
            var server = data.GetRandomServer();
            var matches = data.GetUniqueRandomMatchesForServer(server, 10);

            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());
            var expectedMatches = matches.OrderByDescending(i => i.Timestamp).Take(5).ToList();
            var actualMatches = database.GetRecentMatchesAsync(5).Result;

            Assert.AreEqual(5, actualMatches.Count);
            Assert.True(expectedMatches.SequenceEqual(actualMatches));
        }

        [Test]
        public void PlayersCount0_WhenGetBestPlayers_ForEmptyDatabase()
        {
            var actualBestPlayers = database.GetBestPlayersAsync(1).Result;

            Assert.AreEqual(0, actualBestPlayers.Count);
        }

        [Test]
        public void PlayersCount0_WhenGetBestPlayers_For9Mathces1Player()
        {
            var data = new RandomData(1, 1, 1, 1);
            var server = data.GetRandomServer();
            var matches = data.GetUniqueRandomMatchesForServer(server, 9);

            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());
            var actualBestPlayers = database.GetBestPlayersAsync(1).Result;

            Assert.AreEqual(0, actualBestPlayers.Count);
        }

        [Test]
        public void PlayersCount1_WhenGetBestPlayers_For10Mathces1Player()
        {
            var data = new RandomData(1, 1, 1, 1);
            var server = data.GetRandomServer();
            var matches = data.GetUniqueRandomMatchesForServer(server, 10);

            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());
            var actualBestPlayers = database.GetBestPlayersAsync(1).Result;

            Assert.AreEqual(1, actualBestPlayers.Count);
        }

        [Test]
        public void Success_WhenGetBestPlayers_For100RandomMathces5Players()
        {
            var data = new RandomData(1);
            var server = data.GetRandomServer();
            var matches = data.GetUniqueRandomMatchesForServer(server, 100);

            database.InsertOrUpdateServerAsync(server).Wait();
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());
            var actualBestPlayers = database.GetBestPlayersAsync(5).Result;

            Assert.AreNotEqual(0, actualBestPlayers.Count);
        }

        [Test]
        public void CountServers0_WhenGetPopularServers_ForServerWithoutMatches()
        {
            var data = new RandomData(1);
            var server = data.GetRandomServer();

            database.InsertOrUpdateServerAsync(server).Wait();
            var actualPopularServers = database.GetPopularServersAsync(5).Result;

            Assert.AreEqual(0, actualPopularServers.Count);
        }

        [Test]
        public void CountServers5_WhenGetPopularServers_For5ServersWithMatches()
        {
            var count = 5;
            var data = new RandomData(count);
            var servers = data.GetServers();
            var matches = servers.SelectMany(i => data.GetUniqueRandomMatchesForServer(i, 1)).ToList();
            servers.ForEach(i => database.InsertOrUpdateServerAsync(i).Wait());
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());

            var actualPopularServers = database.GetPopularServersAsync(count).Result;

            Assert.AreEqual(count, actualPopularServers.Count);
        }

        [Test]
        public void CountServers50_WhenGetPopularServers_For51ServersWithMatches()
        {
            var count = 51;
            var data = new RandomData(count);
            var servers = data.GetServers();
            var matches = servers.SelectMany(i => data.GetUniqueRandomMatchesForServer(i, 1)).ToList();
            servers.ForEach(i => database.InsertOrUpdateServerAsync(i).Wait());
            matches.ForEach(i => database.TryInsertOrIgnoreMatchAsync(i).Wait());

            var actualPopularServers = database.GetPopularServersAsync(count).Result;

            Assert.AreEqual(maxValuesInReport, actualPopularServers.Count);
        }

        [Test]
        public void AverageMatchesPerDay1_WhenGetPopularServers_For2MathcesInNeighboringDays()
        {
            var data = new RandomData(1);
            var server = data.GetRandomServer();
            var match1 = data.GetRandomMatchForServer(server, DateTime.Now);
            var match2 = data.GetRandomMatchForServer(server, DateTime.Now.AddDays(1));
            database.InsertOrUpdateServerAsync(server).Wait();
            database.TryInsertOrIgnoreMatchAsync(match1).Wait();
            database.TryInsertOrIgnoreMatchAsync(match2).Wait();

            var actualPopularServers = database.GetPopularServersAsync(1).Result;

            Assert.AreEqual(1m, actualPopularServers[0].AverageMatchesPerDay);
        }
    }
}
