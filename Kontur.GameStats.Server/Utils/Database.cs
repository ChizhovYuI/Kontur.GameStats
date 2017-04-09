using System;
using System.Data.SQLite;
using System.Linq;
using Kontur.GameStats.Server.Domains;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Inerfaces;

namespace Kontur.GameStats.Server.Utils
{
    public class Database : IDisposable
    {
        public Database() {}

        /// <summary>
        /// Конструктор класса базы данных
        /// </summary>
        /// <param name="dataSource">Путь к базе данных</param>
        /// <param name="cacheTime">Время актуальности значений в кэше в секундах</param>
        /// <param name="maxCountItemsInReport">Максимальное количество элементов в отчете Reports</param>
        public Database(string dataSource, int cacheTime, int maxCountItemsInReport)
        {
            this.maxCountItemsInReport = maxCountItemsInReport;

            connectionString =
                new SQLiteConnectionStringBuilder
                {
                    DataSource = dataSource,
                    Version = 3,
                    SyncMode = SynchronizationModes.Normal,
                    JournalMode = SQLiteJournalModeEnum.Wal,
                    DateTimeKind = DateTimeKind.Utc,
                    DateTimeFormat = SQLiteDateFormats.ISO8601
                }.ConnectionString;

            recentMathcesCache = new ReportCache<Match>(cacheTime);
            bestPlayersCache = new ReportCache<BestPlayer>(cacheTime);
            popularServersCache = new ReportCache<PopularServer>(cacheTime);
            serverStatCache = new StatsCache<ServerStat>(cacheTime);
            playerStatCache = new StatsCache<PlayerStat>(cacheTime);

            Init();
        }

        public void Init()
        {
            ExecuteNonQuery(
                Queries.CreateTableServer,
                Queries.CreateTableMatch,
                Queries.CreateTableScoreboard,
                Queries.CreateIndexMatchServer,
                Queries.CreateIndexMatchTimestamp,
                Queries.CreateIndexScoreboardMatchId,
                Queries.CreateIndexScoreboardSearchName,
                Queries.CreateTablePlayer,
                Queries.CreateIndexPlayerKillToDeathRatio
            );
        }

        public void DropTables()
        {
            ExecuteNonQuery(
                Queries.DropTableServer,
                Queries.DropTableMatch,
                Queries.DropTableScoreboard,
                Queries.DropTablePlayer
            );
        }

        public virtual async Task InsertOrUpdateServerAsync(Domains.Server server)
        {
            await ExecuteNonQueryAsync(Queries.InsertServer(server));
        }

        public virtual async Task<bool> TryInsertOrIgnoreMatchAsync(Match match)
        {
            return await ExecuteQueryAsync(TryInsertOrIgnoreMatchAsync, match);
        }

        public virtual async Task<ServerInfo> GetServerInfoAsync(string endpoint)
        {
            return await ExecuteQueryAsync(GetServerInfoAsync, endpoint);
        }

        public virtual async Task<List<Domains.Server>> GetAllServersAsync()
        {
            return await ExecuteQueryAsync(GetAllServersAsync);
        }

        public virtual async Task<MatchResult> GetMatchResultAsync(string endpoint, DateTime timestamp)
        {
            return await ExecuteQueryAsync(GetMatchResultAsync, new Match(endpoint, timestamp));
        }

        public virtual async Task<ServerStat> GetServerStatAsync(string endpoint)
        {
            return await GetStatWithCacheAsync(endpoint, serverStatCache, GetServerStatAsync);
        }

        public virtual async Task<PlayerStat> GetPlayerStatAsync(string name)
        {
            return await GetStatWithCacheAsync(name.ToLower(), playerStatCache, GetPlayerStatAsync);
        }

        public virtual async Task<List<Match>> GetRecentMatchesAsync(int count)
        {
            return await GetReportWithCacheAsync(count, recentMathcesCache, GetRecentMatchesAsync);
        }

        public virtual async Task<List<PopularServer>> GetPopularServersAsync(int count)
        {
            return await GetReportWithCacheAsync(count, popularServersCache, GetPopularServersAsync);
        }

        public virtual async Task<List<BestPlayer>> GetBestPlayersAsync(int count)
        {
            return await GetReportWithCacheAsync(count, bestPlayersCache, GetBestPlayersAsync);
        }

        private void ExecuteNonQuery(params string[] queries)
        {
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        var command = new SQLiteCommand(query, connection, transaction);
                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }

                connection.Close();
            }
        }

        private async Task<T> GetStatWithCacheAsync<T>(
            string key,
            StatsCache<T> cache,
            Func<SQLiteConnection, SQLiteTransaction, string, Task<T>> getEntityFunc) where T : class, ICacheable
        {
            return await cache.GetValueAsync(key, k => ExecuteQueryAsync(getEntityFunc, k));
        }

        private async Task<List<T>> GetReportWithCacheAsync<T>(
            int count,
            ReportCache<T> cache,
            Func<SQLiteConnection, SQLiteTransaction, Task<List<T>>> funcAsync)
        {
            return await cache.GetValueAsync(count, () => ExecuteQueryAsync(funcAsync));
        }

        private async Task ExecuteNonQueryAsync(params string[] queries)
        {
            using (var connection = new SQLiteConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        var command = new SQLiteCommand(query, connection, transaction);
                        await command.ExecuteNonQueryAsync();
                    }

                    transaction.Commit();
                }

                connection.Close();
            }
        }

        private async Task<T1> ExecuteQueryAsync<T1, T2>(
            Func<SQLiteConnection, SQLiteTransaction, T2, Task<T1>> funcAsync,
            T2 input)
        {
            T1 result;
            using (var connection = new SQLiteConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    result = await funcAsync(connection, transaction, input);

                    transaction.Commit();
                }

                connection.Close();
            }
            return result;
        }

        private async Task<T1> ExecuteQueryAsync<T1>(Func<SQLiteConnection, SQLiteTransaction, Task<T1>> funcAsync)
        {
            T1 result;
            using (var connection = new SQLiteConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                {
                    result = await funcAsync(connection, transaction);

                    transaction.Commit();
                }

                connection.Close();
            }
            return result;
        }

        private async Task<bool> TryInsertOrIgnoreMatchAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            Match match)
        {
            if (!await IsExistsServerAsync(match.Server, connection, transaction))
                return false;

            if (await IsExistsMatchAsync(match, connection, transaction))
                return true;

            await InsertMatchAsync(match, connection, transaction);
            var matchId = await GetLastInsertIdAsync(connection, transaction);
            await InsertScoreboardAsync(match.Results.Scoreboard, connection, transaction, matchId);
            await InsertBestPlayersAsync(match.Results.Scoreboard, connection, transaction);
            return true;
        }

        private async Task InsertMatchAsync(
            Match match,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var command = new SQLiteCommand(Queries.InsertMatch(match), connection, transaction);
            await command.ExecuteNonQueryAsync();
        }

        private async Task<bool> IsExistsServerAsync(string endpoint,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            return await IsExistsAsync(Queries.SelectExistServer(endpoint), connection, transaction);
        }

        private async Task<bool> IsExistsMatchAsync(
            Match match,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            return await IsExistsAsync(Queries.SelectExistMatch(match), connection, transaction);
        }

        private async Task<bool> IsExistsAsync(string query,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            return await ExecuteScalarAsync(query, connection, transaction) != null;
        }

        private async Task InsertScoreboardAsync(List<Scoreboard> scoreboard,
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            long matchId)
        {
            var command = new SQLiteCommand(Queries.InsertScoreboard(scoreboard, matchId), connection, transaction);
            await command.ExecuteNonQueryAsync();
        }

        private async Task InsertBestPlayersAsync(List<Scoreboard> scoreboards,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var bestPlayers = new List<BestPlayer>();
            foreach (var scoreboard in scoreboards)
            {
                var playerList = await ExecuteReaderAsync(Queries.SelectPlayer(scoreboard.Name.ToLower()),
                    connection,
                    transaction,
                    DataReaderHelper.GetPlayer);
                var player = playerList.FirstOrDefault();
                var newPlayer = new BestPlayer(scoreboard.Name,
                    (player?.Frags ?? 0) + scoreboard.Frags,
                    (player?.Kills ?? 0) + scoreboard.Kills,
                    (player?.Deaths ?? 0) + scoreboard.Deaths,
                    (player?.MatchCount ?? 0) + 1
                );
                bestPlayers.Add(newPlayer);
            }

            var command = new SQLiteCommand(Queries.InsertPlayers(bestPlayers), connection, transaction);
            await command.ExecuteNonQueryAsync();
        }

        private async Task<long> GetLastInsertIdAsync(SQLiteConnection connection, SQLiteTransaction transaction)
        {
            var command = new SQLiteCommand(Queries.SelectLastInsertId, connection, transaction);
            return (long)await command.ExecuteScalarAsync();
        }

        private async Task<ServerInfo> GetServerInfoAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            string endpoint)
        {
            var serverInfoList = await ExecuteReaderAsync(Queries.SelectServerInfo(endpoint),
                connection,
                transaction,
                DataReaderHelper.GetServerInfo);
            return serverInfoList.FirstOrDefault();
        }

        private async Task<List<Domains.Server>> GetAllServersAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var allServersList = await ExecuteReaderAsync(Queries.SelectAllServers,
                connection,
                transaction,
                DataReaderHelper.GetServer);
            return allServersList;
        }

        private async Task<MatchResult> GetMatchResultAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            Match match)
        {
            var matchResultList = await ExecuteReaderAsync(
                Queries.SelectMatchResult(match),
                connection,
                transaction,
                DataReaderHelper.GetMatchResult);
            var matchResult = matchResultList.FirstOrDefault();
            if (matchResult?.Id != null)
            {
                matchResult.Scoreboard = await GetScoreboardForMatchAsync(connection, transaction, matchResult.Id.Value);
            }

            return matchResult;
        }

        private async Task<ServerStat> GetServerStatAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            string endpoint)
        {
            var lastMatchDate = await GetLastMatchDateTimeAsync(connection, transaction);
            var matches = lastMatchDate != null
                ? await GetMatchesForServerStatAsync(endpoint, connection, transaction)
                : null;
            if (matches == null || matches.Count == 0)
                return new ServerStat(endpoint);

            return GetServerStat(lastMatchDate.Value, matches, endpoint);
        }

        private ServerStat GetServerStat(DateTime lastMatchDate, List<Match> matches, string endpoint)
        {
            var firstMatchDateTime = matches.Min(i => i.Timestamp);
            var countDays = (lastMatchDate.Date - firstMatchDateTime.Date).Days + 1;
            var totalMatchesPlayed = matches.Count;
            var maximumMatchesPerDay = matches.GroupBy(i => i.Timestamp.Date).Max(i => i.Count());
            var averageMatchesPerDay = (decimal)totalMatchesPlayed / countDays;
            var maximumPopulation = matches.Max(i => i.Results.Population);
            var averagePopulation = (decimal)matches.Sum(i => i.Results.Population) / totalMatchesPlayed;
            var top5GameModes =
                matches.GroupBy(i => i.Results.GameMode)
                       .OrderByDescending(i => i.Count())
                       .ThenBy(i => i.Key)
                       .Take(5)
                       .Select(i => i.Key).ToArray();
            var top5Maps =
                matches.GroupBy(i => i.Results.Map)
                       .OrderByDescending(i => i.Count())
                       .ThenBy(i => i.Key)
                       .Take(5)
                       .Select(
                           i => i.Key).ToArray();
            var serverStat = new ServerStat(endpoint,
                totalMatchesPlayed,
                maximumMatchesPerDay,
                averageMatchesPerDay,
                maximumPopulation,
                averagePopulation,
                top5GameModes,
                top5Maps);
            return serverStat;
        }

        private async Task<List<Match>> GetMatchesForServerStatAsync(string endpoint,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var matches = await ExecuteReaderAsync(
                Queries.GetMachesForServer(endpoint),
                connection,
                transaction,
                DataReaderHelper.GetMatchForServerStat);
            return matches;
        }

        private async Task<PlayerStat> GetPlayerStatAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            string name)
        {
            var lastMatchDate = await GetLastMatchDateTimeAsync(connection, transaction);
            var scoreboards = lastMatchDate != null
                ? await GetScoreboardForPlayerStatAsync(name, connection, transaction)
                : null;
            if (scoreboards == null || scoreboards.Count == 0)
                return new PlayerStat(name);

            return GetPlayerStat(lastMatchDate.Value, scoreboards, name);
        }

        private async Task<List<Scoreboard>> GetScoreboardForPlayerStatAsync(
            string name,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var scoreboards = await ExecuteReaderAsync(
                Queries.SelectScoreboardForPlayerStat(name),
                connection,
                transaction,
                DataReaderHelper.GetScoreboardWithMatch);
            return scoreboards;
        }

        private PlayerStat GetPlayerStat(DateTime lastMatchDate, List<Scoreboard> scoreboards, string name)
        {
            var firstMatchDateTime = scoreboards.Min(i => i.Match.Timestamp);
            var countDays = (lastMatchDate.Date - firstMatchDateTime.Date).Days + 1;
            var totalMatchesPlayed = scoreboards.Count;
            var totalMatchesWon = scoreboards.Count(i => i.Place == 1);
            var favoriteServer =
                scoreboards.GroupBy(i => i.Match.Server).OrderByDescending(i => i.Count()).ThenBy(i => i.Key).First()
                           .Select(i => i.Match.Server).First();
            var uniqueServers = scoreboards.GroupBy(i => i.Match.Server).Count();
            var favoriteGameMode =
                scoreboards.GroupBy(i => i.Match.Results.GameMode).OrderByDescending(i => i.Count()).ThenBy(i => i.Key)
                           .First().Select(i => i.Match.Results.GameMode).First();
            var averageScoreboardPercent =
                scoreboards.Select(
                               i =>
                                   i.Match.Results.Population == 1
                                       ? 100
                                       : (decimal)(i.Match.Results.Population - i.Place) /
                                         (i.Match.Results.Population - 1) * 100)
                           .Sum() / totalMatchesPlayed;
            var maximumMatchesPerDay = scoreboards.GroupBy(i => i.Match.Timestamp.Date).Max(i => i.Count());
            var averageMatchesPerDay = (decimal)totalMatchesPlayed / countDays;
            var lastMatchPlayed = scoreboards.Max(i => i.Match.Timestamp);
            var totalDeaths = scoreboards.Sum(i => i.Deaths);
            var killToDeathRatio = totalDeaths > 0 ? (decimal)scoreboards.Sum(i => i.Kills) / totalDeaths : 0;
            var playerStat = new PlayerStat(name,
                totalMatchesPlayed,
                totalMatchesWon,
                favoriteServer,
                uniqueServers,
                favoriteGameMode,
                averageScoreboardPercent,
                maximumMatchesPerDay,
                averageMatchesPerDay,
                lastMatchPlayed,
                killToDeathRatio);
            return playerStat;
        }

        private async Task<List<Match>> GetRecentMatchesAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var matches = await ExecuteReaderAsync(
                Queries.GetRecentMatches(maxCountItemsInReport),
                connection,
                transaction,
                DataReaderHelper.GetMatch);
            matches.ForEach(
                async i =>
                    i.Results.Scoreboard =
                        i.Results.Id != null
                            ? await GetScoreboardForMatchAsync(connection, transaction, i.Results.Id.Value)
                            : new List<Scoreboard>());
            return matches;
        }

        private async Task<List<Scoreboard>> GetScoreboardForMatchAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            long matchId)
        {
            return await ExecuteReaderAsync(
                Queries.SelectScoreboardForMatch(matchId),
                connection,
                transaction,
                DataReaderHelper.GetScoreboard);
        }

        private async Task<List<PopularServer>> GetPopularServersAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var lastMatchDateTime = await GetLastMatchDateTimeAsync(connection, transaction);
            if (lastMatchDateTime == null)
                return new List<PopularServer>();
            {
                var popularServers =
                    await ExecuteReaderAsync(
                        Queries.SelectPopularServers(maxCountItemsInReport, lastMatchDateTime.Value),
                        connection,
                        transaction,
                        DataReaderHelper.GetPopularServer);
                return popularServers;
            }
        }

        private async Task<DateTime?> GetLastMatchDateTimeAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            return (DateTime?)await ExecuteScalarAsync(Queries.LastMatchDate, connection, transaction);
        }

        private async Task<object> ExecuteScalarAsync(string query,
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var command = new SQLiteCommand(query, connection, transaction);
            return await command.ExecuteScalarAsync();
        }

        private async Task<List<BestPlayer>> GetBestPlayersAsync(
            SQLiteConnection connection,
            SQLiteTransaction transaction)
        {
            var bestPlayers = await ExecuteReaderAsync(
                Queries.SelectBestPlayers(maxCountItemsInReport),
                connection,
                transaction,
                DataReaderHelper.GetBestPlayer);
            return bestPlayers;
        }

        private async Task<List<T>> ExecuteReaderAsync<T>(string query,
            SQLiteConnection connection,
            SQLiteTransaction transaction,
            Func<DbDataReader, T> readFunc)
        {
            var resultList = new List<T>();
            var command = new SQLiteCommand(query, connection, transaction);
            var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                resultList.Add(readFunc(reader));
            }

            reader.Close();
            return resultList;
        }

        private readonly string connectionString;

        private readonly ReportCache<Match> recentMathcesCache;

        private readonly ReportCache<BestPlayer> bestPlayersCache;

        private readonly ReportCache<PopularServer> popularServersCache;

        private readonly StatsCache<ServerStat> serverStatCache;

        private readonly StatsCache<PlayerStat> playerStatCache;
        
        /// <summary>
        /// Максимальное количество элементов для вывода в отчеты
        /// </summary>
        private readonly int maxCountItemsInReport;

        public void Dispose()
        {
            serverStatCache.Dispose();
            playerStatCache.Dispose();
            //DropTables();
        }
    }
}
