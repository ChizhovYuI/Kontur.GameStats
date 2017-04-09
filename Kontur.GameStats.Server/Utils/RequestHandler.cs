using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Domains;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Utils
{
    public class RequestHandler
    {
        public RequestHandler(Database database)
        {
            this.database = database;

            commands = new[]
                       {
                           new Command(UrlPattern.ServerInfo, WebRequestMethods.Http.Put, InsertServer),
                           new Command(UrlPattern.MatchResult, WebRequestMethods.Http.Put, InsertMatch),
                           new Command(UrlPattern.ServerInfo, WebRequestMethods.Http.Get, GetServerInfo),
                           new Command(UrlPattern.MatchResult, WebRequestMethods.Http.Get, GetMatchResult),
                           new Command(UrlPattern.AllServers, WebRequestMethods.Http.Get, GetAllServers),
                           new Command(UrlPattern.ServerStat, WebRequestMethods.Http.Get, GetServerStat),
                           new Command(UrlPattern.PlayerStat, WebRequestMethods.Http.Get, GetPlayerStat),
                           new Command(UrlPattern.RecentMatches, WebRequestMethods.Http.Get, GetRecentMatches),
                           new Command(UrlPattern.BestPlayers, WebRequestMethods.Http.Get, GetBestPlayers),
                           new Command(UrlPattern.PopularServers, WebRequestMethods.Http.Get, GetPopularServers)
                       };
        }

        public async Task<Response> GetResponseAsync(string path, string httpMethod, string requestBody)
        {
            var command =
                commands.FirstOrDefault(i => i.UrlPatternRegex.IsMatch(path) && i.HttpMethod.Equals(httpMethod));
            if (command == null)
            {
                return new Response(HttpStatusCode.BadRequest, null);
            }

            var response = command.Func(requestBody, SplitUrlPath(path, command.UrlPatternRegex));
            return await response;
        }

        private async Task<Response> InsertServer(string requestBody, GroupCollection splitUrl)
        {
            var serverInfo = GetValueFromRequestBody<ServerInfo>(requestBody);
            var endpoint = splitUrl[1].ToString();
            var server = new Domains.Server(endpoint, serverInfo);

            await database.InsertOrUpdateServerAsync(server);

            return new Response(HttpStatusCode.OK, null);
        }

        private async Task<Response> InsertMatch(string requestBody, GroupCollection splitUrl)
        {
            var matchResult = GetValueFromRequestBody<MatchResult>(requestBody);
            var endpoint = splitUrl[1].ToString();
            DateTime timestamp;
            if (!DateTime.TryParse(splitUrl[2].ToString(), out timestamp))
                return new Response(HttpStatusCode.BadRequest, null);
            var match = new Domains.Match(endpoint, timestamp, matchResult);

            var isSuccess = await database.TryInsertOrIgnoreMatchAsync(match);

            return !isSuccess
                ? new Response(HttpStatusCode.BadRequest, null)
                : new Response(HttpStatusCode.OK, null);
        }

        private async Task<Response> GetServerInfo(string requestBody, GroupCollection splitUrl)
        {
            var endpoint = splitUrl[1].ToString();
            var serverInfo = await database.GetServerInfoAsync(endpoint);

            return serverInfo == null
                ? new Response(HttpStatusCode.NotFound, null)
                : new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(serverInfo));
        }

        private async Task<Response> GetMatchResult(string requestBody, GroupCollection splitUrl)
        {
            var endpoint = splitUrl[1].ToString();
            var timestamp = DateTime.Parse(splitUrl[2].ToString()).ToUniversalTime();

            var matchResult = await database.GetMatchResultAsync(endpoint, timestamp);

            return matchResult == null
                ? new Response(HttpStatusCode.NotFound, null)
                : new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(matchResult));
        }

        private async Task<Response> GetAllServers(string requestBody, GroupCollection splitUrl)
        {
            var servers = await database.GetAllServersAsync();

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(servers));
        }

        private async Task<Response> GetServerStat(string requestBody, GroupCollection splitUrl)
        {
            var endpoint = splitUrl[1].ToString();

            var serverStat = await database.GetServerStatAsync(endpoint);

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(serverStat));
        }

        private async Task<Response> GetPlayerStat(string requestBody, GroupCollection splitUrl)
        {
            var name = splitUrl[1].ToString();

            var playerStat = await database.GetPlayerStatAsync(name);

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(playerStat));
        }

        private async Task<Response> GetRecentMatches(string requestBody, GroupCollection splitUrl)
        {
            var count = GetCountFromUrl(splitUrl);

            var matches = await database.GetRecentMatchesAsync(count);

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(matches));
        }

        private async Task<Response> GetBestPlayers(string requestBody, GroupCollection splitUrl)
        {
            var count = GetCountFromUrl(splitUrl);

            var players = await database.GetBestPlayersAsync(count);

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(players));
        }

        private async Task<Response> GetPopularServers(string requestBody, GroupCollection splitUrl)
        {
            var count = GetCountFromUrl(splitUrl);

            var servers = await database.GetPopularServersAsync(count);

            return new Response(HttpStatusCode.OK, JsonConvert.SerializeObject(servers));
        }

        private static GroupCollection SplitUrlPath(string path, Regex regex)
        {
            return regex.Match(path).Groups;
        }

        private static T GetValueFromRequestBody<T>(string requestBody)
        {
            return JsonConvert.DeserializeObject<T>(requestBody);
        }

        private static int GetCountFromUrl(GroupCollection splitUrl)
        {
            int count;
            var strCount = splitUrl[1].ToString().TrimStart('/');
            return !int.TryParse(strCount, out count)
                ? defaultCountItemsInReport
                : count;
        }

        private static class UrlPattern
        {
            public const string ServerInfo = @"^/servers/(\S+-\d+)/info$";

            public const string MatchResult = @"^/servers/(\S+-\d+)/matches/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)$";

            public const string AllServers = @"^/servers/info$";

            public const string ServerStat = @"^/servers/(\S+-\d+)/stats$";

            public const string PlayerStat = @"^/players/(.+)/stats$";

            public const string RecentMatches = @"^/reports/recent-matches(/-{0,1}\d+){0,1}$";

            public const string BestPlayers = @"^/reports/best-players(/-{0,1}\d+){0,1}$";

            public const string PopularServers = @"^/reports/popular-servers(/-{0,1}\d+){0,1}$";
        }

        private readonly Command[] commands;

        private readonly Database database;

        private const int defaultCountItemsInReport = 5;

        private class Command
        {
            public Regex UrlPatternRegex { get; }

            public string HttpMethod { get; }

            public Func<string, GroupCollection, Task<Response>> Func { get; }

            public Command(string urlPattern,
                string httpMethod,
                Func<string, GroupCollection, Task<Response>> func)
            {
                UrlPatternRegex = new Regex(urlPattern);
                HttpMethod = httpMethod;
                Func = func;
            }
        }
    }
}
