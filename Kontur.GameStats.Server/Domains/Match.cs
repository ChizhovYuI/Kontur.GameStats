using System;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class Match
    {
        public Match(DateTime timestamp, MatchResult results)
        {
            Timestamp = timestamp;
            Results = results;
        }

        public Match(string server, DateTime timestamp)
        {
            Server = server;
            Timestamp = timestamp;
        }

        [JsonConstructor]
        public Match(string server, DateTime timestamp, MatchResult results)
        {
            Server = server;
            Timestamp = timestamp;
            Results = results;
        }

        public string Server { get; }

        public DateTime Timestamp { get; }

        public MatchResult Results { get; }

        protected bool Equals(Match other)
        {
            return string.Equals(Server, other.Server) &&
                   Timestamp.ToUniversalTime().Equals(other.Timestamp.ToUniversalTime()) &&
                   Equals(Results, other.Results);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((Match)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Server?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ Timestamp.GetHashCode();
                hashCode = (hashCode * 397) ^ (Results?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string Server = "server";

            public const string Timestamp = "timestamp";
        }
    }
}
