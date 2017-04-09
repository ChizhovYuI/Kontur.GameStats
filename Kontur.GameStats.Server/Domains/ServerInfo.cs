using System.Linq;

namespace Kontur.GameStats.Server.Domains
{
    public class ServerInfo
    {
        public ServerInfo(string name, string[] gameModes)
        {
            Name = name;
            GameModes = gameModes;
        }

        public string Name { get; }

        public string[] GameModes { get; }

        protected bool Equals(ServerInfo other)
        {
            return string.Equals(Name, other.Name) &&
                   (GameModes?.SequenceEqual(other.GameModes) ?? false);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((ServerInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name?.GetHashCode() ?? 0) * 397) ^ (GameModes?.GetHashCode() ?? 0);
            }
        }

        public static class Properties
        {
            public const string Name = "name";

            public const string GameModes = "gameModes";
        }
    }
}
