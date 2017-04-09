
namespace Kontur.GameStats.Server.Domains
{
    public class Server
    {
        public Server(string endpoint, ServerInfo info)
        {
            Endpoint = endpoint;
            Info = info;
        }

        public string Endpoint { get; }

        public ServerInfo Info { get; }

        protected bool Equals(Server other)
        {
            return string.Equals(Endpoint, other.Endpoint) && Equals(Info, other.Info);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((Server)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Endpoint?.GetHashCode() ?? 0) * 397) ^ (Info?.GetHashCode() ?? 0);
            }
        }

        public static class Properties
        {
            public const string Endpoint = "endpoint";
        }
    }
}
