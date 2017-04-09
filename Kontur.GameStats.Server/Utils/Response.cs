using System.Net;

namespace Kontur.GameStats.Server.Utils
{
    public class Response
    {
        public Response(HttpStatusCode httpStatusCode, string content)
        {
            HttpStatusCode = httpStatusCode;
            Content = content;
        }

        public HttpStatusCode HttpStatusCode { get; }

        public string Content { get; }
    }
}
