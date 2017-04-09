using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Kontur.GameStats.Server.Utils;

namespace Kontur.GameStats.Server
{
    internal class StatServer : IDisposable
    {
        public StatServer()
        {
            listener = new HttpListener();

            var database = new Database(databasePath, cashTime, maxCountItemsInReport);
            requestHandler = new RequestHandler(database);

            Logger.InitLogger();
        }

        public void Start(string prefix)
        {
            lock(listener)
            {
                if(!isRunning)
                {
                    listener.Prefixes.Clear();
                    listener.Prefixes.Add(prefix);
                    listener.Start();

                    listenerThread = new Thread(Listen)
                                     {
                                         IsBackground = true,
                                         Priority = ThreadPriority.Highest
                                     };
                    listenerThread.Start();

                    isRunning = true;
                }
            }
        }

        public void Stop()
        {
            lock(listener)
            {
                if(!isRunning)
                    return;

                listener.Stop();

                listenerThread.Abort();
                listenerThread.Join();

                isRunning = false;
            }
        }

        public void Dispose()
        {
            if(disposed)
                return;

            disposed = true;

            Stop();

            listener.Close();
        }

        private void Listen()
        {
            while(true)
            {
                try
                {
                    if(listener.IsListening)
                    {
                        var context = listener.GetContext();
                        var exc = Task.Run(() => HandleContextAsync(context)).Exception;
                        if (exc != null)
                            throw exc;
                    }
                    else
                        Thread.Sleep(0);
                }
                catch(ThreadAbortException)
                {
                    return;
                }
                catch(Exception error)
                {
                    Logger.Log.Info(null, error);
                }
            }
        }

        private async Task HandleContextAsync(HttpListenerContext listenerContext)
        {
            try
            {
                var path = WebUtility.UrlDecode(listenerContext.Request.Url.AbsolutePath);
                var httpMethod = listenerContext.Request.HttpMethod;
                var requestBody = GetRequestBody(listenerContext);

                var response = await requestHandler.GetResponseAsync(path, httpMethod, requestBody);

                listenerContext.Response.StatusCode = (int)response.HttpStatusCode;

                if (response.Content != null)
                    using (var writer = new StreamWriter(listenerContext.Response.OutputStream))
                        writer.WriteLine(response.Content);
            }
            catch (Exception error)
            {
                listenerContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                Logger.Log.Info(null, error);
            }
            finally
            {
                listenerContext.Response.Close();
            }
        }

        private static string GetRequestBody(HttpListenerContext listenerContext)
        {
            using (var reader = new StreamReader(listenerContext.Request.InputStream))
            {
                return reader.ReadToEnd();
            }
        }

        private readonly HttpListener listener;

        private Thread listenerThread;

        private bool disposed;

        private volatile bool isRunning;

        private readonly RequestHandler requestHandler;

        private const string databasePath = "ServerStat.sqlite";
        
        private const int cashTime = 60;
        
        private const int maxCountItemsInReport = 50;
    }
}