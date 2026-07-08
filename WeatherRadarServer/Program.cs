using System;
using System.IO;
using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;

namespace WeatherRadarServer
{
    class Program
    {
        private static int _activeRequests = 0;
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private static readonly object _fileLock = new object();
        internal static void Log(string message)
        {
            string formattedMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.WriteLine(formattedMessage);

            lock (_fileLock)
            {
                try
                {
                    File.AppendAllText("logs.txt", formattedMessage + Environment.NewLine);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LOG ERROR] Neuspešan upis u logs.txt: {ex.Message}");
                }
            }
        }

        static async Task Main(string[] args)
        {
            var config = ConfigurationFactory.ParseString(@"
                custom-task-dispatcher {
                    type = Dispatcher
                    executor = thread-pool-executor
                    thread-pool-executor {
                        core-pool-size-min = 2
                        core-pool-size-max = 10
                    }
                }");

            using var system = ActorSystem.Create("WeatherSystem", config);
            var coordinator = system.ActorOf(Props.Create<WeatherCoordinatorActor>(), "weatherCoordinator");
            var cacheCleanerSubscription = Observable.Interval(TimeSpan.FromMinutes(1))
                .SubscribeOn(TaskPoolScheduler.Default)
                .ObserveOn(TaskPoolScheduler.Default)
                .Subscribe(_ =>
                {
                    Log("[BACKGROUND THREAD] Čišćenje keša.");
                    coordinator.Tell(new CleanCacheMessage());
                });

            var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");

            try
            {
                listener.Start();
            }
            catch (Exception ex)
            {
                Log($"[KRITIČNO] Server nije mogao da se pokrene: {ex.Message}");
                cacheCleanerSubscription.Dispose();
                return;
            }

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Log("[SHUTDOWN] Pokrenut graceful shutdown.");
                _cts.Cancel();
                try { listener.Stop(); } catch { }
            };

            Log("===================================================");
            Log("Weather Web Server pokrenut na http://localhost:8080/");
            Log("Pritisnite CTRL+C za bezbedno gašenje servera.");
            Log("===================================================\n");

            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var context = await listener.GetContextAsync();
                    Interlocked.Increment(ref _activeRequests);
                    _ = Task.Run(() => HandleHttpRequest(context, coordinator));
                }
                catch (HttpListenerException) when (_cts.Token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (!_cts.Token.IsCancellationRequested)
                    {
                        Log($"[GREŠKA] Problem pri prihvatanju HTTP zahteva: {ex.Message}");
                    }
                }
            }

            Log("[SHUTDOWN] Nove konekcije su blokirane. Čekanje na aktivne zahteve...");
            while (Volatile.Read(ref _activeRequests) > 0)
            {
                Log($"[SHUTDOWN] Još uvek se obrađuju zahtevi: {_activeRequests}. Čekanje 500ms...");
                await Task.Delay(500);
            }

            Log("[SHUTDOWN] Gasišenje tajmera za keš...");
            cacheCleanerSubscription.Dispose();

            Log("[SHUTDOWN] Svi zahtevi procesirani. Gašenje Akka ActorSystem...");
            await system.Terminate();
            Log("[SHUTDOWN] Server je uspešno i bezbedno ugašen.");
        }

        private static async Task HandleHttpRequest(HttpListenerContext context, IActorRef coordinator)
        {
            var request = context.Request;
            var response = context.Response;
            var reqId = Guid.NewGuid().ToString().Substring(0, 6);

            Log($"[#{reqId}] PRIMLJEN ZAHTEV: {request.HttpMethod} {request.Url}");

            try
            {
                var lat = request.QueryString["lat"] ?? "43.32";
                var lng = request.QueryString["lng"] ?? "21.89";
                var startDate = request.QueryString["start"] ?? DateTime.Now.ToString("yyyy-MM-dd");
                var endDate = request.QueryString["end"] ?? DateTime.Now.AddDays(3).ToString("yyyy-MM-dd");

                var command = new FetchWeatherCommand(lat, lng, startDate, endDate);

                var actorResponse = await coordinator.Ask<WeatherResultResponse>(command, TimeSpan.FromSeconds(30));

                response.ContentType = "application/json; charset=utf-8";

                if (!string.IsNullOrEmpty(actorResponse.Error))
                {
                    Log($"[#{reqId}] GREŠKA TOKOM OBRADE: {actorResponse.Error}");
                    response.StatusCode = 500;
                    WriteResponse(response, JsonSerializer.Serialize(new { error = actorResponse.Error }));
                }
                else
                {
                    Log($"[#{reqId}] USPEŠNO OBRAĐEN ZAHTEV.");
                    response.StatusCode = 200;
                    WriteResponse(response, JsonSerializer.Serialize(actorResponse.Data, new JsonSerializerOptions { WriteIndented = true }));
                }
            }
            catch (Exception ex)
            {
                Log($"[#{reqId}] KRITIČNI PROPROPST: {ex.Message}");
                response.StatusCode = 500;
                WriteResponse(response, JsonSerializer.Serialize(new { error = "Internal Server Error" }));
            }
            finally
            {
                response.Close();
                Interlocked.Decrement(ref _activeRequests);
            }
        }

        private static void WriteResponse(HttpListenerResponse response, string content)
        {
            var buffer = Encoding.UTF8.GetBytes(content);
            response.ContentLength64 = buffer.Length;
            response.OutputStream.Write(buffer, 0, buffer.Length);
        }
    }
}