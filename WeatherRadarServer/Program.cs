using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;

namespace WeatherRadarServer
{
    public class OpenMeteoResponse
    {
        [JsonPropertyName("hourly")]
        public HourlyData? Hourly { get; set; }
    }

    public class HourlyData
    {
        [JsonPropertyName("time")] public List<string>? Time { get; set; }
        [JsonPropertyName("temperature_2m")] public List<double>? Temperature2m { get; set; }
        [JsonPropertyName("uv_index")] public List<double>? UvIndex { get; set; }
    }

    public class WeatherDataDto
    {
        public string Time { get; set; } = string.Empty;
        public double Temperature { get; set; }
        public double UvIndex { get; set; }
    }

    public record FetchWeatherCommand(string Lat, string Lng, string StartDate, string EndDate);
    public record StreamCompletedMessage();
    public record ErrorMessage(string Reason);
    public record WeatherResultResponse(object? Data, string Error);

    public static class WeatherRxService
    {
        public static IObservable<WeatherDataDto> StreamWeatherData(string lat, string lng, string startDate, string endDate)
        {
            return Observable.Create<WeatherDataDto>(async (observer, ct) =>
            {
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Add("User-Agent", "WeatherRadarServer/1.0");
                string url = $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&start_date={startDate}&end_date={endDate}&hourly=temperature_2m,uv_index";

                try
                {
                    var response = await client.GetAsync(url, ct);
                    response.EnsureSuccessStatusCode();
                    var json = await response.Content.ReadAsStringAsync(ct);
                    var apiData = JsonSerializer.Deserialize<OpenMeteoResponse>(json);

                    if (apiData?.Hourly?.Time != null)
                    {
                        var hourly = apiData.Hourly;
                        int count = hourly.Time.Count;

                        for (int i = 0; i < count; i++)
                        {
                            var dto = new WeatherDataDto
                            {
                                Time = hourly.Time[i],
                                Temperature = hourly.Temperature2m?[i] ?? 0.0,
                                UvIndex = hourly.UvIndex?[i] ?? 0.0
                            };
                            observer.OnNext(dto); // Emitovanje poruke aktoru
                        }
                    }
                    observer.OnCompleted();
                }
                catch (Exception ex)
                {
                    observer.OnError(ex);
                }
            })
            .SubscribeOn(TaskPoolScheduler.Default)
            .ObserveOn(TaskPoolScheduler.Default);
        }
    }

    public class WeatherRequestHandlerActor : ReceiveActor
    {
        private readonly IActorRef _replyTo;
        private readonly List<WeatherDataDto> _internalTimeSeries = new();

        public WeatherRequestHandlerActor(IActorRef replyTo, string lat, string lng, string startDate, string endDate)
        {
            _replyTo = replyTo;
            IActorRef self = Self;

            WeatherRxService.StreamWeatherData(lat, lng, startDate, endDate)
                .Subscribe(
                    onNext: data => self.Tell(data),
                    onError: ex => self.Tell(new ErrorMessage(ex.Message)),
                    onCompleted: () => self.Tell(new StreamCompletedMessage())
                );

            Receive<WeatherDataDto>(data =>
            {
                _internalTimeSeries.Add(data);
            });

            Receive<StreamCompletedMessage>(_ =>
            {
                if (_internalTimeSeries.Count == 0)
                {
                    _replyTo.Tell(new WeatherResultResponse(null, "Nema podataka za zadati period."));
                    Context.Stop(self);
                    return;
                }

                var temperatures = _internalTimeSeries.Select(x => x.Temperature).ToList();
                var uvIndices = _internalTimeSeries.Select(x => x.UvIndex).ToList();

                var calculatedMetrics = new
                {
                    Period = $"{startDate} do {endDate}",
                    Coordinates = new { Latitude = lat, Longitude = lng },
                    TemperatureMetrics = new
                    {
                        Min = temperatures.Min(),
                        Max = temperatures.Max(),
                        Average = Math.Round(temperatures.Average(), 2)
                    },
                    UvIndexMetrics = new
                    {
                        Min = uvIndices.Min(),
                        Max = uvIndices.Max(),
                        Average = Math.Round(uvIndices.Average(), 2)
                    },
                    TotalHoursAnalyzed = _internalTimeSeries.Count
                };

                _replyTo.Tell(new WeatherResultResponse(calculatedMetrics, string.Empty));
                Context.Stop(self);
            });

            Receive<ErrorMessage>(err =>
            {
                _replyTo.Tell(new WeatherResultResponse(null, err.Reason));
                Context.Stop(self);
            });
        }

        public static Props Props(IActorRef replyTo, string lat, string lng, string startDate, string endDate) =>
            Akka.Actor.Props.Create(() => new WeatherRequestHandlerActor(replyTo, lat, lng, startDate, endDate));
    }

    public class WeatherCoordinatorActor : ReceiveActor
    {
        public WeatherCoordinatorActor()
        {
            Receive<FetchWeatherCommand>(cmd =>
            {
                Context.ActorOf(WeatherRequestHandlerActor.Props(Sender, cmd.Lat, cmd.Lng, cmd.StartDate, cmd.EndDate)
                    .WithDispatcher("custom-task-dispatcher"));
            });
        }
    }

    class Program
    {
        private static int _activeRequests = 0;
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private static readonly object _fileLock = new object();

        private static void Log(string message)
        {
            string formattedMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";

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

            var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");

            try
            {
                listener.Start();
            }
            catch (Exception ex)
            {
                Log($"[KRITIČNO] Server nije mogao da se pokrene: {ex.Message}");
                return;
            }

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("Pokrenut graceful shutdown. Zatvaram ulazne konekcije...");
                Log("[SHUTDOWN] Pokrenut graceful shutdown. Zatvaram ulazne konekcije...");
                _cts.Cancel();
                try { listener.Stop(); } catch { }
            };
            Log("Weather Web Server pokrenut na http://localhost:8080/");
            Log("Pritisnite CTRL+C za bezbedno gašenje servera.");
            Console.WriteLine("===================================================");
            Console.WriteLine("Weather Web Server pokrenut na http://localhost:8080/");
            Console.WriteLine("Pritisnite CTRL+C za bezbedno gašenje servera.");
            Console.WriteLine("===================================================\n");

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

            Log("[SHUTDOWN] Nove konekcije su blokirane. Čekam da se završe aktivni zahtevi...");
            while (Volatile.Read(ref _activeRequests) > 0)
            {
                Log($"[SHUTDOWN] Još uvek se obrađuje zahteva: {_activeRequests}. Čekam 500ms...");
                await Task.Delay(500);
            }

            Log("[SHUTDOWN] Svi zahtevi procesirani. Gasim Akka ActorSystem...");
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