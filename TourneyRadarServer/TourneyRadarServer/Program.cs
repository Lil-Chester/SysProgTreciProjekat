using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;

namespace TourneyRadarServer
{
    public class ApiResponse
    {
        [JsonPropertyName("data")]
        public List<ApiTournament>? Data { get; set; }
    }

    public class ApiTournament
    {
        [JsonPropertyName("name")] public string? Name { get; set; }
        [JsonPropertyName("date")] public string? Date { get; set; }
        [JsonPropertyName("end_date")] public string? EndDate { get; set; }
    }

    public class TournamentDto
    {
        public string Name { get; set; } = string.Empty;
        public string TimePeriod { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
    }

    public record FetchTournamentsCommand(string Country);
    public record StreamCompletedMessage();
    public record ErrorMessage(string Reason);
    public record GroupedResultResponse(object Data, string Error);

    public static class TournamentRxService
    {
        public static IObservable<TournamentDto> StreamTournaments(string country)
        {
            return Observable.Create<TournamentDto>(async (observer, ct) =>
            {
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Add("User-Agent", "TourneyRadarServer/1.0");

                string url = string.IsNullOrEmpty(country)
                    ? "https://tourneyradar-api.vercel.app/v1/tournaments"
                    : $"https://tourneyradar-api.vercel.app/v1/tournaments?country={country}";

                try
                {
                    var response = await client.GetAsync(url, ct);
                    response.EnsureSuccessStatusCode();
                    var json = await response.Content.ReadAsStringAsync(ct);
                    var apiData = JsonSerializer.Deserialize<ApiResponse>(json);

                    if (apiData?.Data != null)
                    {
                        foreach (var item in apiData.Data)
                        {
                            var dto = new TournamentDto
                            {
                                Name = item.Name ?? "Nepoznato",
                                TimePeriod = $"{item.Date} - {item.EndDate}",
                                Status = DetermineStatus(item.Date, item.EndDate)
                            };
                            observer.OnNext(dto);
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

        private static string DetermineStatus(string? startDateStr, string? endDateStr)
        {
            if (DateTime.TryParse(startDateStr, out var start) && DateTime.TryParse(endDateStr, out var end))
            {
                var now = DateTime.Now;
                if (now < start) return "Predstojeći";
                if (now > end) return "Završeni";
                return "Aktivni";
            }
            return "Nepoznat status";
        }
    }

    public class RequestHandlerActor : ReceiveActor
    {
        private readonly IActorRef _replyTo;
        private readonly List<TournamentDto> _internalState = new();

        public RequestHandlerActor(IActorRef replyTo, string country)
        {
            _replyTo = replyTo;
            IActorRef self = Self;

            TournamentRxService.StreamTournaments(country)
                .Subscribe(
                    onNext: tournament => self.Tell(tournament),
                    onError: ex => self.Tell(new ErrorMessage(ex.Message)),
                    onCompleted: () => self.Tell(new StreamCompletedMessage())
                );

            Receive<TournamentDto>(t =>
            {
                _internalState.Add(t);
            });

            Receive<StreamCompletedMessage>(_ =>
            {
                var grouped = new
                {
                    Predstojeci = _internalState.Where(x => x.Status == "Predstojeći").ToList(),
                    Aktivni = _internalState.Where(x => x.Status == "Aktivni").ToList(),
                    Zavrseni = _internalState.Where(x => x.Status == "Završeni").ToList()
                };

                _replyTo.Tell(new GroupedResultResponse(grouped, string.Empty));
                Context.Stop(self);
            });

            Receive<ErrorMessage>(err =>
            {
                _replyTo.Tell(new GroupedResultResponse(null, err.Reason));
                Context.Stop(self);
            });
        }

        public static Props Props(IActorRef replyTo, string country) =>
            Akka.Actor.Props.Create(() => new RequestHandlerActor(replyTo, country));
    }

    public class CoordinatorActor : ReceiveActor
    {
        public CoordinatorActor()
        {
            Receive<FetchTournamentsCommand>(cmd =>
            {
                var handler = Context.ActorOf(RequestHandlerActor.Props(Sender, cmd.Country)
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
                    Console.WriteLine($"[LOG GREŠKA] Upis u logs.txt neuspešan: {ex.Message}");
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

            using var system = ActorSystem.Create("TourneySystem", config);
            var coordinator = system.ActorOf(Props.Create<CoordinatorActor>(), "coordinator");

            var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");

            try
            {
                listener.Start();
            }
            catch (Exception ex)
            {
                Log($"[KRITIČNO] Neuspešno pokretanje servera: {ex.Message}");
                return;
            }

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("[SHUTDOWN] Signal za gašenje primljen. Pokrećem graceful shutdown...");
                _cts.Cancel();

                try { listener.Stop(); } catch { }
            };

            Console.WriteLine("===================================================");
            Console.WriteLine("Web Server pokrenut na http://localhost:8080/");
            Console.WriteLine("Pritisnite CTRL+C za gašenje servera.");
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
                        Log($"[GREŠKA] Problem pri prihvatanju konekcije: {ex.Message}");
                    }
                }
            }

            Log("[SHUTDOWN] Server više ne prima nove zahteve. Čekam obradu preostalih...");

            while (Volatile.Read(ref _activeRequests) > 0)
            {
                Log($"[SHUTDOWN] Preostalo zahteva u obradi: {_activeRequests}. Čekam 500ms...");
                await Task.Delay(500);
            }

            Log("[SHUTDOWN] Svi zahtevi završeni. Gasim Akka.NET ActorSystem...");
            await system.Terminate();
            Log("[SHUTDOWN] ActorSystem bezbedno ugašen. Aplikacija se zatvara.");
        }

        private static async Task HandleHttpRequest(HttpListenerContext context, IActorRef coordinator)
        {
            var request = context.Request;
            var response = context.Response;
            var reqId = Guid.NewGuid().ToString().Substring(0, 6);

            Log($"[#{reqId}] PRIMLJEN ZAHTEV: {request.HttpMethod} {request.Url}");

            try
            {
                var country = request.QueryString["country"] ?? "";
                var command = new FetchTournamentsCommand(country);

                var actorResponse = await coordinator.Ask<GroupedResultResponse>(command, TimeSpan.FromSeconds(60));

                response.ContentType = "application/json; charset=utf-8";

                if (!string.IsNullOrEmpty(actorResponse.Error))
                {
                    Log($"[#{reqId}] GREŠKA PRI OBRADI: {actorResponse.Error}");
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
                Log($"[#{reqId}] KRITIČNA GREŠKA: {ex.Message}");
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