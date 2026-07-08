using System;
using System.Collections.Generic;
using Akka.Actor;

namespace WeatherRadarServer
{
    public class WeatherCoordinatorActor : ReceiveActor
    {
        private readonly Dictionary<FetchWeatherCommand, (WeatherResultResponse Response, DateTime ExpiresAt)> _cache = new();
        private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);

        public WeatherCoordinatorActor()
        {
            Receive<FetchWeatherCommand>(cmd =>
            {
                var now = DateTime.Now;

                if (_cache.TryGetValue(cmd, out var cacheEntry) && now < cacheEntry.ExpiresAt)
                {
                    Sender.Tell(cacheEntry.Response);
                    return;
                }

                Context.ActorOf(WeatherRequestHandlerActor.Props(Sender, cmd.Lat, cmd.Lng, cmd.StartDate, cmd.EndDate)
                    .WithDispatcher("custom-task-dispatcher"));
            });

            Receive<CacheWeatherResultCommand>(cacheCmd =>
            {
                if (string.IsNullOrEmpty(cacheCmd.Response.Error))
                {
                    var expiresAt = DateTime.Now.Add(CacheDuration);
                    _cache[cacheCmd.Command] = (cacheCmd.Response, expiresAt);
                }
            });

            Receive<CleanCacheMessage>(_ =>
            {
                var now = DateTime.Now;
                var expiredKeys = new List<FetchWeatherCommand>();

                foreach (var kvp in _cache)
                {
                    if (now >= kvp.Value.ExpiresAt)
                    {
                        expiredKeys.Add(kvp.Key);
                    }
                }

                foreach (var key in expiredKeys)
                {
                    _cache.Remove(key);
                }

                Program.Log($"[CACHE] Pozadinsko čišćenje završeno. Obrisano isteklih zapisa: {expiredKeys.Count}. Trenutno u kešu: {_cache.Count}");
            });
        }
    }
}