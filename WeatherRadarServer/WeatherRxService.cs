using System;
using System.Net.Http;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text.Json;

namespace WeatherRadarServer
{
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
    }
}