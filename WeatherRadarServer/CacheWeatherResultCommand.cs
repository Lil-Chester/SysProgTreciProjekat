namespace WeatherRadarServer
{
    public record CacheWeatherResultCommand(FetchWeatherCommand Command, WeatherResultResponse Response);
}