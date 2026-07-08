namespace WeatherRadarServer
{
    public record FetchWeatherCommand(string Lat, string Lng, string StartDate, string EndDate);
}