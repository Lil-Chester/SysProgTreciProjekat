using System.Text.Json.Serialization;

namespace WeatherRadarServer
{
    public class OpenMeteoResponse
    {
        [JsonPropertyName("hourly")]
        public HourlyData? Hourly { get; set; }
    }
}