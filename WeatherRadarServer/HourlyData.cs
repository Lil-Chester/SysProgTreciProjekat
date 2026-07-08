using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace WeatherRadarServer
{
    public class HourlyData
    {
        [JsonPropertyName("time")] public List<string>? Time { get; set; }
        [JsonPropertyName("temperature_2m")] public List<double>? Temperature2m { get; set; }
        [JsonPropertyName("uv_index")] public List<double>? UvIndex { get; set; }
    }
}