namespace WeatherRadarServer
{
    public class WeatherDataDto
    {
        public string Time { get; set; } = string.Empty;
        public double Temperature { get; set; }
        public double UvIndex { get; set; }
    }
}