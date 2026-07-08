using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;

namespace WeatherRadarServer
{
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

                var finalResponse = new WeatherResultResponse(calculatedMetrics, string.Empty);
                _replyTo.Tell(finalResponse);

                Context.Parent.Tell(new CacheWeatherResultCommand(
                    new FetchWeatherCommand(lat, lng, startDate, endDate),
                    finalResponse
                ));

                Context.Stop(self);
            });
        }

        public static Props Props(IActorRef replyTo, string lat, string lng, string startDate, string endDate) =>
            Akka.Actor.Props.Create(() => new WeatherRequestHandlerActor(replyTo, lat, lng, startDate, endDate));
    }
}