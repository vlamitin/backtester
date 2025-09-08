document.addEventListener("DOMContentLoaded", async () => {

    const symbol = new URL(window.location.href).searchParams.get("symbol") || "SOLUSDT"

    document.title = symbol + " inner_candles"

    const innerCandles = await fetch(
        `http://localhost:8000/api/inner_candles/${symbol}`
    ).then(response => response.json());

    const candles = []
    const plotBands = []

    innerCandles.forEach((innerCandle, i) => {
        candles.push(toHCCandle(
            innerCandle,
            // `CME Open 18:00-19:00`,
            // candle_types_map[day.date_readable]["CME Open"],
            // 'c'
        ))


        if (isEvenHourInUTC(new Date(innerCandle[5]))) {
            plotBands.push({
                from: Number(new Date(innerCandle[5])),
                to: Number(new Date(innerCandle[5])) + 1,
                color: 'lightgray'
            })
        }
        //
        // if ([1].includes(new Date(day.date_readable).getDay())) {
        //     plotBands.push({
        //         from: Number(new Date(day.date_readable)),
        //         to: Number(new Date(day.date_readable)) + 1000 * 60 * 60,
        //         color: 'red'
        //     })
        // }
    })

    Highcharts.setOptions({
        time: {
            timezone: "America/New_York"
        }
    });

    // create the chart
    const chart = Highcharts.stockChart('container', {
        plotOptions: {
            candlestick: {
                color: 'black',
                lineColor: 'black',
                upColor: 'white',
                upLineColor: 'black'
            }
        },

        rangeSelector: {
            buttons: [{
                type: 'day',
                count: 1,
                text: '1D',
                title: 'View 1 day'
            }, {
                type: 'day',
                count: 3,
                text: '3D',
                title: 'View 3 days'
            },  {
                type: 'week',
                count: 1,
                text: '1W',
                title: 'View 1 week'
            }, {
                type: 'day',
                count: 10,
                text: '10D',
                title: 'View 10 days'
            },   {
                type: 'week',
                count: 2,
                text: '2W',
                title: 'View 2 weeks'
            },  {
                type: 'month',
                count: 1,
                text: '1M',
                title: 'View 1 month'
            },   {
                type: 'month',
                count: 3,
                text: '3M',
                title: 'View 3 months'
            }, {
                type: 'all',
                text: 'All',
                title: 'View all'
            }],
            selected: 0
        },

        tooltip: {
            distance: 32,
            formatter() {
                let s = '<b>' + new Highcharts.Time({ timezone: "America/New_York" })
                    .dateFormat('%Y-%m-%d %H:%M', this.x) + '</b>'

                // s += `<br/><b>${this.point.custom.name}</b> ${this.point.custom.type}`
                // if (this.point.custom.impact == "UNSPECIFIED") {
                //     s += `<br/>impact: ${this.point.custom.impact}`
                // } else {
                //     s += `<br/>impact: <b>${this.point.custom.impact}</b>`
                // }


                // const perf = (this.point.close - this.point.open).toFixed(2)
                // const volat = (this.point.high - this.point.low).toFixed(2)

                s += `<br/>open:   ${Number(this.point.open.toFixed(3)).toLocaleString('ru-RU')}`
                s += `<br/>high:   ${Number(this.point.high.toFixed(3)).toLocaleString('ru-RU')}`
                s += `<br/>low:    ${Number(this.point.low.toFixed(3)).toLocaleString('ru-RU')}`
                s += `<br/>close:  ${Number(this.point.close.toFixed(3)).toLocaleString('ru-RU')}`
                // s += `<br/>perf: ${perf > 0 ? "+" : ""}${perf} (<b>${(perf / this.point.open * 100).toFixed(2)}%</b>)`
                // s += `<br/>volat: ${volat} (<b>${(volat / this.point.open * 100).toFixed(2)}%</b>)`
                // const wicksFractions = [(this.point.high - Math.max(this.point.open, this.point.close)) / (this.point.high - this.point.low),
                // (Math.min(this.point.open, this.point.close) - this.point.low) / (this.point.high - this.point.low)]
                // const body_fraction = 1 - wicksFractions[0] - wicksFractions[1]
                // s += `<br/>anatomy: ${wicksFractions[0].toFixed(2)} <b>${(body_fraction).toFixed(2)}</b> ${wicksFractions[1].toFixed(2)} `

                return s;
            }
        },

        title: {
            text: `${symbol} sessions`,
        },

        series: [{
            type: 'candlestick',
            name: `${symbol} sessions`,
            data: candles,
            dataGrouping: {
                enabled: false
            }
        }],

        xAxis: {
            plotBands,
            // min: candles[0].x,
            crosshair: {
                label: {
                    enabled: true
                }
            }
        },

        yAxis: {
            crosshair: {
                label: {
                    enabled: true
                }
            }
        }
    });

    chart.xAxis[0].setExtremes(
        chart.series[0].options.data[0].x,
        chart.series[0].options.data[0].x + 8 * 3600 * 1000
    );

    window.chart = chart
    window.candles = candles
})

function toHCCandle(candle) {
    const [open, high, low, close, _, date] = candle
    return {
        x: Number(new Date(date)),
        open,
        high,
        low,
        close,
        custom: {
            // label,
            // name,
            // type: typeImpact[0],
            // impact: typeImpact[1]
        },
        dataLabels: {
            enabled: true,
            style: {
                fontWeight: 'bold'
            },
            // formatter() {
            //     return label
            // },
            alignTo: 'plotEdges',
            verticalAlign: 'top',
            overflow: true,
            crop: false
        },
    }
}

function isEvenHourInUTC(date) {
    const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: "UTC",
        hour12: false,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
    }).formatToParts(date);

    const values = Object.fromEntries(parts.map(p => [p.type, p.value]));

    const hour = parseInt(values.hour, 10);
    const minute = parseInt(values.minute, 10);
    const second = parseInt(values.second, 10);

    return minute === 0 && second === 0 && hour % 2 === 0;
}
