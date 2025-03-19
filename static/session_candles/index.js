(async () => {

    const symbol = "BTCUSDT"

    document.title = symbol + " sessions"

    const days = await fetch(
        `http://localhost:8000/api/days/${symbol}`
    ).then(response => response.json());

    const sessions = await fetch(
        `http://localhost:8000/api/sessions/${symbol}`
    ).then(response => response.json());

    const candles = []
    const plotBands = []

    const candle_types = {
        "CME Open": "",
        "Asia Open": "",
        "London Open": "",
        "Early session": "",
        "Premarket": "",
        "NY AM Open": "",
        "NY AM": "",
        "NY Lunch": "",
        "NY PM": "",
        "NY PM Close": "",
    }

    const candle_types_map = {}

    sessions.forEach(s => {
        if (!candle_types_map[s.day_date]) {
            candle_types_map[s.day_date] = {}
        }
        candle_types_map[s.day_date][s.name] = s.type
    })

    days.forEach((day, i) => {
        day.cme_as_candle[5] && candles.push(toHCCandle(day.cme_as_candle, `CME Open 18:00-19:00 ${candle_types_map[day.date_readable]["CME Open"] || ""}`, 'c'))
        day.asia_as_candle[5] && candles.push(toHCCandle(day.asia_as_candle, `Asia Open 19:00-22:00 ${candle_types_map[day.date_readable]["Asia Open"] || ""}`, 'a'))
        day.london_as_candle[5] && candles.push(toHCCandle(day.london_as_candle, `London Open 02:00-05:00 ${candle_types_map[day.date_readable]["London Open"] || ""}`, 'l'))
        day.early_session_as_candle[5] && candles.push(toHCCandle(day.early_session_as_candle, `Early session 07:00-08:00 ${candle_types_map[day.date_readable]["Early session"] || ""}`, 'e'))
        day.premarket_as_candle[5] && candles.push(toHCCandle(day.premarket_as_candle, `Premarket 08:00-09:30 ${candle_types_map[day.date_readable]["Premarket"] || ""}`, 'p'))
        day.ny_am_open_as_candle[5] && candles.push(toHCCandle(day.ny_am_open_as_candle, `NY AM Open 09:30-10:00 ${candle_types_map[day.date_readable]["NY AM Open"] || ""}`, 'O'))
        day.ny_am_as_candle[5] && candles.push(toHCCandle(day.ny_am_as_candle, `NY AM 10:00-12:00 ${candle_types_map[day.date_readable]["NY AM"] || ""}`, 'A'))
        day.ny_lunch_as_candle[5] && candles.push(toHCCandle(day.ny_lunch_as_candle, `NY Lunch 12:00-13:00 ${candle_types_map[day.date_readable]["NY Lunch"] || ""}`, 'L'))
        day.ny_pm_as_candle[5] && candles.push(toHCCandle(day.ny_pm_as_candle, `NY PM 13:00-15:00 ${candle_types_map[day.date_readable]["NY PM"] || ""}`, 'P'))
        day.ny_pm_close_as_candle[5] && candles.push(toHCCandle(day.ny_pm_close_as_candle, `NY PM Close 15:00-16:00 ${candle_types_map[day.date_readable]["NY PM Close"] || ""}`, 'C'))


        if (![6, 0].includes(new Date(day.date_readable).getDay())) {
            plotBands.push({
                from: Number(new Date(day.date_readable)),
                to: Number(new Date(day.date_readable)) + 1000 * 60 * 15,
                color: 'lightgray'
            })
        }

        if ([1].includes(new Date(day.date_readable).getDay())) {
            plotBands.push({
                from: Number(new Date(day.date_readable)),
                to: Number(new Date(day.date_readable)) + 1000 * 60 * 60,
                color: 'red'
            })
        }
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
            selected: 4
        },

        tooltip: {
            distance: 32,
            formatter() {
                let s = '<b>' + new Highcharts.Time({ timezone: "America/New_York" })
                    .dateFormat('%a %b %e %Y', this.x) + '</b>'

                s += `<br/><b>${this.description}</b>`

                const perf = (this.point.close - this.point.open).toFixed(2)
                const volat = (this.point.high - this.point.low).toFixed(2)

                s += `<br/>open:  ${this.point.open}`
                s += `<br/>high:  ${this.point.high}`
                s += `<br/>low:   ${this.point.low}`
                s += `<br/>close: ${this.point.close}`
                s += `<br/>perf: ${perf > 0 ? "+" : ""}${perf} (<b>${(perf / this.point.open * 100).toFixed(2)}%</b>)`
                s += `<br/>volat: ${volat} (<b>${(volat / this.point.open * 100).toFixed(2)}%</b>)`
                const wicksFractions = [(this.point.high - Math.max(this.point.open, this.point.close)) / (this.point.high - this.point.low),
                (Math.min(this.point.open, this.point.close) - this.point.low) / (this.point.high - this.point.low)]
                const body_fraction = 1 - wicksFractions[0] - wicksFractions[1]
                s += `<br/>anatomy: ${wicksFractions[0].toFixed(2)} <b>${(body_fraction).toFixed(2)}</b> ${wicksFractions[1].toFixed(2)} `

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

    window.chart = chart
    window.candles = candles

    setTimeout(() => {
        document.getElementById("all").addEventListener("click", function () {
            const newChecked = document.getElementById("all").checked
            document.getElementById("c").checked = newChecked
            document.getElementById("a").checked = newChecked
            document.getElementById("l").checked = newChecked
            document.getElementById("e").checked = newChecked
            document.getElementById("p").checked = newChecked
            document.getElementById("O").checked = newChecked
            document.getElementById("A").checked = newChecked
            document.getElementById("L").checked = newChecked
            document.getElementById("P").checked = newChecked
            document.getElementById("C").checked = newChecked
            updateChart()
        });

        document.getElementById("c").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("a").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("l").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("e").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("p").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("O").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("A").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("L").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("P").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
        document.getElementById("C").addEventListener("click", (e) => {
            if (!e.target.checked) {
                document.getElementById("all").checked = false
            }
            updateChart()
        });
    }, 1000)

    function updateChart() {
        const allChecked = document.getElementById("all").checked
        const cChecked = document.getElementById("c").checked
        const aChecked = document.getElementById("a").checked
        const lChecked = document.getElementById("l").checked
        const eChecked = document.getElementById("e").checked
        const pChecked = document.getElementById("p").checked
        const OChecked = document.getElementById("O").checked
        const AChecked = document.getElementById("A").checked
        const LChecked = document.getElementById("L").checked
        const PChecked = document.getElementById("P").checked
        const CChecked = document.getElementById("C").checked

        if (allChecked) {
            chart.series[0].setData(candles)
            return
        }

        chart.series[0].setData(candles.filter(c => {
            if (c.custom.label === "c" && cChecked) {
                return true
            } else if (c.custom.label === "a" && aChecked) {
                return true
            } else if (c.custom.label === "l" && lChecked) {
                return true
            } else if (c.custom.label === "e" && eChecked) {
                return true
            } else if (c.custom.label === "p" && pChecked) {
                return true
            } else if (c.custom.label === "O" && OChecked) {
                return true
            } else if (c.custom.label === "A" && AChecked) {
                return true
            } else if (c.custom.label === "L" && LChecked) {
                return true
            } else if (c.custom.label === "P" && PChecked) {
                return true
            } else if (c.custom.label === "C" && CChecked) {
                return true
            } else {
                return false
            }
        }))
    }
})();

function toHCCandle(binanceCandle, description, label, candleType) {
    const [open, high, low, close, _, date] = binanceCandle
    return {
        x: Number(new Date(date)),
        open,
        high,
        low,
        close,
        description,
        custom: {
            label,
            candleType
        },
        dataLabels: {
            enabled: true,
            style: {
                fontWeight: 'bold'
            },
            formatter() {
                return label
            },
            alignTo: 'plotEdges',
            verticalAlign: 'top',
            overflow: true,
            crop: false
        },
    }
}
