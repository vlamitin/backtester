const colors = Highcharts.getOptions().colors
const data1 = [
    {
        name: 'prevDay',
        type: 'FLAT',
        times: 130,
        next: [
            {
                name: 'cme',
                type: 'FLAT',
                times: 100,
                next: [
                    {
                        name: 'asia',
                        type: 'FLAT',
                        times: 75,
                        next: [
                            {
                                name: 'london',
                                type: 'FLAT',
                                times: 55,
                                next: [
                                    {
                                        name: 'early',
                                        type: 'FLAT',
                                        times: 40,
                                        next: [
                                            {
                                                name: 'premarket',
                                                type: 'FLAT',
                                                times: 30,
                                                next: [
                                                    {
                                                        name: 'open',
                                                        type: 'FLAT',
                                                        times: 27,
                                                        next: [
                                                            {
                                                                name: 'nyam',
                                                                type: 'FLAT',
                                                                times: 20,
                                                                next: [
                                                                    {
                                                                        name: 'lunch',
                                                                        type: 'FLAT',
                                                                        times: 14,
                                                                        next: [
                                                                            {
                                                                                name: 'nypm',
                                                                                type: 'FLAT',
                                                                                times: 10,
                                                                                next: [
                                                                                    {
                                                                                        name: 'close',
                                                                                        type: 'FLAT',
                                                                                        times: 7,
                                                                                    },
                                                                                    {
                                                                                        name: 'close',
                                                                                        type: 'BULL',
                                                                                        times: 3
                                                                                    },
                                                                                ]
                                                                            },
                                                                            {
                                                                                name: 'nypm',
                                                                                type: 'BULL',
                                                                                times: 4
                                                                            },
                                                                        ]
                                                                    },
                                                                    {
                                                                        name: 'lunch',
                                                                        type: 'BULL',
                                                                        times: 6
                                                                    },
                                                                ]
                                                            },
                                                            {
                                                                name: 'nyam',
                                                                type: 'BULL',
                                                                times: 7
                                                            },
                                                        ]
                                                    },
                                                    {
                                                        name: 'open',
                                                        type: 'BULL',
                                                        times: 13
                                                    },
                                                ]
                                            },
                                            {
                                                name: 'premarket',
                                                type: 'BULL',
                                                times: 10
                                            },
                                        ]
                                    },
                                    {
                                        name: 'early',
                                        type: 'BULL',
                                        times: 15
                                    },
                                ]
                            },
                            {
                                name: 'london',
                                type: 'BULL',
                                times: 20
                            },
                        ]
                    },
                    {
                        name: 'asia',
                        type: 'BULL',
                        times: 25
                    },
                ]
            },
            {
                name: 'cme',
                type: 'BULL',
                times: 30
            },
        ]
    },
    {
        name: 'prevDay',
        type: 'BULL',
        times: 70
    },
]

const resultVariants = []

function handleVariants(variants, parent_id) {
    if (variants.length === 0) return

    variants.forEach((variant, i) => {
        const id = `${parent_id}_${i + 1}`
        const name = `${variant.name} ${variant.type}`
        const value = variant.times
        let color = colors[0]

        switch (variant.type) {
            default:
                break
            case 'FLAT':
                color = colors[0]
                break
            case 'BULL':
                color = colors[1]
                break
        }

        resultVariants.push({ color, id, name, value, parent: parent_id })

        if (variant.next) {
            handleVariants(variant.next, id)
        }
    })
}

handleVariants(data1, 'id_0')

console.log('points', JSON.stringify(resultVariants.slice(0, 10), null, 2))
console.log('resultVariants', resultVariants);

(async () => {
    Highcharts.chart('container', {
        series: [
            {
                name: 'times',
                type: 'treemap',
                layoutAlgorithm: 'squarified',
                allowDrillToNode: true,
                animationLimit: 1000,
                dataLabels: {
                    enabled: false
                },
                levels: [
                    {
                        level: 1,
                        dataLabels: {
                            enabled: true
                        },
                        borderWidth: 3,
                        levelIsConstant: false
                    },
                    {
                        level: 1,
                        dataLabels: {
                            style: {
                                fontSize: '14px'
                            }
                        }
                    }
                ],
                accessibility: {
                    exposeAsGroupOnly: true
                },
                data: resultVariants
            }
        ],
        title: {
            text: 'bla bla',
            align: 'left'
        }
    })
})()
