const symbol = 'BTCUSDT';

(async () => {
    const profiles = await fetch(
        `http://localhost:8000/api/profiles/${symbol}`
    ).then(response => response.json())

//     const new_row = $('<tr class=\'search-header\'/>')
//     $('#table thead th').each(function (i) {
//         const title = $(this).text()
//         const new_th = $('<th style="' + $(this).attr('style') + '" />')
//         $(new_th).append('<input type="text" placeholder="' + title + '" data-index="' + i + '"/>')
//         $(new_row).append(new_th)
//     })
//     $('#table thead').prepend(new_row)

    const columns = ['chance%', 'num', 'denom'].concat(...Object.keys(profiles))
    const dataSet = []
    Object.keys(profiles).forEach(sessionName => {
        Object.keys(profiles[sessionName]).forEach(candleType => {
            profiles[sessionName][candleType].forEach(profile => {
                const session_keys = profile[0]
                const chance = profile[1]

                dataSet.push(columns.map(columnName => {
                    if (columnName === 'chance%') {
                        return Number(chance[2].replace('%', ''))
                    }
                    if (columnName === 'num') {
                        return chance[0]
                    }
                    if (columnName === 'denom') {
                        return chance[1]
                    }
                    if (columnName === sessionName) {
                        return `${sessionName}__${candleType}`
                    }

                    const session_key = session_keys.find(key => key && key.startsWith(columnName + '__'))
                    return session_key || '-'
                }))
            })
        })
    })

    const table = new DataTable('#table', {
        columns: columns.map(name => {
            if (name === 'chance%') {
                return { title: name, type: 'num' }
            }
            if (name === 'num') {
                return { title: name, type: 'num' }
            }
            if (name === 'denom') {
                return { title: name, type: 'num' }
            }
            return { title: name, type: 'html' }
        }),
        data: dataSet,
        paging: false,
        scrollX: false,
        scrollY: false,
        searching: true,
    })

    table.search.fixed('range', function (searchStr, data, index) {
        const value = document.querySelector('#search').value

        if (!value) {
            return true
        }

        try {
            return evaluateQuery(value, data, columns)
        } catch (e) {
            console.error(e)
            return false
        }
    })

    columns.forEach(column => {
        const button = document.createElement("button")
        button.textContent = column
        button.addEventListener("click", () => {
            document.querySelector('#search').value += column
            document.querySelector('#search').focus()
        })
        document.getElementById("btn-container").appendChild(button)
    });

    document.querySelector('#search').addEventListener('keyup', (event) => {
        if (event.keyCode === 13) {
            try {
                const value = document.querySelector('#search').value
                if (value) {
                    evaluateQuery(value, table.row(1).data(), columns)
                }
                $("#dt-search-0").val("").trigger("change");
                table.draw()
            } catch (e) {
                console.error(e)
                alert(e)
            }
        }
    })
})()

function parseCondition(condition) {
    const operators = ['>=', '<=', '!=', '=', '>', '<']
    for (let op of operators) {
        let parts = condition.split(op)
        if (parts.length === 2) {
            let key = parts[0].trim()
            let value = parts[1].trim()
            return { key, op, value: isNaN(value) ? value.replace(/['"]+/g, '') : Number(value) }
        }
    }
    throw new Error(`Invalid condition: ${condition}, valid operators: ${operators.join(',')}`)
}

function evaluateCondition(data, columns, { key, op, value }) {
    const keyIndex = columns.findIndex(col => col === key)
    if (keyIndex === -1) {
        return false
    }
    let dataValue = data[keyIndex]
    switch (op) {
        case '>':
            return dataValue > value
        case '<':
            return dataValue < value
        case '>=':
            return dataValue >= value
        case '<=':
            return dataValue <= value
        case '!=':
            return dataValue !== value
        case '=':
            return dataValue === value
        default:
            throw new Error(`Unknown operator: ${op}`)
    }
}

function evaluateQuery(query, data, columns) {
    let conditions = query.split(/\s+and\s+/i).map(parseCondition)
    return conditions.every(cond => evaluateCondition(data, columns, cond))
}
