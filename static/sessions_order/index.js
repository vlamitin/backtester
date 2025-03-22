class TreeNode {
    constructor(key, parent, count) {
        this.key = key
        this.count = count
        this.parent = parent
        this.children = []
    }
}

class Tree {
    constructor(key, count) {
        this.root = new TreeNode(key, null, count)
    }

    *traverseChildren(node) {
        yield node
        if (node.children.length) {
            for (let child of node.children) {
                yield* this.traverseChildren(child)
            }
        }
    }

    *traverseParents(node) {
        yield node
        if (node.parent) {
            yield* this.traverseParents(node.parent)
        }
    }

    insert(parentNodeKey, key) {
        for (let parentNode of this.traverseChildren(this.root)) {
            if (parentNode.key !== parentNodeKey) {
                continue
            }

            const sameKeyNode = parentNode.children.find(ch => ch.key === key)
            if (sameKeyNode) {
                sameKeyNode.count++
            } else {
                parentNode.children.push(new TreeNode(key, parentNode, 1))
            }

            for (let node of this.traverseParents(parentNode)) {
                node.count++
            }

            return true
        }
        return false
    }

    find(key) {
        for (let node of this.traverseChildren(this.root)) {
            if (node.key === key) return node
        }
        return null
    }
}

const symbol = 'BTCUSDT'
const colors = Highcharts.getOptions().colors

const colorsMap = {
    'COMPRESSION': colors[0],
    'DOJI': colors[1],
    'INDECISION': colors[3],
    'BULL': colors[2],
    'TO_THE_MOON': colors[6],
    'STB': colors[5],
    'REJECTION_BULL': colors[4],
    'HAMMER': colors[7],
    'BEAR': colors[8],
    'FLASH_CRASH': colors[9],
    'BTS': colors[10],
    'REJECTION_BEAR': colors[11],
    'BEAR_HAMMER': colors[12],
    'V_SHAPE': colors[13],
    'PUMP_AND_DUMP': colors[14],
}

console.log('colorsMap', colorsMap)

function treeToVariants(tree) {
    if (tree.root.count === 0) {
        return []
    }

    const result = []

    for (let node of tree.traverseChildren(tree.root)) {
        if (node.key === tree.root.key) {
            continue
        }

        const [session, candleType] = node.key.split('__')
        const id = node.key
        const name = `${session} ${candleType}`
        const value = node.count
        let color = colorsMap[candleType]

        result.push({ color, id, name, value, parent: node.parent.key })
    }

    return result

}

function fillTree(sessions) {
    const tree = new Tree('total', 0)

    let dayGroups = []
    sessions.forEach(session => {
        if (dayGroups.length === 0 || dayGroups[dayGroups.length - 1][0].day_date !== session.day_date) {
            dayGroups.push([session])
            return
        }

        dayGroups[dayGroups.length - 1].push(session)
    })

    dayGroups.forEach(dayGroup => {
        if (dayGroup.length !== 10) {
            return
        }


        const [cme, asia, london, early, pre, open, nyam, lunch, nypm, close] = dayGroup
        if (!['BULL', 'TO_THE_MOON', 'BEAR', 'FLASH_CRASH'].includes(close.type)) {
            return
        }

        tree.insert('total', `${close.name}__${close.type}`)
        tree.insert(`${close.name}__${close.type}`, `${nypm.name}__${nypm.type}`)
        tree.insert(`${nypm.name}__${nypm.type}`, `${lunch.name}__${lunch.type}`)
        tree.insert(`${lunch.name}__${lunch.type}`, `${nyam.name}__${nyam.type}`)
        tree.insert(`${nyam.name}__${nyam.type}`, `${open.name}__${open.type}`)
        tree.insert(`${open.name}__${open.type}`, `${pre.name}__${pre.type}`)

        // tree.insert('total', `${asia.name}__${asia.type}`)
        // tree.insert(`${asia.name}__${asia.type}`, `${london.name}__${london.type}`)
        // tree.insert(`${london.name}__${london.type}`, `${pre.name}__${pre.type}`)
        // tree.insert(`${pre.name}__${pre.type}`, `${open.name}__${open.type}`)
        // tree.insert(`${open.name}__${open.type}`, `${nyam.name}__${nyam.type}`)

        // tree.insert('total', `${cme.name}__${cme.type}`)
        // tree.insert(`${cme.name}__${cme.type}`, `${asia.name}__${asia.type}`)
        // tree.insert(`${asia.name}__${asia.type}`, `${london.name}__${london.type}`)
        // tree.insert(`${london.name}__${london.type}`, `${early.name}__${early.type}`)
        // tree.insert(`${early.name}__${early.type}`, `${pre.name}__${pre.type}`)
        // tree.insert(`${pre.name}__${pre.type}`, `${open.name}__${open.type}`)
        // tree.insert(`${open.name}__${open.type}`, `${nyam.name}__${nyam.type}`)
        // tree.insert(`${nyam.name}__${nyam.type}`, `${lunch.name}__${lunch.type}`)
        // tree.insert(`${lunch.name}__${lunch.type}`, `${nypm.name}__${nypm.type}`)
        // tree.insert(`${nypm.name}__${nypm.type}`, `${close.name}__${close.type}`)
    })

    return tree
}

(async () => {
    const sessions = await fetch(
        `http://localhost:8000/api/sessions/${symbol}`
    ).then(response => response.json());

    const tree = fillTree(sessions)
    // window.tree = tree

    const variants = treeToVariants(tree)

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
                data: variants
            }
        ],
        title: {
            text: 'bla bla',
            align: 'left'
        }
    })
})()
