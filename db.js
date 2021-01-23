const betterSqlite3 = require('better-sqlite3');

let db;

const sql = {
    nodes: 'SELECT '
        + 'id, '
        + 'name '
        + 'FROM nodes',

    node: ''
        + 'SELECT '
        + 'n.id AS id, '
        + 'n.name AS name, '
        + 't.description AS type, '
        + 'v.hash AS firmwareVersion, '
        + 'm.timestamp, '
        + 'm.id AS measurementId, '
        + 'ROUND(m.temperature,2) AS temperature, '
        + 'ROUND(m.humidity,2) AS humidity, '
        + 'ROUND(m.battery1_level,3) AS battery1_level, '
        + 'ROUND(m.battery2_level,3) AS battery2_level '
        + 'FROM nodes AS n '
        + 'INNER JOIN node_types AS t ON n.type_id = t.id '
        + 'LEFT JOIN firmware_versions AS v ON n.firmware_id = v.id '
        + 'INNER JOIN measurements AS m ON m.id = (SELECT m.id FROM measurements AS m WHERE m.node_id=(@nodeId) ORDER BY m.id DESC LIMIT 1) '
        + 'WHERE n.id = (@nodeId)',

    measurements: ''
        + 'SELECT '
        + 'm.id, '
        + 'n.name AS name, '
        + 'node_id AS nodeId, '
        + 'timestamp, '
        + 'ROUND(temperature,2) AS temperature, '
        + 'ROUND(humidity,2) AS humidity, '
        + 'ROUND(battery1_level,3) AS battery1_level, '
        + 'ROUND(battery2_level,3) AS battery2_level '
        + 'FROM measurements AS m '
        + 'INNER JOIN nodes AS n ON n.id = m.node_id '
        + 'WHERE timestamp > (@timestamp) AND m.id > (@id) '
        + 'ORDER BY m.id',

    averageMeasurements: ''
        + 'SELECT '
        + 'node_id AS nodeId, '
        + '(@period * CAST(timestamp/@period AS INT)) AS timestamp, '
        + 'ROUND(AVG(temperature),2) AS temperature, '
        + 'ROUND(AVG(humidity),2) AS humidity, '
        + 'ROUND(AVG(battery1_level),3) AS battery1_level, '
        + 'ROUND(AVG(battery2_level),3) AS battery2_level '
        + 'FROM measurements AS m '
        + 'WHERE @start <= m.timestamp AND m.timestamp < @end '
        + 'GROUP BY (@period * CAST(timestamp/@period AS INT)), node_id '
        + 'ORDER BY (@period * CAST(timestamp/@period AS INT))',

    rawMeasurements: ''
        + 'SELECT '
        + 'node_id AS nodeId, '
        + 'timestamp, '
        + 'ROUND(temperature,2) AS temperature, '
        + 'ROUND(humidity,2) AS humidity, '
        + 'ROUND(battery1_level,3) AS battery1_level, '
        + 'ROUND(battery2_level,3) AS battery2_level '
        + 'FROM measurements AS m '
        + 'WHERE @start <= m.timestamp AND m.timestamp < @end '
        + 'ORDER BY m.timestamp',

    newestMeasurements: ''
        + 'SELECT '
        + 'node_id AS nodeId, '
        + 'n.name AS name, '
        + 'MAX(timestamp) AS timestamp, '
        + 'ROUND(temperature,2) AS temperature, '
        + 'ROUND(humidity,2) AS humidity, '
        + 'ROUND(battery1_level,3) AS battery1_level, '
        + 'ROUND(battery2_level,3) AS battery2_level '
        + 'FROM measurements AS m '
        + 'INNER JOIN nodes AS n ON n.id = m.node_id '
        + 'GROUP BY node_id ',

    debugMessages: ''
        + 'SELECT '
        + 'm.id AS id, '
        + 'n.id AS nodeId, '
        + 'n.name AS name, '
        + 'm.timestamp AS timestamp, '
        + 'm.message AS message '
        + 'FROM debug_messages AS m '
        + 'INNER JOIN nodes AS n ON m.node_id = n.id '
        + 'WHERE m.timestamp > (@timestamp) AND m.id > (@id) '
        + 'ORDER BY m.timestamp DESC '
        + 'LIMIT (@limit)',

    updateNodeName: ''
        + 'UPDATE nodes '
        + 'SET name = (@name) '
        + 'WHERE id = (@nodeId)',
};

const stmt = {};

function open(filename) {
    console.log(`Opening database ${filename}`);
    db = betterSqlite3(filename);
    Object.keys(sql).forEach((key) => { stmt[key] = db.prepare(sql[key]); });

    Object.assign(module.exports, {
        getNodes: () => stmt.nodes.all(),

        getNode: (nodeId) => stmt.node.get({ nodeId }),

        getMeasurements: ({ timestamp, lastId }) => stmt.measurements.all({
            timestamp: timestamp || -1,
            id: lastId || -1,
        }),

        getRawMeasurements: ({ start, end }) => stmt.rawMeasurements.all(
            { start, end },
        ),

        getAverageMeasurements: ({ period, start, end }) => stmt.averageMeasurements.all(
            { period, start, end },
        ),

        getNewestMeasurements: () => stmt.newestMeasurements.all(),

        getDebugMessages: ({ timestamp, limit, lastId }) => stmt.debugMessages.all({
            timestamp: timestamp || -1,
            limit: limit || -1,
            id: lastId || -1,
        }),

        updateNodeName: (nodeId, name) => stmt.updateNodeName.run({ nodeId, name }),
    });
}

module.exports = {
    open,
};
