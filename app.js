const express = require('express');
const bodyParser = require('body-parser');
const addRequestId = require('express-request-id')({ setHeader: false });
const morgan = require('morgan');

const db = require('./db');

const dbFilename = process.env.HUMIDITY_DB;

db.open(dbFilename);

const port = process.env.HTTP_PORT || 3001;

const defaultIntervalInSeconds = 2 * 60;

const app = express();

require('express-ws')(app);

app.use(addRequestId);

morgan.token('id', (req) => req.id.split('-')[0]);

app.use(morgan('[:date[iso] #:id] Started :method :url for :remote-addr', { immediate: true }));
app.use(morgan('[:date[iso] #:id] Completed :status :res[content-length] in :response-time ms'));

app.use(bodyParser.json());

class WebSocketWrapper {
    constructor(ws, lastId) {
        this.ws = ws;
        this.lastId = lastId;
    }

    send(m) {
        if (this.lastId < m.id) {
            try {
                this.ws.send(JSON.stringify(m), (error) => {
                    if (error) {
                        console.log('Error sending websocket message. Closing socket.');
                        this.ws.close();
                    } else {
                        this.lastId = m.id;
                    }
                });
            } catch (e) {
                console.log(`Caught: ${e}. Closing socket.`);
                this.ws.close();
            }
        }
    }
}

function toNumber(str, def) {
    const n = Number.parseInt(str, 10);
    return Number.isNaN(n) ? def : n;
}

class WebSocketHandler {
    constructor(url, fetch) {
        this.cacheList = [];
        this.fetch = fetch;

        app.ws(url, (ws, req) => {
            const timestamp = toNumber(req.query.timestamp, undefined);
            const lastId = toNumber(req.query.id, -1);

            const cws = new WebSocketWrapper(ws, lastId);
            this.cacheList.push(cws);

            ws.on('close', () => {
                this.cacheList = this.cacheList.filter((e) => e !== cws);
            });

            this.doFetch({ timestamp, lastId }).forEach((m) => {
                cws.send(m);
            });
        });
    }

    doFetch({ timestamp, lastId }) {
        if (timestamp || (lastId > -1)) {
            return this.fetch({ timestamp, lastId });
        }

        const defaultTimestamp = Math.floor(Date.now() / 1000) - defaultIntervalInSeconds;

        return this.fetch({ timestamp: defaultTimestamp });
    }

    update() {
        if (this.cacheList.length > 0) {
            const lastId = Math.min(...this.cacheList.map((mws) => mws.lastId));

            this.doFetch({ lastId }).forEach((m) => {
                this.cacheList.forEach((cws) => cws.send(m));
            });
        }
    }
}

const wsUpdaters = [
    new WebSocketHandler('/ws/measurements', db.getMeasurements),
    new WebSocketHandler('/ws/debug', db.getDebugMessages),
];

app.get('/api/measurements', (req, res) => {
    const timestamp = toNumber(req.query.timestamp, undefined);
    const lastId = toNumber(req.query.id, -1);

    res.set('Access-Control-Allow-Origin', '*');

    if (timestamp || (lastId > -1)) {
        res.send(db.getMeasurements({ timestamp, lastId }));
    } else {
        const defaultTimestamp = Math.floor(Date.now() / 1000) - defaultIntervalInSeconds;
        res.send(db.getMeasurements({ timestamp: defaultTimestamp }));
    }
});

app.get('/api/measurements/average', (req, res) => {
    const start = toNumber(req.query.start);
    const end = toNumber(req.query.end);
    const period = toNumber(req.query.period);

    const numSteps = (end - start) / period;
    const maxSteps = 20000;

    if (!start || !end || !period) {
        res.status(400).send({ error: 'The query parameters \'start\', \'end\' and \'period\' are required' });
    } else if (numSteps > maxSteps) {
        res.status(400).send({ error: `${numSteps} requested but maximally ${maxSteps} is allowed` });
    } else {
        res.set('Access-Control-Allow-Origin', '*');
        res.send(db.getAverageMeasurements({ start, end, period }));
    }
});

app.get('/api/measurements/raw', (req, res) => {
    const start = toNumber(req.query.start);
    const end = toNumber(req.query.end);

    const maxInterval = 3 * 24 * 60 * 60;

    if (!start || !end) {
        res.status(400).send({ error: 'The query parameters \'start\' and \'end\' are required' });
    } else if (end - start > maxInterval) {
        res.status(400).send({ error: `Max allowed interval is ${maxInterval} seconds` });
    } else {
        res.set('Access-Control-Allow-Origin', '*');
        res.send(db.getRawMeasurements({ start, end }));
    }
});

app.get('/api/measurements/newest', (req, res) => {
    res.set('Access-Control-Allow-Origin', '*');

    res.send(db.getNewestMeasurements());
});

app.get('/api/measurements/bulk', (req, res) => {
    const totalPeriod = toNumber(req.query.period);

    if (!totalPeriod) {
        res.status(400).send({ error: 'The query parameter \'period\' is required' });
    } else {
        const end = Math.floor(Date.now() / 1000);
        const start = end - totalPeriod;

        res.set('Access-Control-Allow-Origin', '*');
        if (totalPeriod < 7 * 24 * 60 * 60) {
            res.send(db.getRawMeasurements({ start, end }));
        } else if (totalPeriod < 21 * 24 * 60 * 60) {
            res.send(db.getAverageMeasurements({ start, end, period: 60 * 60 }));
        } else {
            res.send(db.getAverageMeasurements({ start, end, period: 4 * 60 * 60 }));
        }
    }
});

app.get('/api/nodes', (req, res) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.send(db.getNodes());
});

app.get('/api/node/:nodeId', (req, res) => {
    const { nodeId } = req.params;
    const node = db.getNode(nodeId);

    if (!node) {
        res.status(400).send({ error: 'No node id' });
    } else {
        res.set('Access-Control-Allow-Origin', '*');

        if (node) {
            res.send(node);
        } else {
            res.status(404);
            res.send({
                status: 404,
                message: 'Node not found',
            });
        }
    }
});

app.put('/api/node/:nodeId', (req, res) => {
    const { nodeId, name } = req.body;

    res.set('Access-Control-Allow-Origin', '*');

    if (nodeId && name) {
        if (db.getNode(nodeId)) {
            console.log(`Setting name of ${nodeId} to '${name}'`);
            db.updateNodeName(nodeId, name);
            res.status(204).end();
        } else {
            res.status(404).send({
                error: 'Node not found',
            });
        }
    } else {
        res.status(400).send({
            error: 'Node or name missing',
        });
    }
});

app.get('/', (req, res) => res.redirect('/index.html'));

app.use(express.static('public'));

app.listen(port, () => {
    console.log(`Listening on port ${port}`);

    setInterval(() => {
        wsUpdaters.forEach((u) => u.update());
    }, 500);
});
