"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const no_kafka_1 = require("no-kafka");
const PQueue = require("p-queue");
const index_1 = require("./index");
const Logger = require("./logger");
exports.createSend = async ({ url, name, topic, concurrency = 8, ssl = {} }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSend should be called with a config containing a url, name and topic.');
    }
    const producer = new no_kafka_1.Producer({
        connectionString: url,
        ssl,
        logger: {
            logFunction: Logger.log
        }
    });
    console.log('Queueing send with concurrency:', concurrency);
    const queue = new PQueue({
        concurrency
    });
    const send = ({ action, key, data }) => {
        if (!index_1.isOperation({ action, key, data })) {
            throw new Error('Trying to send a non-action: ' + JSON.stringify({ action, key, data }));
        }
        const value = JSON.stringify({ label: name, action, key, data });
        return queue.add(async () => {
            await producer.send({ topic, message: { key, value } });
            return Logger.log('SEND', key, value);
        });
    };
    return producer.init().then(() => send);
};
