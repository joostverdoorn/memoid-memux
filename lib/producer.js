"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const no_kafka_1 = require("no-kafka");
const PQueue = require("p-queue");
const index_1 = require("./index");
const Logger = require("./logger");
exports.createSend = async ({ url, name, topic, concurrency = 8 }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSend should be called with a config containing a url, name and topic.');
    }
    const producer = new no_kafka_1.Producer({
        connectionString: url,
        logger: {
            logFunction: Logger.log
        }
    });
    const queue = new PQueue({
        concurrency
    });
    const send = ({ type, quad }) => {
        if (!index_1.isAction({ type, quad })) {
            throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
        }
        const value = JSON.stringify({ type, quad: Object.assign({ label: name }, quad) });
        return queue.add(async () => {
            await producer.send({ topic, message: { value } });
            return Logger.log('SEND', value);
        });
    };
    return producer.init().then(() => send);
};
