"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const PQueue = require("p-queue");
const index_1 = require("./index");
const log = console.log.bind(console);
exports.createSink = ({ url, name, topic, concurrency = 8 }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSink should be called with a config containing a url, name and topic.');
    }
    const subject = new rxjs_1.Subject();
    const producer = new no_kafka_1.Producer({ connectionString: url });
    const queue = new PQueue({
        concurrency
    });
    queue.add(async () => {
        await producer.init();
    });
    const send = ({ type, quad }) => {
        if (!index_1.isAction({ type, quad })) {
            throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
        }
        const value = JSON.stringify({ type, quad: Object.assign({ label: name }, quad) });
        return queue.add(async () => {
            await producer.send({ topic, message: { value } });
            return log('SEND', value);
        });
    };
    subject.subscribe({
        next: send
    });
    return subject;
};
