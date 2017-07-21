"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const p_queue_1 = require("p-queue");
const index_1 = require("./index");
const log = console.log.bind(console);
const createSink = (connectionString, label, topic, concurrency) => {
    const subject = new rxjs_1.Subject();
    const producer = new no_kafka_1.Producer({ connectionString });
    const queue = new p_queue_1.default({
        concurrency
    });
    queue.add(async () => {
        await producer.init();
    });
    const send = ({ type, quad }) => {
        if (!index_1.isAction({ type, quad })) {
            throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
        }
        const value = JSON.stringify({ type, quad: Object.assign({ label }, quad) });
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
