"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const PQueue = require("p-queue");
const index_1 = require("./index");
const Logger = require("./logger");
class KafkaSubject extends rxjs_1.Subject {
    constructor(url, name, topic) {
        super();
        this._send = async ({ type, quad }) => {
            if (!index_1.isAction({ type, quad })) {
                throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
            }
            const value = JSON.stringify({ type, quad: Object.assign({ label: this._name }, quad) });
            return this._queue.add(async () => {
                const result = await this._producer.send({ topic: this._topic, message: { value } });
                return Logger.log('SEND', value, result);
            });
        };
        this._name = name;
        this._topic = topic;
        this._producer = new no_kafka_1.Producer({
            connectionString: url,
            logger: {
                logFunction: Logger.log
            }
        });
        const queue = new PQueue({
            concurrency: 8
        });
        queue.add(async () => {
            await this._producer.init();
        });
        this._queue = queue;
    }
    next(value) {
        return this._send(value)
            .then(() => super.next(value), (err) => this.error(err));
    }
}
exports.KafkaSubject = KafkaSubject;
exports.createSink = ({ url, name, topic, concurrency = 8 }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSink should be called with a config containing a url, name and topic.');
    }
    return new KafkaSubject(url, name, topic);
};
