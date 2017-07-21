"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const p_queue_1 = require("p-queue");
const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;
const log = (...things) => {
    console.log(new Date().toISOString(), ...things);
};
const onError = error => {
    console.error(error);
    process.exit(1);
};
exports.isQuad = (quad) => {
    return typeof quad === 'object' &&
        typeof quad.subject === 'string' &&
        typeof quad.predicate === 'string' &&
        typeof quad.object === 'string';
};
exports.isAction = (action) => {
    return typeof action === 'object' &&
        (action.type === 'write' || action.type === 'delete') &&
        exports.isQuad(action.quad);
};
exports.isProgress = (progress) => {
    return typeof progress === 'object' &&
        typeof progress.offset === 'number' &&
        typeof progress.partition === 'number' &&
        typeof progress.topic === 'string';
};
const DEFAULT_OPTIONS = {
    concurrency: 8
};
const memux = (config) => {
    const { url, name, input = null, output = null, options } = config;
    const { source, sink } = input ? createSource(url, name, input) : { source: undefined, sink: undefined };
    const send = output ? createSend(url, name, output, options.concurrency) : null;
    return { source, sink, send };
};
const createSource = (connectionString, groupId, topic) => {
    const sink = new rxjs_1.Subject();
    const source = new rxjs_1.Subject();
    const consumer = new no_kafka_1.SimpleConsumer({ connectionString, groupId, recoveryOffset: no_kafka_1.EARLIEST_OFFSET });
    const partition = 0;
    consumer.init().then(() => {
        sink.bufferTime(OFFSET_COMMIT_INTERVAL).subscribe(progress => {
            consumer.commitOffset(progress).catch(onError);
        }, onError);
        consumer.fetchOffset([{ topic, partition }]).then(([{ offset }]) => {
            consumer.subscribe(topic, partition, {
                offset,
                time: offset === no_kafka_1.LATEST_OFFSET ? no_kafka_1.EARLIEST_OFFSET : null
            }, (messageSet, nextTopic, nextPartition) => {
                return messageSet.forEach(({ offset: nextOffset, message: { value } }) => {
                    const data = value.toString();
                    const progress = { topic: nextTopic, partition: nextPartition, offset: nextOffset };
                    let action;
                    try {
                        action = JSON.parse(data);
                    }
                    catch (error) {
                        if (!(error instanceof SyntaxError)) {
                            throw error;
                        }
                    }
                    log('RECV', data);
                    if (exports.isAction(action)) {
                        source.next({ action, progress });
                    }
                });
            });
        });
    });
    return { source, sink };
};
const createSend = (connectionString, label, topic, concurrency) => {
    const producer = new no_kafka_1.Producer({ connectionString });
    const ready = producer.init().catch(onError);
    const queue = new p_queue_1.default({
        concurrency
    });
    return ({ type, quad }) => {
        if (!exports.isAction({ type, quad })) {
            return onError(new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad })));
        }
        const value = JSON.stringify({ type, quad: Object.assign({ label }, quad) });
        return queue.add(() => ready.then(() => {
            return producer.send({ topic, message: { value } });
        }).then(() => {
            return log('SEND', value);
        }));
    };
};
exports.default = memux;
