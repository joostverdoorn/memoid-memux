"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;
const log = (...things) => {
    console.log(new Date().toISOString(), ...things);
};
const onError = error => {
    console.error(error);
    process.exit(1);
};
const isQuad = quad => {
    return typeof quad === 'object' &&
        typeof quad.subject === 'string' &&
        typeof quad.predicate === 'string' &&
        typeof quad.object === 'string';
};
const isAction = action => {
    return typeof action === 'object' &&
        (action.type === 'write' || action.type === 'delete') &&
        isQuad(action.quad);
};
const isProgress = progress => {
    return typeof progress === 'object' &&
        typeof progress.offset === 'number' &&
        typeof progress.partition === 'number' &&
        typeof progress.topic === 'string';
};
const memux = ({ url, name, input = null, output = null }) => {
    const { source, sink } = input ? createSource(url, name, input) : { source: undefined, sink: undefined };
    const send = output ? createSend(url, name, output) : null;
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
                    if (isAction(action)) {
                        source.next({ action, progress });
                    }
                });
            });
        });
    });
    return { source, sink };
};
const createSend = (connectionString, label, topic) => {
    const producer = new no_kafka_1.Producer({ connectionString });
    const ready = producer.init().catch(onError);
    return ({ type, quad }) => {
        if (!isAction({ type, quad })) {
            return onError(new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad })));
        }
        const value = JSON.stringify({ type, quad: Object.assign({ label }, quad) });
        return ready.then(() => {
            return producer.send({ topic, message: { value } });
        }).then(() => {
            return log('SEND', value);
        });
    };
};
exports.default = memux;
