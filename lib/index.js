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
const isQuad = (quad) => {
    return typeof quad === 'object' &&
        typeof quad.subject === 'string' &&
        typeof quad.predicate === 'string' &&
        typeof quad.object === 'string';
};
const isAction = (action) => {
    return typeof action === 'object' &&
        (action.type === 'write' || action.type === 'delete') &&
        isQuad(action.quad);
};
const isProgress = (progress) => {
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
                    if (isAction(action)) {
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
        if (!isAction({ type, quad })) {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSwwQ0FBcUU7QUFDckUsdUNBQW1HO0FBR25HLE1BQU0sc0JBQXNCLEdBQUcsSUFBSSxDQUFDO0FBQ3BDLE1BQU0sY0FBYyxHQUFHLElBQUksR0FBRyxHQUFHLEdBQUcsRUFBRSxDQUFDO0FBRXZDLE1BQU0sR0FBRyxHQUFHLENBQUMsR0FBRyxNQUFNO0lBRXBCLE9BQU8sQ0FBQyxHQUFHLENBQUMsSUFBSSxJQUFJLEVBQUUsQ0FBQyxXQUFXLEVBQUUsRUFBRSxHQUFHLE1BQU0sQ0FBQyxDQUFDO0FBQ25ELENBQUMsQ0FBQztBQUVGLE1BQU0sT0FBTyxHQUFHLEtBQUs7SUFFbkIsT0FBTyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUNyQixPQUFPLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO0FBQ2xCLENBQUMsQ0FBQztBQUVGLE1BQU0sTUFBTSxHQUFHLElBQUk7SUFDakIsTUFBTSxDQUFDLE9BQU8sSUFBSSxLQUFLLFFBQVE7UUFDeEIsT0FBTyxJQUFJLENBQUMsT0FBTyxLQUFLLFFBQVE7UUFDaEMsT0FBTyxJQUFJLENBQUMsU0FBUyxLQUFLLFFBQVE7UUFDbEMsT0FBTyxJQUFJLENBQUMsTUFBTSxLQUFLLFFBQVEsQ0FBQztBQUN6QyxDQUFDLENBQUM7QUFFRixNQUFNLFFBQVEsR0FBRyxNQUFNO0lBQ3JCLE1BQU0sQ0FBQyxPQUFPLE1BQU0sS0FBSyxRQUFRO1FBQzFCLENBQUMsTUFBTSxDQUFDLElBQUksS0FBSyxPQUFPLElBQUksTUFBTSxDQUFDLElBQUksS0FBSyxRQUFRLENBQUM7UUFDckQsTUFBTSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQztBQUM3QixDQUFDLENBQUM7QUFFRixNQUFNLFVBQVUsR0FBRyxRQUFRO0lBQ3pCLE1BQU0sQ0FBQyxPQUFPLFFBQVEsS0FB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