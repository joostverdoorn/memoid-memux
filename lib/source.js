"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const index_1 = require("./index");
const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;
const parseMessage = ({ offset, message: { value } }) => {
    const data = value.toString();
    let message;
    try {
        message = JSON.parse(data);
    }
    catch (error) {
        if (!(error instanceof SyntaxError)) {
            throw error;
        }
    }
    return { message, offset };
};
const createSource = ({ connectionString, groupId, topic, partition }) => {
    const consumer = new no_kafka_1.SimpleConsumer({ connectionString, groupId, recoveryOffset: no_kafka_1.EARLIEST_OFFSET });
    return new rxjs_1.Observable((observer) => {
        (async () => {
            await consumer.init();
            const [{ offset }] = await consumer.fetchOffset([{ topic, partition }]);
            consumer.subscribe(topic, partition, {
                offset,
                time: offset === no_kafka_1.LATEST_OFFSET ? no_kafka_1.EARLIEST_OFFSET : null
            }, async (messageSet, nextTopic, nextPartition) => {
                return messageSet.map(parseMessage).forEach(({ message, offset }) => {
                    if (index_1.isAction(message)) {
                        const progress = { topic: nextTopic, partition: nextPartition, offset };
                        observer.next({ action: message, progress });
                    }
                    else {
                        console.error(new Error(`Non-action encountered: ${message}`));
                    }
                });
            });
        })();
    });
};
