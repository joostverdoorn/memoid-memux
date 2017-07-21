"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const uuid = require("node-uuid");
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
exports.createSource = ({ url, name, topic }) => {
    const consumer = new no_kafka_1.GroupConsumer({ connectionString: url, groupId: name, recoveryOffset: no_kafka_1.EARLIEST_OFFSET });
    const observable = new rxjs_1.Observable((observer) => {
        const dataHandler = async (messageSet, nextTopic, nextPartition) => {
            return messageSet.map(parseMessage).forEach(({ message, offset }) => {
                if (index_1.isAction(message)) {
                    const progress = { topic: nextTopic, partition: nextPartition, offset };
                    observer.next({ action: message, progress });
                }
                else {
                    console.error(new Error(`Non-action encountered: ${message}`));
                }
            });
        };
        const strategies = [{
                subscriptions: [topic],
                metadata: {
                    id: `${name}-${uuid.v4()}`,
                    weight: 50
                },
                strategy: new no_kafka_1.ConsistentAssignmentStrategy(),
                handler: dataHandler
            }];
        consumer.init(strategies);
    });
    const commitOffset = async ({ action, progress }) => {
        await consumer.commitOffset(progress);
        return action;
    };
    return observable
        .map(commitOffset);
};
