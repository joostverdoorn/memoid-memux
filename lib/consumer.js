"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const no_kafka_1 = require("no-kafka");
const uuid = require("node-uuid");
const PQueue = require("p-queue");
const index_1 = require("./index");
const Logger = require("./logger");
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
exports.createReceive = async ({ url, name, topic, receive, ssl = {}, concurrency = 8 }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSource should be called with a config containing a url, name, topic and receiveFn.');
    }
    const consumer = new no_kafka_1.GroupConsumer({
        connectionString: url,
        ssl,
        groupId: name,
        startingOffset: no_kafka_1.EARLIEST_OFFSET,
        recoveryOffset: no_kafka_1.EARLIEST_OFFSET,
        heartbeatTimeout: 5000,
        logger: {
            logFunction: Logger.log
        }
    });
    console.log('Queueing receive with concurrency:', concurrency);
    const queue = new PQueue({
        concurrency
    });
    const dataHandler = async (messageSet, topic, partition) => {
        return await Promise.all(messageSet.map(parseMessage).map(({ message, offset }) => {
            if (!index_1.isOperation(message))
                throw new Error(`Non-action encountered: ${message}`);
            const progress = { topic, partition, offset };
            return queue.add(async () => {
                await receive(message);
                return consumer.commitOffset(progress);
            });
        }));
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
    return consumer.init(strategies);
};
