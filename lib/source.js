"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const no_kafka_1 = require("no-kafka");
const uuid = require("node-uuid");
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
exports.createSource = ({ url, name, topic }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('createSource should be called with a config containing a url, name and topic.');
    }
    const consumer = new no_kafka_1.GroupConsumer({
        connectionString: url,
        groupId: name,
        startingOffset: no_kafka_1.EARLIEST_OFFSET,
        recoveryOffset: no_kafka_1.EARLIEST_OFFSET,
        logger: {
            logFunction: Logger.log
        }
    });
    const outerObservable = new rxjs_1.Observable((outerObserver) => {
        const dataHandler = async (messageSet, topic, partition) => {
            const innerObservable = new rxjs_1.Observable((observer) => {
                let progress;
                const messagesSent = Promise.all(messageSet.map(parseMessage).map(({ message, offset }) => {
                    if (index_1.isAction(message)) {
                        observer.next({ action: message });
                        progress = { topic, partition, offset };
                    }
                    else {
                        console.error(new Error(`Non-action encountered: ${message}`));
                    }
                }));
                const teardown = async () => {
                    await messagesSent;
                    return consumer.commitOffset(progress);
                };
                return teardown;
            });
            return outerObserver.next(innerObservable);
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
        consumer.init(strategies).catch(outerObserver.error);
    });
    return outerObservable.flatMap(rxjs_1.Observable.merge);
};
