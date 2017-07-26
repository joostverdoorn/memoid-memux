"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const consumer_1 = require("./consumer");
const producer_1 = require("./producer");
const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;
exports.isQuad = (quad) => {
    return quad != null &&
        typeof quad === 'object' &&
        typeof quad.subject === 'string' &&
        typeof quad.predicate === 'string' &&
        typeof quad.object === 'string';
};
exports.isAction = (action) => {
    return action != null &&
        typeof action === 'object' &&
        (action.type === 'write' || action.type === 'delete') &&
        exports.isQuad(action.quad);
};
exports.isProgress = (progress) => {
    return progress != null &&
        typeof progress === 'object' &&
        typeof progress.offset === 'number' &&
        typeof progress.partition === 'number' &&
        typeof progress.topic === 'string';
};
const DEFAULT_OPTIONS = {
    concurrency: 8
};
const memux = (config) => {
    const { url, name, input = null, output = null, options = DEFAULT_OPTIONS } = config;
    if (input == null && output == null) {
        throw new Error('An input, ouput or both must be provided.');
    }
    if (output == null) {
        return {
            consumer: consumer_1.Consumer({ url, name, topic: input })
        };
    }
    if (input == null) {
        return {
            producer: producer_1.Producer({ url, name, topic: output, concurrency: options.concurrency })
        };
    }
    return {
        consumer: consumer_1.Consumer({ url, name, topic: input }),
        producer: producer_1.Producer({ url, name, topic: output, concurrency: options.concurrency })
    };
};
exports.default = memux;
