"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const source_1 = require("./source");
const sink_1 = require("./sink");
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
    if (input == null && output == null)
        throw new Error('An input, ouput or both must be provided.');
    if (output == null) {
        return {
            source: source_1.createSource({ url, name, topic: input })
        };
    }
    if (input == null) {
        return {
            sink: sink_1.createSink({ url, name, topic: output, concurrency: options.concurrency })
        };
    }
    return {
        source: source_1.createSource({ url, name, topic: input }),
        sink: sink_1.createSink({ url, name, topic: output, concurrency: options.concurrency })
    };
};
exports.default = memux;
