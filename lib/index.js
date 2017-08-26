"use strict";
function __export(m) {
    for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
const consumer_1 = require("./consumer");
const producer_1 = require("./producer");
__export(require("./consumer"));
__export(require("./producer"));
exports.isOperation = (operation) => {
    return operation != null &&
        typeof operation === 'object' &&
        (operation.action === 'write' || operation.action === 'delete') &&
        typeof operation.key === 'string';
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
async function memux({ name, url, input, output, receive, concurrency, ssl }) {
    if (input != null && receive != null)
        await consumer_1.createReceive({
            name,
            url,
            topic: input,
            receive,
            concurrency: concurrency,
            ssl
        });
    if (output != null)
        return producer_1.createSend({
            name,
            url,
            topic: output,
            concurrency: concurrency,
            ssl
        });
}
exports.default = memux;
