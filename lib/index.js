"use strict";
function __export(m) {
    for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
__export(require("./consumer"));
__export(require("./producer"));
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
