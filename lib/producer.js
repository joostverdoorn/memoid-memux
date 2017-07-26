"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const rxjs_1 = require("@reactivex/rxjs");
const Kafka = require("no-kafka");
const PQueue = require("p-queue");
const index_1 = require("./index");
const Logger = require("./logger");
exports.Producer = ({ url, name, topic, concurrency = 8 }) => {
    if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
        throw new Error('Producer should be called with a config containing a url, name and topic.');
    }
    const sink = new rxjs_1.Subject();
    const producer = new Kafka.Producer({
        connectionString: url,
        logger: {
            logFunction: Logger.log
        }
    });
    const queue = new PQueue({
        concurrency
    });
    const source = new rxjs_1.Subject();
    const send = (value) => {
        console.log(`Sending action on ${topic}.`);
        if (!index_1.isAction(value.action)) {
            throw new Error('Trying to send a non-action: ' + JSON.stringify(value.action));
        }
        const action = { type: value.action.type, quad: Object.assign({ label: name }, value.action.quad) };
        const str = JSON.stringify(action);
        return queue.add(async () => {
            const [progress] = await producer.send({ topic, message: { value: str } });
            await Logger.log('SEND', str);
            return source.next(Object.assign({}, value, { action, progress }));
        });
    };
    queue.add(async () => {
        console.log('Initializing producer');
        await producer.init();
    });
    sink.subscribe({
        next: send
    });
    return {
        source: source.asObservable(),
        sink: sink
    };
};
