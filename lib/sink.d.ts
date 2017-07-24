import { Subject } from '@reactivex/rxjs';
import { Producer } from 'no-kafka';
import { Action } from './index';
export declare type SinkConfig = {
    url: string;
    name: string;
    topic: string;
    concurrency: number;
};
export declare class KafkaSubject<T> extends Subject<T> {
    protected _producer: Producer;
    protected _name: string;
    protected _topic: string;
    private _queue;
    constructor(url: string, name: string, topic: string);
    _send: ({type, quad}: Action) => Promise<any>;
}
export declare const createSink: ({url, name, topic, concurrency}: {
    url: any;
    name: any;
    topic: any;
    concurrency?: number;
}) => KafkaSubject<Action>;
