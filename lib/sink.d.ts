import { Subject } from '@reactivex/rxjs';
export declare type SinkConfig = {
    url: string;
    name: string;
    topic: string;
    concurrency: number;
};
export declare const createSink: ({url, name, topic, concurrency}: {
    url: any;
    name: any;
    topic: any;
    concurrency?: number;
}) => Subject<{}>;
