import { Action, Progress, Duplex } from './index';
export declare type ProducerConfig = {
    url: string;
    name: string;
    topic: string;
    concurrency: number;
};
export declare type Producer = Duplex<[Action, Progress], Action>;
export declare const Producer: ({url, name, topic, concurrency}: {
    url: any;
    name: any;
    topic: any;
    concurrency?: number;
}) => Duplex<[Action, Progress], Action>;
