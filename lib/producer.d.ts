import { Actionable, Progressable, Duplex } from './index';
export declare type ProducerConfig = {
    url: string;
    name: string;
    topic: string;
    concurrency: number;
};
export declare type Producer = Duplex<Actionable & Progressable, Actionable>;
export declare const Producer: ({url, name, topic, concurrency}: ProducerConfig) => Duplex<Actionable & Progressable, Actionable>;
