/// <reference types="node" />
import { Actionable, Progressable, Duplex } from './index';
export declare type ConsumerConfig = {
    url: string;
    name: string;
    topic: string;
};
export declare type KafkaMessage = {
    message: {
        value: Buffer | string;
    };
    offset?: number;
};
export declare type Consumer = Duplex<Actionable & Progressable, Progressable>;
export declare const Consumer: ({url, name, topic}: ConsumerConfig) => Duplex<Actionable & Progressable, Progressable>;
