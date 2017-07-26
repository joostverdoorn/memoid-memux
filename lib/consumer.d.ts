/// <reference types="node" />
import { Action, Progress, Duplex } from './index';
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
export declare type Consumer = Duplex<[Action, Progress], Progress>;
export declare const Consumer: ({url, name, topic}: ConsumerConfig) => Duplex<[Action, Progress], Progress>;
