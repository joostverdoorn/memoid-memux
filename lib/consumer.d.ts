/// <reference types="node" />
import { Action } from './index';
export declare type SourceConfig = {
    url: string;
    name: string;
    topic: string;
    receiveFn: (action: Action) => Promise<void>;
};
export declare type KafkaMessage = {
    message: {
        value: Buffer | string;
    };
    offset?: number;
};
export declare const createReceive: ({url, name, topic, receiveFn}: SourceConfig) => Promise<any>;
