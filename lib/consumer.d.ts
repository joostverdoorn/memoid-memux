/// <reference types="node" />
import { Operation } from './index';
export declare type SourceConfig<T> = {
    url: string;
    name: string;
    topic: string;
    receive: (action: Operation<T>) => Promise<void>;
};
export declare type KafkaMessage = {
    message: {
        value: Buffer | string;
    };
    offset?: number;
};
export declare const createReceive: <T>({url, name, topic, receive}: SourceConfig<T>) => Promise<any>;
