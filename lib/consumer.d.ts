/// <reference types="node" />
import { Operation, SSLConfig } from './index';
export declare type SourceConfig<T> = {
    url: string;
    name: string;
    topic: string;
    receive: (action: Operation<T>) => Promise<void>;
    ssl?: SSLConfig;
    concurrency?: number;
};
export declare type KafkaMessage = {
    message: {
        value: Buffer | string;
    };
    offset?: number;
};
export declare const createReceive: <T>({url, name, topic, receive, ssl, concurrency}: SourceConfig<T>) => Promise<any>;
