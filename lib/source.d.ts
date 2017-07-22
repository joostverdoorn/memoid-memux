/// <reference types="node" />
import { Observable } from '@reactivex/rxjs';
export declare type SourceConfig = {
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
export declare const createSource: ({url, name, topic}: SourceConfig) => Observable<any>;
