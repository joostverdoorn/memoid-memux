import { Operation, SSLConfig } from './index';
export declare type SendConfig<T> = {
    url: string;
    name: string;
    topic: string;
    concurrency: number;
    ssl?: SSLConfig;
};
export declare const createSend: <T>({url, name, topic, concurrency, ssl}: SendConfig<T>) => Promise<(<T>({action, key, data}: Operation<T>) => any)>;
