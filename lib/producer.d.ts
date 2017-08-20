import { Operation } from './index';
export declare const createSend: ({url, name, topic, concurrency}: {
    url: any;
    name: any;
    topic: any;
    concurrency?: number;
}) => Promise<(<T>({action, key, data}: Operation<T>) => any)>;
