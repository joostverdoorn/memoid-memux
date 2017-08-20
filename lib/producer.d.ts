import { Action } from './index';
export declare const createSend: ({url, name, topic, concurrency}: {
    url: any;
    name: any;
    topic: any;
    concurrency?: number;
}) => Promise<({type, quad}: Action) => any>;
