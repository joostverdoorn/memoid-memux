export * from './consumer';
export * from './producer';
export declare type Operation<T> = {
    action: 'write' | 'delete';
    key: string;
    data: T;
};
export declare const isOperation: <T>(operation: any) => operation is Operation<T>;
export declare type Progress = {
    offset: number;
    partition: number;
    topic: string;
};
export declare const isProgress: (progress: any) => progress is Progress;
export declare type SSLConfig = {
    ca?: string;
    cert?: string;
    key?: string;
};
export declare type MemuxConfig<T> = {
    url: string;
    name: string;
    input?: string;
    output?: string;
    receive?: (action: Operation<T>) => Promise<void>;
    concurrency?: number;
    ssl?: SSLConfig;
};
declare function memux<T>({name, url, input, output, receive, concurrency, ssl}: MemuxConfig<T>): Promise<(<T>({action, key, data}: Operation<T>) => any)>;
export default memux;
