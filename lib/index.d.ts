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
export declare type MemuxOptions = {
    concurrency: number;
};
export declare type MemuxConfig<T> = {
    url: string;
    name: string;
    input?: string;
    output?: string;
    receive?: (action: Operation<T>) => Promise<void>;
    options: MemuxOptions;
};
declare function memux<T>({name, url, input, output, receive, options}: MemuxConfig<T>): Promise<(<T>({action, key, data}: Operation<T>) => any)>;
export default memux;
