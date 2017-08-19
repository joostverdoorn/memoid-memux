import { Observable, Subject } from '@reactivex/rxjs';
export * from './consumer';
export * from './producer';
export declare type Quad = {
    subject: string;
    predicate: string;
    object: string;
    label?: string;
};
export declare const isQuad: (quad: any) => quad is Quad;
export declare type Action = {
    type: 'write' | 'delete';
    quad: Quad;
};
export declare const isAction: (action: any) => action is Action;
export declare type Actionable = {
    action: Action;
};
export declare type Progress = {
    offset: number;
    partition: number;
    topic: string;
};
export declare const isProgress: (progress: any) => progress is Progress;
export declare type Progressable = {
    progress: Progress;
};
export declare type MemuxOptions = {
    concurrency: number;
};
export declare type MemuxConfig = {
    url: string;
    name: string;
    input?: string;
    output?: string;
    options: MemuxOptions;
};
export declare type Readable<T> = {
    source: Observable<T>;
};
export declare type Writeable<T> = {
    sink: Subject<T>;
};
export declare type Duplex<U, V> = Readable<U> & Writeable<V>;
declare const memux: (config: MemuxConfig) => {
    consumer?: Duplex<Actionable & Progressable, Progressable>;
    producer?: Duplex<Actionable & Progressable, Actionable>;
};
export default memux;
