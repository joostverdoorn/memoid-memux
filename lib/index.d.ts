import { Observable, Subject } from '@reactivex/rxjs';
export declare type Quad = {
    subject: string;
    predicate: string;
    object: string;
};
export declare const isQuad: (quad: any) => quad is Quad;
export declare type Action = {
    type: 'write' | 'delete';
    quad: Quad;
};
export declare const isAction: (action: any) => action is Action;
export declare type Progress = {
    offset: number;
    partition: number;
    topic: string;
};
export declare const isProgress: (progress: any) => progress is Progress;
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
declare const memux: (config: MemuxConfig) => {
    source: Observable<Promise<any>>;
} | {
    sink: Subject<{}>;
} | {
    source: Observable<Promise<any>>;
    sink: Subject<{}>;
};
export default memux;
