import { Subject } from '@reactivex/rxjs';
export interface IQuad {
    subject: string;
    predicate: string;
    object: string;
}
export interface IAction {
    type: 'write' | 'delete';
    quad: IQuad;
}
export interface IProgress {
    offset: number;
    partition: number;
    topic: string;
}
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
    source: Subject<{
        action: IAction;
        progress: IProgress;
    }>;
    sink: Subject<IAction>;
    send: ({type, quad}: IAction) => any;
};
export default memux;
