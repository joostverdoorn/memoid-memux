import { Subject } from '@reactivex/rxjs';
export declare type MemuxConfig = {
    url: string;
    name: string;
    input?: string;
    output?: string;
};
declare const memux: (config: MemuxConfig) => {
    source: Subject<{}>;
    sink: Subject<{}>;
    send: ({type, quad}: {
        type: any;
        quad: any;
    }) => void | Promise<void>;
};
export default memux;
