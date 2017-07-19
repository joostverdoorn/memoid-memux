import { Subject } from '@reactivex/rxjs';
declare const memux: ({url, name, input, output}: {
    url: any;
    name: any;
    input?: null;
    output?: null;
}) => {
    source: Subject<{}>;
    sink: Subject<{}>;
    send: ({type, quad}: {
        type: any;
        quad: any;
    }) => void | Promise<void>;
};
export default memux;
