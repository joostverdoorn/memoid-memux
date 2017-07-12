declare const Producer: any, SimpleConsumer: any, GroupConsumer: any, LATEST_OFFSET: any, EARLIEST_OFFSET: any;
declare const Observable: any, Subject: any, ReplaySubject: any;
declare const PQueue: any;
declare const OFFSET_COMMIT_INTERVAL = 1000;
declare const RETENTION_TIME: number;
declare const log: (...things: any[]) => void;
declare const onError: (error: any) => void;
declare const isQuad: (quad: any) => boolean;
declare const isAction: (action: any) => boolean;
declare const isProgress: (progress: any) => boolean;
declare const memux: ({url, name, input, output}: {
    url: any;
    name: any;
    input?: null;
    output?: null;
}) => {
    source: any;
    sink: any;
    send: ({type, quad}: {
        type: any;
        quad: any;
    }) => any;
};
declare const createSource: (connectionString: any, groupId: any, topic: any) => {
    source: any;
    sink: any;
};
declare const createSend: (connectionString: any, label: any, topic: any) => ({type, quad}: {
    type: any;
    quad: any;
}) => any;
