/// <reference types="node" />
export declare type SourceConfig = {
    connectionString: string;
    groupId: string;
    topic: string;
    partition: number;
};
export declare type KafkaMessage = {
    message: {
        value: Buffer | string;
    };
    offset?: number;
};
