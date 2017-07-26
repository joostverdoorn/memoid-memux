import { Observable, Subject } from '@reactivex/rxjs';
// import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
// import PQueue from 'p-queue';

// import { createSource } from './source';
// import { createSink, KafkaSubject } from './sink';

// export * from './source';
// export * from './sink';

import { Consumer } from './consumer';
import { Producer } from './producer';

import * as Logger from './logger';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

export type Quad = {
  subject: string;
  predicate: string;
  object: string;
};

export const isQuad = (quad): quad is Quad => {
  return quad != null &&
         typeof quad === 'object' &&
         typeof quad.subject === 'string' &&
         typeof quad.predicate === 'string' &&
         typeof quad.object === 'string';
};

export type Action = {
  type: 'write' | 'delete';
  quad: Quad;
};

export const isAction = (action): action is Action => {
  return action != null &&
         typeof action === 'object' &&
         (action.type === 'write' || action.type === 'delete') &&
         isQuad(action.quad);
};

export type Progress = {
  offset: number;
  partition: number;
  topic: string;
};

export const isProgress = (progress): progress is Progress => {
  return progress != null &&
         typeof progress === 'object' &&
         typeof progress.offset === 'number' &&
         typeof progress.partition === 'number' &&
         typeof progress.topic === 'string';
};

export type MemuxOptions = {
  concurrency: number
};

export type MemuxConfig = {
  url: string;
  name: string;
  input?: string;
  output?: string;
  options: MemuxOptions
};

const DEFAULT_OPTIONS = {
  concurrency: 8
};

export type Readable<T> = { source: Observable<T> };
export type Writeable<T> = { sink: Subject<T> };
export type Duplex<U,V> = Readable<U> & Writeable<V>;

const memux = (config: MemuxConfig): { consumer?: Consumer, producer?: Producer } => {
  // return null;
  const { url, name, input = null, output = null, options = DEFAULT_OPTIONS } = config;
  if (input == null && output == null) {
    throw new Error('An input, ouput or both must be provided.');
  }

  if (output == null) {
    return {
      consumer: Consumer({ url, name, topic: input })
    };
  }

  if (input == null) {
    return {
      producer: Producer({ url, name, topic: output, concurrency: options.concurrency })
    };
  }

  return {
    consumer: Consumer({ url, name, topic: input }),
    producer: Producer({ url, name, topic: output, concurrency: options.concurrency })
  };
};

export default memux;
