import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
import PQueue from 'p-queue';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

const log = (...things) => {
  // tslint:disable-next-line no-console
  console.log(new Date().toISOString(), ...things);
};

const onError = error => {
  // tslint:disable-next-line no-console
  console.error(error);
  process.exit(1);
};

export type Quad = {
  subject: string;
  predicate: string;
  object: string;
};

export const isQuad = (quad): quad is Quad => {
  return typeof quad === 'object' &&
         typeof quad.subject === 'string' &&
         typeof quad.predicate === 'string' &&
         typeof quad.object === 'string';
};

export type Action = {
  type: 'write' | 'delete';
  quad: Quad;
};

export const isAction = (action): action is Action => {
  return typeof action === 'object' &&
         (action.type === 'write' || action.type === 'delete') &&
         isQuad(action.quad);
};

export type Progress = {
  offset: number;
  partition: number;
  topic: string;
};

export const isProgress = (progress): progress is Progress => {
  return typeof progress === 'object' &&
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

const memux = (config: MemuxConfig) => {
  const { url, name, input = null, output = null, options } = config;
  const { source, sink } = input ? createSource(url, name, input) : { source: undefined, sink: undefined };
  const send = output ? createSend(url, name, output, options.concurrency) : null;
  return { source, sink, send };
};

const createSource = (connectionString, groupId, topic) => {
  const sink = new Subject<Action>();
  const source = new Subject<{ action: Action, progress: Progress }>();
  const consumer = new SimpleConsumer({ connectionString, groupId, recoveryOffset: EARLIEST_OFFSET });
  const partition = 0;

  consumer.init().then(() => {
    sink.bufferTime(OFFSET_COMMIT_INTERVAL).subscribe(progress => {
      (consumer as any).commitOffset(progress).catch(onError);
    }, onError);

    consumer.fetchOffset([{ topic, partition }]).then(([{ offset }]: any) => {
      consumer.subscribe(topic, partition, {
        offset,
        time: offset === LATEST_OFFSET ? EARLIEST_OFFSET : null
      }, (messageSet, nextTopic, nextPartition) => {
        return messageSet.forEach(({ offset: nextOffset, message: { value }}) => {
          const data = value.toString();
          const progress = { topic: nextTopic, partition: nextPartition, offset: nextOffset };

          let action;

          try {
            action = JSON.parse(data);
          } catch (error) {
            if (!(error instanceof SyntaxError)) {
              throw error;
            }
          }

          log('RECV', data);
          if (isAction(action)) {
            source.next({ action, progress });
          }
        }) as any;
      });
    });
  });

  return { source, sink };
};

const createSend = (connectionString: string, label: string, topic: string, concurrency: number) => {
  const producer = new Producer({ connectionString });
  const ready = producer.init().catch(onError);

  const queue = new PQueue({
    concurrency
  });

  return ({ type, quad }: Action) => {
    if (!isAction({ type, quad })) {
      return onError(new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad })));
    }
    const value = JSON.stringify({ type, quad: { label, ...quad } });

    return queue.add( () => ready.then(() => {
      return producer.send({ topic, message: { value } });
    }).then(() => {
      return log('SEND', value);
    }));
  };
};

export default memux;
