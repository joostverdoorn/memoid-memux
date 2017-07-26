import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, ConsistentAssignmentStrategy } from 'no-kafka';
import * as uuid from 'node-uuid';

import { Action, Actionable, isAction, Progress, Progressable, Quad, Duplex  } from './index';

import * as Logger from './logger';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

export type ConsumerConfig = {
  url: string;
  name: string;
  topic: string;
};

export type KafkaMessage = {
  message: {
    value: Buffer | string
  }
  offset?: number;
};

const parseMessage = ({ offset, message: { value } }: KafkaMessage) => {
  const data = value.toString();
  let message: string;

  try {
    message = JSON.parse(data);
  } catch (error) {
    if (!(error instanceof SyntaxError)) {
      throw error;
    }
  }

  return { message, offset };
};

export type Consumer = Duplex<Actionable & Progressable, Progressable>;
export const Consumer = ({ url, name, topic }: ConsumerConfig): Consumer => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('Consumer should be called with a config containing a url, name and topic.');
  }
  const consumer = new GroupConsumer({
    connectionString: url,
    groupId: name,
    startingOffset: EARLIEST_OFFSET,
    recoveryOffset: EARLIEST_OFFSET,
    logger: {
      logFunction: Logger.log
    }
  });

  const sink = new Subject<Progressable>();

  sink.subscribe({
    next: ({ progress }) => {
      console.log('Committing progress.');
      consumer.commitOffset(progress);
    }
  });

  const source = new Observable<Observable<Actionable & Progressable>>((outerObserver) => {
    const dataHandler = async (messageSet, topic, partition) => {
      const innerObservable = new Observable<Actionable & Progressable>((observer) => {
        let progress;

        const messagesSent = Promise.all(messageSet.map(parseMessage).map(({ message, offset}) => {
          if (isAction(message)) {
            progress = { topic, partition, offset };
            observer.next({ action: message as Action, progress });
          } else {
            console.error(new Error(`Non-action encountered: ${message}`));
          }
        }));
      });

      return outerObserver.next(innerObservable);
    };

    const strategies = [{
      subscriptions: [ topic ],
      metadata: {
        id: `${name}-${uuid.v4()}`,
        weight: 50
      },
      strategy: new ConsistentAssignmentStrategy(),
      handler: dataHandler

    }]

    consumer.init(strategies).catch(outerObserver.error);
  });

  return {
    source: source.concatMap(o => o),
    sink: sink
  }
};
