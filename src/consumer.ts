import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, ConsistentAssignmentStrategy } from 'no-kafka';
import * as uuid from 'node-uuid';

import { Action, isAction, Progress, Quad, Duplex  } from './index';

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

export type Consumer = Duplex<[ Action, Progress ], Progress>;
export const Consumer = ({ url, name, topic }: ConsumerConfig): Consumer => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('Consumer should be called with a config containing a url, name and topic.');
  }
  // const consumer = new SimpleConsumer({ connectionString: url, groupId: name, recoveryOffset: EARLIEST_OFFSET });
  const consumer = new GroupConsumer({
    connectionString: url,
    groupId: name,
    startingOffset: EARLIEST_OFFSET,
    recoveryOffset: EARLIEST_OFFSET,
    logger: {
      logFunction: Logger.log
    }
  });

  const sink = new Subject<Progress>();

  sink.subscribe({
    next: (p) => {
      console.log('Committing progress.');
      consumer.commitOffset(p);
      // observer.complete();
      // sinkSubscription.unsubscribe();
      // if (JSON.stringify(p) === JSON.stringify(progress)) {
      // }
    }
  });

  const source = new Observable<Observable<[Action, Progress]>>((outerObserver) => {
    const dataHandler = async (messageSet, topic, partition) => {
      const innerObservable = new Observable<[Action, Progress]>((observer) => {
        let progress;

        const messagesSent = Promise.all(messageSet.map(parseMessage).map(({ message, offset}) => {
          if (isAction(message)) {
            progress = { topic, partition, offset };

            // const sinkSubscription =

            observer.next([ message as Action, progress ]);
          } else {
            console.error(new Error(`Non-action encountered: ${message}`));
          }
        }));

        const teardown = async () => {
          console.log('Teardown logic called. Committing offset.');
          await messagesSent;
          return consumer.commitOffset(progress);
        };

        // return teardown;
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
