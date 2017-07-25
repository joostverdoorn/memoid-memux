import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, ConsistentAssignmentStrategy } from 'no-kafka';
import * as uuid from 'node-uuid';

import { Action, isAction, Progress, Quad  } from './index';

import * as Logger from './logger';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

export type SourceConfig = {
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

export const createSource = ({ url, name, topic }: SourceConfig): Observable<Action> => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSource should be called with a config containing a url, name and topic.');
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

  const outerObservable = new Observable<Observable<Action>>((outerObserver) => {
    const dataHandler = async (messageSet, topic, partition) => {
      const innerObservable = new Observable<Action>((observer) => {
        let progress;

        const messagesSent = Promise.all(messageSet.map(parseMessage).map(({ message, offset}) => {
          if (isAction(message)) {
            observer.next(message as Action);
            progress = { topic, partition, offset };
          } else {
            console.error(new Error(`Non-action encountered: ${message}`));
          }
        })).then(() => {
          observer.complete();
        });

        const teardown = async () => {
          await messagesSent;
          return consumer.commitOffset(progress);
        };

        return teardown;
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

  // return new Observable(observer => {
  //   console.log('Subscribing to outer obervable');
  //   outerObservable.subscribe({
  //     next: (observable) => {
  //       return new Promise((resolve) => {
  //         console.log('Subscribing to inner obervable');
  //         const subscription = observable.subscribe({
  //           next: (value) => {
  //             console.log(`Received value from inner observable:`, value);
  //             observer.next(value);
  //           },
  //           error: observer.error,
  //           complete: () => {
  //             console.log('Inner obervable completed. Calling teardown and moving on to the next inner observable');
  //             // Call teardown logic of innerObservable to commit offset
  //             subscription.unsubscribe();
  //             resolve();
  //           }
  //         });
  //       });
  //     },
  //     error: observer.error,
  //   })
  // });

  return outerObservable.concatMap(o => o);
};
