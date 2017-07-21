import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, ConsistentAssignmentStrategy } from 'no-kafka';
import * as uuid from 'node-uuid';

import { Action, isAction, Progress, Quad  } from './index';


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

export const createSource = ({ url, name, topic }: SourceConfig) => {
  // const consumer = new SimpleConsumer({ connectionString: url, groupId: name, recoveryOffset: EARLIEST_OFFSET });
  const consumer = new GroupConsumer({ connectionString: url, groupId: name, recoveryOffset: EARLIEST_OFFSET });

  const observable = new Observable<{ action: Action, progress: Progress }>((observer) => {
    const dataHandler = async (messageSet, nextTopic, nextPartition) => {
      return messageSet.map(parseMessage).forEach(({ message, offset}) => {
        if (isAction(message)) {
          const progress = { topic: nextTopic, partition: nextPartition, offset };
          observer.next({ action: message, progress });
        } else {
          console.error(new Error(`Non-action encountered: ${message}`));
        }
      });
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

    consumer.init(strategies);
  });

  const commitOffset = async ({ action, progress }) => {
    await consumer.commitOffset(progress);
    return action;
  };

  return observable
    .map(commitOffset);
};
