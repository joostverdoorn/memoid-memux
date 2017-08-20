import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, ConsistentAssignmentStrategy } from 'no-kafka';
import * as uuid from 'node-uuid';

import { Operation, isOperation, Progress  } from './index';

import * as Logger from './logger';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

export type SourceConfig<T> = {
  url: string;
  name: string;
  topic: string;
  receive: (action: Operation<T>) => Promise<void>;
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

export const createReceive = async <T>({ url, name, topic, receive }: SourceConfig<T>) => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSource should be called with a config containing a url, name, topic and receiveFn.');
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

  const dataHandler = async (messageSet, topic, partition) => {
    const messagesSent = Promise.all(messageSet.map(parseMessage).map(({ message, offset}) => {
      if (!isOperation<T>(message)) throw new Error(`Non-action encountered: ${message}`);
      const progress = { topic, partition, offset };
      return Promise.resolve(receive(message)).then(() => {
        return consumer.commitOffset(progress);
      });
    }));
  };

  const strategies = [{
    subscriptions: [ topic ],
    metadata: {
      id: `${name}-${uuid.v4()}`,
      weight: 50
    },
    strategy: new ConsistentAssignmentStrategy(),
    handler: dataHandler

  }];

  return consumer.init(strategies);
};
