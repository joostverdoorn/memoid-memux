import { Observable, Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';

import { Action, isAction, Progress, Quad  } from './index';

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

export type SourceConfig = {
  connectionString: string;
  groupId: string;
  topic: string;
  partition: number;
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

const createSource = ({ connectionString, groupId, topic, partition }: SourceConfig) => {
  const consumer = new SimpleConsumer({ connectionString, groupId, recoveryOffset: EARLIEST_OFFSET });

  return new Observable((observer) => {
    (async () => {
      await consumer.init();
      const [ { offset } ] = await consumer.fetchOffset([{ topic, partition }]) as any;
      consumer.subscribe(topic, partition, {
        offset,
        time: offset === LATEST_OFFSET ? EARLIEST_OFFSET : null
      }, async (messageSet, nextTopic, nextPartition) => {
        return messageSet.map(parseMessage).forEach(({ message, offset}) => {
          if (isAction(message)) {
            const progress = { topic: nextTopic, partition: nextPartition, offset };
            observer.next({ action: message, progress });
          } else {
            console.error(new Error(`Non-action encountered: ${message}`));
          }
        });
      });
    })();
  });
};
