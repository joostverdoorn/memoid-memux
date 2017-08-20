import { Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
import PQueue = require('p-queue');

import { Action, isAction, Progress, Quad  } from './index';

import * as Logger from './logger';

export const createSend = async ({ url, name, topic, concurrency = 8 }) => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSend should be called with a config containing a url, name and topic.');
  }

  const producer = new Producer({
    connectionString: url,
    logger: {
      logFunction: Logger.log
    }
  });

  const queue = new PQueue({
    concurrency
  });

  const send = ({ type, quad }: Action) => {
    if (!isAction({ type, quad })) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
    }
    const value = JSON.stringify({ type, quad: { label: name, ...quad } });

    return queue.add( async () => {
      await producer.send({ topic, message: { value } });
      return Logger.log('SEND', value);
    });
  };

  return producer.init().then(() => send);
};
