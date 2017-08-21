import { Subject } from '@reactivex/rxjs';
import { Producer } from 'no-kafka';
import PQueue = require('p-queue');

import { Operation, isOperation, Progress, SSLConfig } from './index';

import * as Logger from './logger';

export type SendConfig<T> = {
  url: string;
  name: string;
  topic: string;
  concurrency: number;
  ssl?: SSLConfig
};

export const createSend = async <T>({ url, name, topic, concurrency = 8, ssl = {} }: SendConfig<T>) => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSend should be called with a config containing a url, name and topic.');
  }

  const producer = new Producer({
    connectionString: url,
    ssl,
    logger: {
      logFunction: Logger.log
    }
  });

  const queue = new PQueue({
    concurrency
  });

  const send = <T>({ action, key, data }: Operation<T>) => {
    if (!isOperation<T>({ action, key, data })) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify({ action, key, data }));
    }
    const value = JSON.stringify({ label: name, action, key, data });

    return queue.add( async () => {
      await producer.send({ topic, message: { key, value } });
      return Logger.log('SEND', key, value);
    });
  };

  return producer.init().then(() => send);
};
