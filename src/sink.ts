import { Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
import PQueue = require('p-queue');

import { Action, isAction, Progress, Quad  } from './index';

const log = console.log.bind(console);

export type SinkConfig = {
  url: string;
  name: string;
  topic: string;
  concurrency: number;
};

export const createSink = ({ url, name, topic, concurrency = 8 }) => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSink should be called with a config containing a url, name and topic.');
  }

  const subject = new Subject();
  const producer = new Producer({ connectionString: url });

  const queue = new PQueue({
    concurrency
  });

  queue.add(async () => {
    await producer.init();
  });

  const send = ({ type, quad }: Action) => {
    if (!isAction({ type, quad })) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
    }
    const value = JSON.stringify({ type, quad: { label: name, ...quad } });

    return queue.add( async () => {
      await producer.send({ topic, message: { value } });
      return log('SEND', value);
    });
  };

  subject.subscribe({
    next: send
  })

  return subject;
};
