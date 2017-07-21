import { Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
import PQueue from 'p-queue';

import { Action, isAction, Progress, Quad  } from './index';

const log = console.log.bind(console);

const createSink = (connectionString: string, label: string, topic: string, concurrency: number) => {
  const subject = new Subject();
  const producer = new Producer({ connectionString });

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
    const value = JSON.stringify({ type, quad: { label, ...quad } });

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
