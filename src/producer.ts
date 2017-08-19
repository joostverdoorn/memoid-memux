import { Observable, Subject } from '@reactivex/rxjs';
import * as Kafka from 'no-kafka';
import PQueue = require('p-queue');

import { Action, Actionable, isAction, Progress, Progressable, Quad, Duplex  } from './index';

import * as Logger from './logger';

export type ProducerConfig = {
  url: string;
  name: string;
  topic: string;
  concurrency: number;
};

export type Producer = Duplex<Actionable & Progressable, Actionable>;
export const Producer = ({ url, name, topic, concurrency = 8 }: ProducerConfig): Producer => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('Producer should be called with a config containing a url, name and topic.');
  }

  const sink = new Subject<Actionable>();
  const producer = new Kafka.Producer({
    connectionString: url,
    logger: {
      logFunction: Logger.log
    }
  });

  const queue = new PQueue({
    concurrency
  });

  const source = new Subject<Actionable & Progressable>();

  const send = <T extends Actionable>(value: T): T => {
    console.log(`Sending action on ${topic}.`);
    if (!isAction(value.action)) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify(value.action));
    }
    const action: Action = { type: value.action.type, quad: { label: name, ...value.action.quad } };
    const str = JSON.stringify(action);

    return queue.add( async () => {
      const [ progress ] = await producer.send({ topic, message: { value: str } });
      await Logger.log('SEND', str);
      return source.next(Object.assign({}, value, { action, progress } ));
    });
  };

  queue.add(async () => {
    console.log('Initializing producer');
    await producer.init();
  });

  sink.subscribe({
    next: send
  });

  return {
    source: source.asObservable(),
    sink: sink
  };
};
