import { Observable, Subject } from '@reactivex/rxjs';
import * as Kafka from 'no-kafka';
import PQueue = require('p-queue');

import { Action, isAction, Progress, Quad, Duplex  } from './index';

import * as Logger from './logger';

export type ProducerConfig = {
  url: string;
  name: string;
  topic: string;
  concurrency: number;
};

// export class KafkaSubject<T> extends Subject<T> {
//   protected _producer: Kafka.Producer;
//   protected _name: string;
//   protected _topic: string;
//   private _queue: any;
//   constructor(url: string, name: string, topic: string) {
//     super();
//     this._name = name;
//     this._topic = topic;
//     this._producer = new Kafka.Producer({
//       connectionString: url,
//       logger: {
//         logFunction: Logger.log
//       }
//     });
//
//     const queue = new PQueue({
//       concurrency: 8
//     });
//
//     queue.add(async () => {
//       await this._producer.init();
//     });
//
//     this._queue = queue;
//   }
//
//   _send = async ({ type, quad }: Action) => {
//     if (!isAction({ type, quad })) {
//       throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
//     }
//     const value = JSON.stringify({ type, quad: { label: this._name, ...quad } });
//
//     return this._queue.add( async () => {
//       const result = await this._producer.send({ topic: this._topic, message: { value } });
//       return Logger.log('SEND', value, result);
//     });
//   }
//
//   next(value?: T) {
//     return this._send(value as any)
//       .then(() => super.next(value), (err) => this.error(err));
//   }
// }

// var x = new KafkaSubject<Action>();

export type Producer = Duplex<[ Action, Progress ], Action>;
export const Producer = ({ url, name, topic, concurrency = 8 }): Producer => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('Producer should be called with a config containing a url, name and topic.');
  }

  // return new KafkaSubject<Action>(url, name, topic);

  const sink = new Subject<Action>();
  const producer = new Kafka.Producer({
    connectionString: url,
    logger: {
      logFunction: Logger.log
    }
  });

  const queue = new PQueue({
    concurrency
  });

  const source = new Subject<[ Action, Progress ]>();

  const send = ({ type, quad }: Action) => {
    console.log('Sending action.');
    if (!isAction({ type, quad })) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
    }
    const action = { type, quad: { label: name, ...quad } };
    const value = JSON.stringify(action);

    return queue.add( async () => {
      const [ result ] = await producer.send({ topic, message: { value } });
      await Logger.log('SEND', value);
      return source.next([ action, result ]);
    });
  };

  queue.add(async () => {
    console.log('Initializing producer');
    await producer.init();
  });

  sink.subscribe({
    next: send
  });

  // return sink.lift;
  return {
    source: source.asObservable(),
    sink: sink
  };
};
