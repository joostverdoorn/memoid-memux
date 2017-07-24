import { Subject } from '@reactivex/rxjs';
import { EARLIEST_OFFSET, GroupConsumer, LATEST_OFFSET, Producer, SimpleConsumer } from 'no-kafka';
import PQueue = require('p-queue');

import { Action, isAction, Progress, Quad  } from './index';

import * as Logger from './logger';

export type SinkConfig = {
  url: string;
  name: string;
  topic: string;
  concurrency: number;
};

export class KafkaSubject<T> extends Subject<T> {
  protected _producer: Producer;
  protected _name: string;
  protected _topic: string;
  private _queue: any;
  constructor(url: string, name: string, topic: string) {
    super();
    this._name = name;
    this._topic = topic;
    this._producer = new Producer({
      connectionString: url,
      logger: {
        logFunction: Logger.log
      }
    });

    const queue = new PQueue({
      concurrency: 8
    });

    queue.add(async () => {
      await this._producer.init();
    });

    this._queue = queue;
  }

  _send = async ({ type, quad }: Action) => {
    if (!isAction({ type, quad })) {
      throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
    }
    const value = JSON.stringify({ type, quad: { label: this._name, ...quad } });

    return this._queue.add( async () => {
      const result = await this._producer.send({ topic: this._topic, message: { value } });
      return Logger.log('SEND', value, result);
    });
  }

  next(value?: T) {
    return this._send(value as any)
      .then(() => super.next(value), (err) => this.error(err));
  }
}

// var x = new KafkaSubject<Action>();

export const createSink = ({ url, name, topic, concurrency = 8 }): KafkaSubject<Action> => {
  if (!(typeof url === 'string' && typeof name === 'string' && typeof topic === 'string')) {
    throw new Error('createSink should be called with a config containing a url, name and topic.');
  }

  return new KafkaSubject<Action>(url, name, topic);
  // const subject = new Subject();
  // const producer = new Producer({
  //   connectionString: url,
  //   logger: {
  //     logFunction: Logger.log
  //   }
  // });
  //
  // const queue = new PQueue({
  //   concurrency
  // });
  //
  // queue.add(async () => {
  //   await producer.init();
  // });

  // const send = ({ type, quad }: Action) => {
  //   if (!isAction({ type, quad })) {
  //     throw new Error('Trying to send a non-action: ' + JSON.stringify({ type, quad }));
  //   }
  //   const value = JSON.stringify({ type, quad: { label: name, ...quad } });
  //
  //   return queue.add( async () => {
  //     await producer.send({ topic, message: { value } });
  //     return Logger.log('SEND', value);
  //   });
  // };

  // subject.subscribe({
  //   next: send
  // });

  // return subject.lift;
};
