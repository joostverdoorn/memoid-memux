import { createReceive } from './consumer';
import { createSend } from './producer';

export * from './consumer';
export * from './producer';

import * as Logger from './logger';

export type Operation<T> = {
  action: 'write' | 'delete';
  key: string;
  data: T;
};

export const isOperation = <T>(operation): operation is Operation<T> => {
  return operation != null &&
         typeof operation === 'object' &&
         (operation.action === 'write' || operation.action === 'delete') &&
         typeof operation.key === 'string';
};

export type Progress = {
  offset: number;
  partition: number;
  topic: string;
};

export const isProgress = (progress): progress is Progress => {
  return progress != null &&
         typeof progress === 'object' &&
         typeof progress.offset === 'number' &&
         typeof progress.partition === 'number' &&
         typeof progress.topic === 'string';
};

export type MemuxOptions = {
  concurrency: number
};

export type MemuxConfig<T> = {
  url: string;
  name: string;
  input?: string;
  output?: string;
  receive?: (action: Operation<T>) => Promise<void>;
  options: MemuxOptions
};

const DEFAULT_OPTIONS = {
  concurrency: 8
};

async function memux<T>({ name, url, input, output, receive, options = DEFAULT_OPTIONS }: MemuxConfig<T>) {
  if (input != null && receive != null) await createReceive({
    name,
    url,
    topic: input,
    receive
  });

  if (output != null) return createSend({
    name,
    url,
    topic: output,
    concurrency: options.concurrency
  });
}

export default memux;
