var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

const { Producer, SimpleConsumer, GroupConsumer, LATEST_OFFSET } = require('no-kafka');
const { Observable, Subject, ReplaySubject } = require('rxjs');

const OFFSET_COMMIT_INTERVAL = 1000;
const RETENTION_TIME = 1000 * 365 * 24;

const onError = error => {
  console.error(error);
  process.exit(1);
};

const memux = ({ url, name, input, output }) => {
  const source = input ? createSource(url, name, input) : null;
  const sink = output ? createSink(url, name, output) : null;
  return { sink, source };
};

const createSource = (connectionString, groupId, topic) => processor => {
  const input = new Subject();
  const output = new Subject();
  const offsets = new Subject();
  const consumer = new SimpleConsumer({ connectionString, groupId });
  const partition = 0;

  consumer.init().then(() => {
    consumer.fetchOffset([{ topic, partition }]).then(([{ offset }]) => {
      consumer.subscribe(topic, partition, { offset }, (messageSet, topic, partition) => {
        messageSet.forEach(({ offset, message: { value } }) => {
          input.next({ data: JSON.parse(value.toString()), topic, partition, offset });
        });
      });
    });
  });

  input.subscribe(({ data, topic, partition, offset }) => {
    const observable = Observable.from(processor(data));
    const onNext = data => output.next(data);
    const onComplete = () => offsets.next({ topic, partition, offset });
    observable.subscribe(onNext, onError, onComplete);
  });

  offsets.bufferTime(OFFSET_COMMIT_INTERVAL).subscribe(offs => {
    consumer.commitOffset(offs).catch(onError);
  });

  return output;
};

const createSink = (connectionString, label, topic) => {
  const producer = new Producer({ connectionString });
  const sink = new ReplaySubject();

  producer.init().then(() => {
    sink.subscribe(({ action, quad }) => {
      const message = { value: JSON.stringify({ action, quad: _extends({ label }, quad) }) };
      producer.send({ topic, message }).catch(onError);
    });
  });

  return sink;
};

module.exports = memux;