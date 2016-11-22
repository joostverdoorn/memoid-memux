const { Producer, SimpleConsumer } = require('no-kafka');
const { Observable, Subject, ReplaySubject } = require('rxjs');
const errorHandler = error => { if (error) { console.error(error); throw error; } }

const memux = ({ url, name, input, output }) => {
  const process = input ? createProcess(url, name, input) : null;
  const sink = output ? createSink(url, name, output) : null;
  return { sink, process };
};

const createProcess = (url, name, topic) => fn => {
  const messages = new Subject();
  const metadata = new Subject();
  const ouput = new Subject();
  const consumer = new SimpleConsumer({ connectionString: url, groupId: name });

  const handler = (messageSet, topic, partition) => {
    messageSet.forEach(({ offset, message: { value }}) => {
      messages.next(JSON.parse(value.toString('utf8')));
      metadata.next({ topic, partition, offset });
    });
  };

  consumer.init().then(() => {
    consumer.subscribe(topic, handler);
  });

  messages.map(fn).zip(metadata).forEach(([value, { topic, partition, offset }]) => {
    consumer.commitOffset([{ topic, partition, offset }]).then(() => ouput.next(value));
  });

  return ouput.flatMap(value => value);;
};

const createSink = (url, name, topic) => {
  const producer = new Producer({ connectionString: url });
  const sink = new ReplaySubject();

  producer.init().then(() => {
    sink.subscribe(({ action, quad }) => {
      producer.send({ topic, message: { value: JSON.stringify({ action, quad }) } })
    });
  });

  return sink;
};

module.exports = memux;
