import {expect, it} from 'vitest';
import {PARTITIONS, topics} from './topics';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('draws a partitioned topic as one log per partition, each numbered from zero', () => {
  const {boxes} = topics(words);
  PARTITIONS.forEach((count, partition) => {
    const entries = boxes
      .filter((box) => box.id.startsWith(`p${partition}e`))
      .map((box) => box.lines.join(''));
    expect(entries).toEqual(Array.from({length: count}, (_, entry) => String(entry)));
  });
});

it('sends the producer to the partitioned topic and keeps every topic inside its namespace', () => {
  const {boxes, links} = topics(words);
  expect(links.map((link) => [link.from, link.to])).toEqual([['producer', 'payments']]);
  const namespace = boxes.find((box) => box.id === 'namespace')!;
  for (const id of ['payments', 'created', 'ticks']) {
    const topic = boxes.find((box) => box.id === id)!;
    expect(topic.y + topic.h, id).toBeLessThanOrEqual(namespace.y + namespace.h);
    expect(topic.y, id).toBeGreaterThanOrEqual(namespace.y);
  }
});
