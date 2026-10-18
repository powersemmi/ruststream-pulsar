import {expect, it} from 'vitest';
import {client} from './client';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('looks the topic up through the service URL before it dials the owning broker', () => {
  const {links} = client(words);
  expect(links.map((link) => [link.from, link.to])).toEqual([
    ['url', 'lookup'],
    ['lookup', 'pool'],
    ['pool', 'cluster'],
  ]);
});

it('keeps the producers and the consumers on the pooled connections', () => {
  const {boxes} = client(words);
  const pool = boxes.find((box) => box.id === 'pool')!;
  for (const id of ['producer', 'consumer', 'permits', 'refresh']) {
    const box = boxes.find((candidate) => candidate.id === id)!;
    expect(box.y, id).toBeGreaterThan(pool.y);
    expect(box.y + box.h, id).toBeLessThan(pool.y + pool.h);
  }
});
