import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('brings every retry back through the broker before the client checks the cap', () => {
  const {links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('outcome')).toEqual(['now', 'later']);
  expect(from('now')).toEqual(['redelivered']);
  expect(from('later')).toEqual(['redelivered']);
  expect(from('redelivered')).toEqual(['cap']);
  expect(from('cap')).toEqual(['again', 'deadLetter']);
  expect([...from('again'), ...from('deadLetter')]).toEqual([]);
});
