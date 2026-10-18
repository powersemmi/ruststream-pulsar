import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The entries each partition of the partitioned topic holds in the drawing: partitions grow
// apart, so their entry numbers do.
export const PARTITIONS = [4, 2, 3] as const;

const CELL = 36;
const STEP = 42;

/*
 * Pulsar's topics: a tenant holds namespaces, a namespace holds topics. A partitioned topic is
 * one internal topic per partition, each a log of its own; a persistent topic keeps its entries
 * in BookKeeper ledgers; a non-persistent topic keeps nothing.
 */
export function topics(words: Words): {boxes: Box[]; links: Link[]} {
  const frame = (id: string, x: number, y: number, w: number, h: number, code = false): Box => ({
    id,
    x,
    y,
    w,
    h,
    kind: 'group',
    code,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {
      id: 'producer',
      x: 90,
      y: 0,
      w: 180,
      h: 44,
      kind: 'service',
      lines: lines(words, 'producer'),
    },
    frame('tenant', 0, 72, 360, 368),
    frame('namespace', 8, 98, 344, 334),
    frame('payments', 16, 124, 328, 138, true),
  ];
  PARTITIONS.forEach((count, partition) => {
    const y = 150 + partition * 36;
    boxes.push({
      id: `p${partition}`,
      x: 28,
      y,
      w: 100,
      h: 30,
      kind: 'broker',
      code: true,
      lines: lines(words, `p${partition}`),
    });
    for (let entry = 0; entry < count; entry += 1) {
      boxes.push({
        id: `p${partition}e${entry}`,
        x: 140 + entry * STEP,
        y,
        w: CELL,
        h: 30,
        kind: 'step',
        lines: [String(entry)],
      });
    }
  });
  boxes.push(frame('created', 16, 276, 328, 70, true));
  for (let entry = 0; entry < 4; entry += 1) {
    boxes.push({
      id: `created${entry}`,
      x: 28 + entry * STEP,
      y: 302,
      w: CELL,
      h: 30,
      kind: 'step',
      lines: [String(entry)],
    });
  }
  boxes.push(
    {id: 'ledgers', x: 204, y: 296, w: 128, h: 42, kind: 'broker', lines: lines(words, 'ledgers')},
    frame('ticks', 16, 360, 328, 64, true),
    {id: 'live', x: 28, y: 386, w: 304, h: 30, kind: 'step', lines: lines(words, 'live')},
    {id: 'bare', x: 0, y: 460, w: 360, h: 44, kind: 'note', lines: lines(words, 'bare')},
  );
  const links: Link[] = [{from: 'producer', to: 'payments', label: word(words, 'publishes')}];
  return {boxes, links};
}
