import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The entries the topic holds in the drawing.
const ENTRIES = 7;
const CELL = 40;
const STEP = 48;

// The entry each subscription reads next: every subscription keeps a cursor of its own.
export const CURSORS = {audit: 1, billing: 5} as const;

// The subscription types, in the order the drawing places them, two to a row.
export const TYPES = ['exclusive', 'shared', 'failover', 'keyShared'] as const;

const centre = (entry: number): number => 12 + entry * STEP + CELL / 2;

/*
 * Subscriptions: two named cursors on one topic, each receiving the whole topic, and under one of
 * them how its consumers share it, one panel per subscription type.
 */
export function subscriptions(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'topic', x: 0, y: 0, w: 360, h: 70, kind: 'group', lines: lines(words, 'topic')},
  ];
  for (let entry = 0; entry < ENTRIES; entry += 1) {
    boxes.push({
      id: `e${entry}`,
      x: 12 + entry * STEP,
      y: 26,
      w: CELL,
      h: 30,
      kind: 'step',
      lines: [String(entry)],
    });
  }
  const cursor = (id: keyof typeof CURSORS, x: number): Box => ({
    id,
    x,
    y: 104,
    w: 172,
    h: 44,
    kind: 'core',
    lines: lines(words, id),
  });
  const audit = cursor('audit', 0);
  const billing = cursor('billing', 188);
  boxes.push(audit, billing, {
    id: 'types',
    x: 0,
    y: 176,
    w: 360,
    h: 228,
    kind: 'group',
    lines: lines(words, 'types'),
  });
  TYPES.forEach((type, index) => {
    const x = index % 2 === 0 ? 8 : 186;
    const y = index < 2 ? 202 : 304;
    boxes.push(
      {id: type, x, y, w: 166, h: 92, kind: 'group', code: true, lines: lines(words, type)},
      {id: `${type}C1`, x: x + 8, y: y + 26, w: 150, h: 26, kind: 'core', lines: lines(words, `${type}C1`)},
      {
        id: `${type}C2`,
        x: x + 8,
        y: y + 58,
        w: 150,
        h: 26,
        // A consumer that receives nothing while the first one holds the subscription.
        kind: type === 'exclusive' || type === 'failover' ? 'note' : 'core',
        lines: lines(words, `${type}C2`),
      },
    );
  });
  boxes.push({
    id: 'durable',
    x: 0,
    y: 424,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'durable'),
  });
  const up = (box: Box, entry: number): Link => ({
    from: box.id,
    to: `e${entry}`,
    exit: 'top',
    enter: 'bottom',
    exitAt: (centre(entry) - box.x) / box.w,
    label: word(words, 'next'),
  });
  const links: Link[] = [
    up(audit, CURSORS.audit),
    up(billing, CURSORS.billing),
    {from: 'billing', to: 'types', enterAt: (billing.x + billing.w / 2) / 360},
  ];
  return {boxes, links};
}
