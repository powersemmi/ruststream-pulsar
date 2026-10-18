import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The partitions of the topic the drawing reads.
export const PARTITIONS = 3;

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: its consumer reads every topic and partition it holds, one stream
 * hands the messages over one at a time, and `workers(n, by_key)` keys each message to a lane by
 * its partition key.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = Array.from({length: PARTITIONS}, (_, partition) => ({
    id: `p${partition}`,
    x: 12 + partition * 116,
    y: 0,
    w: 104,
    h: 34,
    kind: 'broker' as const,
    code: true,
    lines: lines(words, `p${partition}`),
  }));
  boxes.push(
    {id: 'consumer', x: 12, y: 66, w: 336, h: 44, kind: 'broker', lines: lines(words, 'consumer')},
    {id: 'stream', x: 12, y: 138, w: 336, h: 44, kind: 'broker', lines: lines(words, 'stream')},
    {
      id: 'keyed',
      x: 12,
      y: 210,
      w: 336,
      h: 44,
      kind: 'core',
      lines: lines(words, 'keyed'),
      href: '/components/publishing/keys/',
    },
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 286,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {id: 'unkeyed', x: 0, y: 368, w: 360, h: 44, kind: 'note', lines: lines(words, 'unkeyed')},
    {
      id: 'keyShared',
      x: 0,
      y: 424,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'keyShared'),
      href: '/architecture/pulsar/subscriptions/',
    },
  );
  const links: Link[] = [
    ...Array.from({length: PARTITIONS}, (_, partition) => ({
      from: `p${partition}`,
      to: 'consumer',
      enterAt: (64 + partition * 116 - 12) / 336,
    })),
    {from: 'consumer', to: 'stream', label: word(words, 'pushed')},
    {from: 'stream', to: 'keyed', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({
      from: 'keyed',
      to: `lane${lane}`,
      exitAt: (x + 80 - 12) / 336,
    })),
  ];
  return {boxes, links};
}
