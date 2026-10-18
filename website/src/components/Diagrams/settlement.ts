import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

/**
 * What the crate asks the client consumer for, for each outcome of a handler: the call it makes,
 * whether it waits first, or `null` where the crate calls nothing.
 */
export const OUTCOMES = [
  {id: 'ack', call: 'ack_with_id', held: false},
  {id: 'retry', call: 'nack_with_id', held: false},
  {id: 'retryAfter', call: 'nack_with_id', held: true},
  {id: 'drop', call: 'ack_with_id', held: false},
  {id: 'unsettled', call: null, held: false},
] as const;

const COLUMN = 112;
const COLUMNS = [0, 124, 248] as const;
const ROW = 58;
const GAP = 12;
const TOP = 28;

/*
 * The settlement table drawn as three columns: what the handler returns, what the crate asks the
 * client for, and what the broker does with the message.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const cell = (id: string, column: number, row: number, fields: Partial<Box>): Box => ({
    id,
    x: COLUMNS[column]!,
    y: TOP + row * (ROW + GAP),
    w: COLUMN,
    h: ROW,
    kind: 'step',
    ...fields,
    lines: fields.lines ?? lines(words, id),
  });
  const callLines = (outcome: (typeof OUTCOMES)[number]): string[] => {
    if (outcome.call === null) {
      return lines(words, 'clientTimer');
    }
    // A delayed call says how long it waits, then names the call.
    return outcome.held ? [...lines(words, 'held'), outcome.call] : [outcome.call];
  };
  const boxes: Box[] = OUTCOMES.flatMap((outcome, row) => [
    cell(outcome.id, 0, row, {kind: 'service', code: true}),
    cell(`${outcome.id}Call`, 1, row, {
      kind: 'broker',
      code: outcome.call !== null,
      lines: callLines(outcome),
    }),
    cell(`${outcome.id}Effect`, 2, row, {
      kind: outcome.id === 'ack' ? 'good' : outcome.id === 'drop' ? 'bad' : 'core',
    }),
  ]);
  boxes.push({
    id: 'counted',
    x: 0,
    y: TOP + OUTCOMES.length * (ROW + GAP) + 8,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'counted'),
    href: '/architecture/pulsar/acknowledgement/',
  });
  const links: Link[] = OUTCOMES.flatMap((outcome) => [
    {from: outcome.id, to: `${outcome.id}Call`},
    {from: `${outcome.id}Call`, to: `${outcome.id}Effect`},
  ]);
  const lanes: Lane[] = (['outcomeLane', 'callLane', 'brokerLane'] as const).map(
    (key, column) => ({x: COLUMNS[column]! + COLUMN / 2, label: word(words, key)}),
  );
  return {boxes, links, lanes};
}
