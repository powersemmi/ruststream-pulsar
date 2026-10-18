import {client} from './client';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {redelivery} from './redelivery';
import {retries} from './retries';
import {settlement} from './settlement';
import {subscriptions} from './subscriptions';
import {topics} from './topics';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  topics,
  subscriptions,
  redelivery,
  client,
  design,
  lifecycle,
  path,
  lanes,
  settlement,
  retries,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
