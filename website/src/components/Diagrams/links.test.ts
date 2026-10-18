import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName, type Layout} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> => {
  const make: (words: Words) => Layout = LAYOUTS[name];
  return Object.fromEntries(
    make(words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );
};

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  const every = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
    Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
  );

  // One test over every box, so the guard holds for whichever diagrams the registry has.
  it('names a page of the site', () => {
    for (const {name, box, href} of every) {
      // A root-relative route: the link prepends the language's base URL, so one route serves
      // every translation, and the site ends every route with a slash.
      expect(href, `${name}, ${box}`).toMatch(/^\/[a-z0-9/-]+\/$/);
      expect(page(href), `${name}, ${box}: no page under docs/ for ${href}`).toBe(true);
    }
  });
});

describe('a box that names a component', () => {
  it('links to that component page on the path of a message', () => {
    expect(hrefs('path')).toEqual({
      consumer: '/architecture/pulsar/client/',
      take: '/components/context/',
      dispatch: '/architecture/lanes/',
      body: '/components/subscriptions/',
      publisher: '/components/publishing/',
      settle: '/components/settlement/',
      ack: '/architecture/pulsar/acknowledgement/',
    });
  });

  it('links to that component page in the design of the crate', () => {
    expect(hrefs('design')).toEqual({
      subscriber: '/architecture/path/',
      publishers: '/components/publishing/',
      message: '/architecture/settlement/',
      seeker: '/components/positions/seek/',
      broker: '/components/broker/',
      descriptors: '/components/subscriptions/',
      consumer: '/architecture/pulsar/client/',
      producer: '/architecture/pulsar/client/',
      cluster: '/architecture/pulsar/',
    });
  });

  it('links to that component page in the lifecycle', () => {
    expect(hrefs('lifecycle')).toEqual({
      unconnected: '/components/broker/',
      connected: '/architecture/design/',
      closed: '/components/broker/shutdown/',
      client: '/components/broker/startup/',
      subscriptions: '/components/subscriptions/',
      producer: '/components/publishing/',
      close: '/components/broker/shutdown/',
      afterShutdown: '/components/broker/shutdown/',
    });
  });

  it('links to that component page in the lanes, the retries and the redelivery', () => {
    expect(hrefs('lanes')).toEqual({
      keyed: '/components/publishing/keys/',
      keyShared: '/architecture/pulsar/subscriptions/',
    });
    expect(hrefs('retries')).toEqual({
      later: '/components/settlement/retry-after/',
      cap: '/components/settlement/dead-letters/',
      deadLetter: '/components/settlement/dead-letters/',
    });
    expect(hrefs('redelivery')).toEqual({
      deadLetter: '/components/settlement/dead-letters/',
      timeout: '/components/settlement/ack-timeout/',
    });
  });

  it('links to that component page in the client', () => {
    expect(hrefs('client')).toEqual({
      url: '/components/broker/',
      retries: '/components/broker/operation-retries/',
    });
  });
});
