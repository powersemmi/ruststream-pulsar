import {describe, expect, it} from 'vitest';
import {resolveMarkdownLinks} from './markdownLinks';

// The hook as Docusaurus calls it: a link it could not resolve, and the page it was written on,
// relative to the site directory.
const resolve = (url: string, sourceFilePath: string): string | undefined =>
  (resolveMarkdownLinks() as (params: {url: string; sourceFilePath: string}) => string | undefined)(
    {url, sourceFilePath},
  );

describe('a Markdown link Docusaurus could not resolve', () => {
  it('on an English page a translation serves, goes to the page it names in that language', () => {
    // A language with the target translated and the source not yet: the English source is served
    // in that language, and its link names the English file of a page the language replaced.
    expect(
      resolve('index.md#cost-of-the-code', '../docs/architecture/benchmarks/methodology.md'),
    ).toBe('/architecture/benchmarks/#cost-of-the-code');
    expect(resolve('benchmarks/index.md', '../docs/architecture/path.md')).toBe(
      '/architecture/benchmarks/',
    );
  });

  it('on a translated page, goes to the English page it names when that one has no translation', () => {
    expect(
      resolve(
        '../lanes.md',
        'i18n/ru/docusaurus-plugin-content-docs/current/architecture/benchmarks/index.md',
      ),
    ).toBe('/architecture/lanes/');
  });

  it('fails the build when the page it names does not exist', () => {
    expect(() => resolve('missing.md', '../docs/architecture/path.md')).toThrow(/does not resolve/);
  });
});
