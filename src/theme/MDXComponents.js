import MDXComponents from '@theme-original/MDXComponents';
import T from '@site/src/components/Term';

/**
 * Globally registers MDX components so they can be used in any .md / .mdx
 * page without an import statement.
 *
 * Currently registered:
 *   <T>term</T> — inline glossary term with hover tooltip (see src/components/Term.js)
 */
export default {
  ...MDXComponents,
  T,
};
