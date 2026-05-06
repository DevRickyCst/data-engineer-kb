import React from 'react';

/**
 * <Warning> — cautionary callout for MDX content.
 * Usage in .mdx:
 *   import Warning from '@site/src/components/Warning';
 *   <Warning>Watch out for this gotcha.</Warning>
 */
export default function Warning({ title = 'Warning', children }) {
  return (
    <aside
      style={{
        borderLeft: '4px solid #e3a72e',
        background: 'rgba(227, 167, 46, 0.1)',
        padding: '0.9rem 1.1rem',
        margin: '1.2rem 0',
        borderRadius: '0 6px 6px 0',
      }}
    >
      <strong style={{ display: 'block', marginBottom: '0.3rem', color: '#c98c1c' }}>
        ⚠️ {title}
      </strong>
      <div>{children}</div>
    </aside>
  );
}
