import React from 'react';

/**
 * <Note> — informational callout for MDX content.
 * Usage in .mdx:
 *   import Note from '@site/src/components/Note';
 *   <Note title="Heads up">Some friendly hint.</Note>
 */
export default function Note({ title = 'Note', children }) {
  return (
    <aside
      style={{
        borderLeft: '4px solid #2e7be3',
        background: 'rgba(46, 123, 227, 0.08)',
        padding: '0.9rem 1.1rem',
        margin: '1.2rem 0',
        borderRadius: '0 6px 6px 0',
      }}
    >
      <strong style={{ display: 'block', marginBottom: '0.3rem', color: '#2e7be3' }}>
        💡 {title}
      </strong>
      <div>{children}</div>
    </aside>
  );
}
