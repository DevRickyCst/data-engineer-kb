import React, { useState } from 'react';
import {
  useFloating,
  useHover,
  useFocus,
  useDismiss,
  useRole,
  useInteractions,
  offset,
  flip,
  shift,
  autoUpdate,
  FloatingPortal,
  arrow,
  FloatingArrow,
} from '@floating-ui/react';
import Link from '@docusaurus/Link';
import glossary from '@site/src/glossary';

/**
 * <T> — inline glossary term with hover tooltip and link to the glossary page.
 *
 * Usage in MDX:
 *   <T>quorum</T>                        — looks up by children text (case-insensitive)
 *   <T term="quorum">a quorum write</T>  — explicit lookup key, custom display text
 *
 * Convention: wrap only the FIRST occurrence of a term per page. Don't wrap
 * inside headings, code blocks, or table cells where layout matters.
 */
export default function Term({ term, children }) {
  const lookupKey = (term ?? String(children ?? '')).toLowerCase().trim();
  const entry = glossary[lookupKey];

  if (!entry) {
    if (process.env.NODE_ENV !== 'production') {
      // eslint-disable-next-line no-console
      console.warn(
        `[Term] No glossary entry for "${lookupKey}". Add it to src/glossary.js or remove the <T> wrap.`,
      );
    }
    return <>{children}</>;
  }

  return <TermTooltip entry={entry} anchor={lookupKey}>{children}</TermTooltip>;
}

function TermTooltip({ entry, anchor, children }) {
  const [isOpen, setIsOpen] = useState(false);
  const arrowRef = React.useRef(null);

  const { refs, floatingStyles, context } = useFloating({
    open: isOpen,
    onOpenChange: setIsOpen,
    placement: 'top',
    middleware: [offset(8), flip(), shift({ padding: 8 }), arrow({ element: arrowRef })],
    whileElementsMounted: autoUpdate,
  });

  const hover = useHover(context, { move: false, delay: { open: 200, close: 100 } });
  const focus = useFocus(context);
  const dismiss = useDismiss(context);
  const role = useRole(context, { role: 'tooltip' });

  const { getReferenceProps, getFloatingProps } = useInteractions([hover, focus, dismiss, role]);

  const href = `/docs/glossary#${anchor}`;

  return (
    <>
      <Link
        ref={refs.setReference}
        to={href}
        {...getReferenceProps()}
        style={{
          color: 'inherit',
          textDecoration: 'none',
          borderBottom: '1px dotted var(--ifm-color-primary)',
          cursor: 'help',
          padding: '0 1px',
        }}
      >
        {children}
      </Link>
      {isOpen && (
        <FloatingPortal>
          <div
            ref={refs.setFloating}
            style={{
              ...floatingStyles,
              maxWidth: 320,
              padding: '0.75rem 0.9rem',
              fontSize: '0.875rem',
              lineHeight: 1.45,
              background: 'var(--ifm-background-surface-color)',
              color: 'var(--ifm-font-color-base)',
              border: '1px solid var(--ifm-color-emphasis-300)',
              borderRadius: 6,
              boxShadow: 'var(--ifm-global-shadow-md)',
              zIndex: 1000,
            }}
            {...getFloatingProps()}
          >
            <FloatingArrow
              ref={arrowRef}
              context={context}
              fill="var(--ifm-background-surface-color)"
              stroke="var(--ifm-color-emphasis-300)"
              strokeWidth={1}
            />
            <strong style={{ display: 'block', marginBottom: '0.35rem' }}>
              {entry.term}
            </strong>
            <div style={{ marginBottom: '0.5rem' }}>{entry.definition}</div>
            <Link
              to={href}
              style={{
                fontSize: '0.8125rem',
                color: 'var(--ifm-color-primary)',
                textDecoration: 'none',
              }}
            >
              Learn more →
            </Link>
          </div>
        </FloatingPortal>
      )}
    </>
  );
}
