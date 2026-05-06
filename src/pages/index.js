import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';

const FEATURES = [
  {
    title: '📚 Data Modeling',
    description:
      'Slowly Changing Dimensions, normalization, star/snowflake schemas — the building blocks of every warehouse.',
    to: '/docs/data-modeling/scd',
  },
  {
    title: '⚙️ Data Pipelines',
    description:
      'Orchestration with Airflow, streaming with Kafka — patterns, tradeoffs, and operational pitfalls.',
    to: '/docs/data-pipeline/airflow',
  },
  {
    title: '🎯 Interview Prep',
    description:
      'Real interview questions answered at Junior, Mid, and Senior levels — with pitfalls and follow-ups.',
    to: '/docs/interview/data-engineer',
  },
];

function Hero() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className="hero hero--data-eng">
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div style={{ marginTop: '1.5rem', display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          <Link className="button button--primary button--lg" to="/docs/intro">
            Start reading →
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/interview/data-engineer">
            Interview prep
          </Link>
        </div>
      </div>
    </header>
  );
}

function Features() {
  return (
    <section className="container">
      <div className="feature-grid">
        {FEATURES.map((f) => (
          <Link key={f.title} to={f.to} className="feature-card" style={{ color: 'inherit', textDecoration: 'none' }}>
            <h3>{f.title}</h3>
            <p>{f.description}</p>
          </Link>
        ))}
      </div>
    </section>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="A personal knowledge base on data engineering — concepts, pipelines, and interview prep."
    >
      <Hero />
      <main>
        <Features />
      </main>
    </Layout>
  );
}
