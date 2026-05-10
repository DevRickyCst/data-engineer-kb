// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Fundamentals',
      collapsed: false,
      items: [
        'fundamentals/olap-vs-oltp',
        'fundamentals/distributed-systems',
        'fundamentals/batch-vs-streaming',
      ],
    },
    {
      type: 'category',
      label: 'Data Modeling',
      collapsed: false,
      items: ['data-modeling/scd', 'data-modeling/normalization', 'data-modeling/star-vs-snowflake'],
    },
    {
      type: 'category',
      label: 'Storage',
      collapsed: false,
      items: ['storage/parquet-and-formats'],
    },
    {
      type: 'category',
      label: 'Lakehouse',
      collapsed: false,
      items: [
        'lakehouse/table-formats-comparison',
        'lakehouse/iceberg',
        'lakehouse/delta-lake',
        'lakehouse/hudi',
      ],
    },
    {
      type: 'category',
      label: 'Data Pipeline',
      collapsed: false,
      items: [
        'data-pipeline/airflow',
        {
          type: 'category',
          label: 'dbt',
          collapsed: true,
          items: [
            'data-pipeline/dbt/fundamentals',
            'data-pipeline/dbt/advanced',
          ],
        },
        'data-pipeline/kafka',
        'data-pipeline/cdc',
      ],
    },
    {
      type: 'category',
      label: 'Quality & Reliability',
      collapsed: false,
      items: ['quality/idempotency-and-backfills'],
    },
  ],

  interviewSidebar: [
    {
      type: 'category',
      label: 'Interview Prep',
      collapsed: false,
      items: ['interview/data-engineer', 'interview/system-design'],
    },
  ],
};

module.exports = sidebars;
