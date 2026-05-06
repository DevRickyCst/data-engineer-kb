// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Data Modeling',
      collapsed: false,
      items: ['data-modeling/scd', 'data-modeling/normalization'],
    },
    {
      type: 'category',
      label: 'Storage / Lakehouse',
      collapsed: false,
      items: ['storage/parquet-and-formats', 'storage/iceberg-vs-delta'],
    },
    {
      type: 'category',
      label: 'Data Pipeline',
      collapsed: false,
      items: [
        'data-pipeline/airflow',
        'data-pipeline/dbt-fundamentals',
        'data-pipeline/dbt-advanced',
        'data-pipeline/kafka',
        'data-pipeline/cdc',
      ],
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
