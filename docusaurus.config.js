// @ts-check
// Docusaurus config — see https://docusaurus.io/docs/api/docusaurus-config

const { themes } = require('prism-react-renderer');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Data Engineer Knowledge Base',
  tagline: 'Concepts, pipelines, and interview prep — all in one place.',
  favicon: 'img/favicon.svg',

  url: 'https://devrickycst.github.io',
  baseUrl: '/data-engineer-kb/',

  organizationName: 'DevRickyCst',
  projectName: 'data-engineer-kb',
  trailingSlash: false,

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // Mermaid diagrams support
  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          routeBasePath: 'docs',
          editUrl: undefined,
        },
        blog: {
          showReadingTime: true,
          blogTitle: 'Notes',
          blogDescription: 'Short-form data-engineering notes',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/social-card.png',
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      docs: {
        sidebar: {
          hideable: true,
          autoCollapseCategories: false,
        },
      },
      navbar: {
        title: 'Data Eng KB',
        logo: {
          alt: 'Data Engineer Knowledge Base',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'docSidebar',
            sidebarId: 'interviewSidebar',
            position: 'left',
            label: 'Interview',
          },
          { to: '/blog', label: 'Notes', position: 'left' },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Data Modeling', to: '/docs/data-modeling/scd' },
              { label: 'Data Pipeline', to: '/docs/data-pipeline/airflow' },
            ],
          },
          {
            title: 'Interview',
            items: [
              { label: 'Data Engineer', to: '/docs/interview/data-engineer' },
              { label: 'System Design', to: '/docs/interview/system-design' },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} — Built with Docusaurus.`,
      },
      prism: {
        theme: themes.github,
        darkTheme: themes.dracula,
        additionalLanguages: ['sql', 'bash', 'python', 'yaml', 'json'],
      },
      mermaid: {
        theme: { light: 'neutral', dark: 'dark' },
      },
      // Local search via plugin can be added later; for now Algolia-ready stub.
    }),
};

module.exports = config;
