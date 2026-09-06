import type { StorybookConfig } from '@storybook/react-vite'

// Storybook picks up the same Vite pipeline the app uses (Tailwind 4 via
// PostCSS), so components render with the exact production styles.
const config: StorybookConfig = {
  framework: '@storybook/react-vite',
  stories: ['../src/**/*.stories.@(ts|tsx)'],
}

export default config
