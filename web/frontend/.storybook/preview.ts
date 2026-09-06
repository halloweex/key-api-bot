import type { Preview } from '@storybook/react-vite'

// Same bootstrap order as src/main.tsx: i18n first, then global styles.
import '../src/lib/i18n'
import '../src/index.css'

const preview: Preview = {
  parameters: {
    layout: 'padded',
    backgrounds: {
      options: {
        app: { name: 'App (slate-50)', value: '#f8fafc' },
        white: { name: 'White', value: '#ffffff' },
      },
    },
  },
  initialGlobals: {
    backgrounds: { value: 'app' },
  },
}

export default preview
