import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { ErrorBoundary } from './components/ErrorBoundary'
import { ToastProvider } from './components/ui/Toast'
import { getQueryClient } from './lib/queryClient'
import App from './App'
import './index.css'

// ─── Initialize ──────────────────────────────────────────────────────────────

const queryClient = getQueryClient()
const rootElement = document.getElementById('root')

if (!rootElement) {
  throw new Error('Root element not found')
}

// ─── Render ──────────────────────────────────────────────────────────────────

createRoot(rootElement).render(
  <StrictMode>
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <ToastProvider>
          <App />
        </ToastProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  </StrictMode>
)
