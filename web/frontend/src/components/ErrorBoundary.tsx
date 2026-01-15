import { Component, type ReactNode } from 'react'

interface Props {
  children: ReactNode
  fallback?: ReactNode
}

interface State {
  hasError: boolean
  error?: Error
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('React Error Boundary caught:', error, errorInfo)
  }

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback
      }

      return (
        <div className="min-h-screen bg-slate-900 flex items-center justify-center p-6">
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-6 max-w-lg">
            <h1 className="text-xl font-bold text-red-400 mb-2">Something went wrong</h1>
            <p className="text-slate-300 mb-4">
              The dashboard encountered an error. Please try refreshing the page.
            </p>
            {this.state.error && (
              <pre className="bg-slate-800 p-3 rounded text-xs text-slate-400 overflow-auto">
                {this.state.error.message}
              </pre>
            )}
            <button
              onClick={() => window.location.reload()}
              className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
            >
              Refresh Page
            </button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
