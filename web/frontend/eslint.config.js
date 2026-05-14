import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { defineConfig, globalIgnores } from 'eslint/config'

// ─── UI-discipline guardrails ────────────────────────────────────────────────
//
// These rules encode the architecture set up in refactor/ui-discipline:
//   1. components/ is flat — no subfolders, no deep paths.
//   2. Raw <input>/<select>/<textarea> are banned outside the primitive files
//      themselves; everyone else uses Input/Select/Textarea/Checkbox.
//   3. The top-level components/index.ts barrel is for external consumers
//      only — internal files must use direct file imports (prevents the
//      circular re-export bundle bloat we hit and fixed).
//
// Scope: only enforced inside src/components/. Outside that, rules don't fire.

const FORM_PRIMITIVE_FILES = [
  'src/components/Input.tsx',
  'src/components/Select.tsx',
  'src/components/Textarea.tsx',
  'src/components/Checkbox.tsx',
  'src/components/BadgeSelect.tsx',
]

export default defineConfig([
  globalIgnores(['dist']),

  // Base config — applies to all TS/TSX.
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
  },

  // ─── components/ guardrails ────────────────────────────────────────────────
  {
    files: ['src/components/**/*.{ts,tsx}'],
    ignores: FORM_PRIMITIVE_FILES,
    rules: {
      // 1) No raw form elements — use the primitives.
      'no-restricted-syntax': [
        'error',
        {
          selector: "JSXOpeningElement[name.name='input']",
          message:
            'Use <Input> / <Checkbox> from "./Input" / "./Checkbox" instead of raw <input>.',
        },
        {
          selector: "JSXOpeningElement[name.name='select']",
          message: 'Use <Select> from "./Select" instead of raw <select>.',
        },
        {
          selector: "JSXOpeningElement[name.name='textarea']",
          message: 'Use <Textarea> from "./Textarea" instead of raw <textarea>.',
        },
      ],
    },
  },

  // ─── Anti-barrel rules inside components/ ──────────────────────────────────
  {
    files: ['src/components/**/*.{ts,tsx}'],
    ignores: ['src/components/index.ts'],
    rules: {
      'no-restricted-imports': [
        'error',
        {
          paths: [
            {
              name: '.',
              message:
                'Do not import from the top-level components/ barrel inside components/. ' +
                'Use direct file imports (e.g. "./Card"). The barrel is for external consumers only.',
            },
            {
              name: '..',
              message:
                'Do not import from "../components" inside components/. ' +
                'Use direct sibling imports (e.g. "./Card").',
            },
          ],
          patterns: [
            {
              group: ['./*/*', '../components/*/*'],
              message:
                'components/ is flat — no subfolders. If you need a new file, place it directly in components/.',
            },
          ],
        },
      ],
    },
  },

  // ─── Outside components/: ban deep paths into the flat folder ─────────────
  {
    files: ['src/**/*.{ts,tsx}'],
    ignores: ['src/components/**'],
    rules: {
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: ['*/components/*/*', './components/*/*'],
              message:
                'components/ is flat — import either "./components/X" (direct) or "./components" (barrel).',
            },
          ],
        },
      ],
    },
  },
])
