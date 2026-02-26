/**
 * DeepL translation script.
 *
 * Reads src/locales/en.json (source of truth) and generates uk.json + ru.json
 * by sending only new/changed keys to DeepL Free API.
 *
 * Usage:
 *   DEEPL_API_KEY=xxx npx tsx scripts/translate.ts
 *
 * Requires DEEPL_API_KEY env var (free tier: 500K chars/month).
 */
import { readFileSync, writeFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const LOCALES_DIR = resolve(__dirname, '../src/locales')

const DEEPL_API_URL = 'https://api-free.deepl.com/v2/translate'
const DEEPL_API_KEY = process.env.DEEPL_API_KEY

// DeepL language codes
const TARGET_LANGS: Record<string, string> = {
  uk: 'UK',  // Ukrainian
  ru: 'RU',  // Russian
}

// Batch size for DeepL API (max 50 texts per request)
const BATCH_SIZE = 50

interface TranslationRecord {
  [key: string]: string
}

async function translateBatch(
  texts: string[],
  targetLang: string
): Promise<string[]> {
  const response = await fetch(DEEPL_API_URL, {
    method: 'POST',
    headers: {
      'Authorization': `DeepL-Auth-Key ${DEEPL_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      text: texts,
      source_lang: 'EN',
      target_lang: targetLang,
    }),
  })

  if (!response.ok) {
    const body = await response.text()
    throw new Error(`DeepL API error ${response.status}: ${body}`)
  }

  const data = await response.json() as { translations: { text: string }[] }
  return data.translations.map((t) => t.text)
}

function loadJson(path: string): TranslationRecord {
  try {
    return JSON.parse(readFileSync(path, 'utf-8'))
  } catch {
    return {}
  }
}

function saveJson(path: string, data: TranslationRecord): void {
  writeFileSync(path, JSON.stringify(data, null, 2) + '\n', 'utf-8')
}

async function translateFile(langCode: string, deeplLang: string): Promise<void> {
  const enPath = resolve(LOCALES_DIR, 'en.json')
  const targetPath = resolve(LOCALES_DIR, `${langCode}.json`)

  const en = loadJson(enPath)
  const existing = loadJson(targetPath)

  // Find keys that need translation (new or changed in English)
  const keysToTranslate: string[] = []
  for (const key of Object.keys(en)) {
    if (!existing[key] || existing[key] === en[key]) {
      // New key or still has English value (untranslated)
      keysToTranslate.push(key)
    }
  }

  // Remove keys no longer in English source
  const result: TranslationRecord = {}
  for (const key of Object.keys(en)) {
    if (keysToTranslate.includes(key)) {
      result[key] = en[key] // Will be replaced with translation
    } else {
      result[key] = existing[key] // Keep existing translation
    }
  }

  if (keysToTranslate.length === 0) {
    console.log(`  ${langCode}: No changes needed (${Object.keys(en).length} keys up to date)`)
    // Still save to ensure key order matches en.json
    saveJson(targetPath, result)
    return
  }

  console.log(`  ${langCode}: Translating ${keysToTranslate.length} keys...`)

  // Translate in batches
  const textsToTranslate = keysToTranslate.map((k) => en[k])
  const translations: string[] = []

  for (let i = 0; i < textsToTranslate.length; i += BATCH_SIZE) {
    const batch = textsToTranslate.slice(i, i + BATCH_SIZE)
    const batchNum = Math.floor(i / BATCH_SIZE) + 1
    const totalBatches = Math.ceil(textsToTranslate.length / BATCH_SIZE)
    console.log(`    Batch ${batchNum}/${totalBatches} (${batch.length} texts)...`)

    const translated = await translateBatch(batch, deeplLang)
    translations.push(...translated)

    // Rate limit: small delay between batches
    if (i + BATCH_SIZE < textsToTranslate.length) {
      await new Promise((r) => setTimeout(r, 200))
    }
  }

  // Merge translations
  for (let i = 0; i < keysToTranslate.length; i++) {
    result[keysToTranslate[i]] = translations[i]
  }

  saveJson(targetPath, result)
  console.log(`  ${langCode}: Done! ${keysToTranslate.length} translated, ${Object.keys(result).length} total keys`)
}

async function main(): Promise<void> {
  if (!DEEPL_API_KEY) {
    console.error('Error: DEEPL_API_KEY environment variable is required')
    console.error('Get a free key at: https://www.deepl.com/pro-api')
    process.exit(1)
  }

  console.log('Translating locales...\n')

  for (const [langCode, deeplLang] of Object.entries(TARGET_LANGS)) {
    await translateFile(langCode, deeplLang)
  }

  console.log('\nDone!')
}

main().catch((err) => {
  console.error('Translation failed:', err)
  process.exit(1)
})
