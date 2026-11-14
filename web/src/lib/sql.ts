export type TokenKind = 'keyword' | 'string' | 'number' | 'comment' | 'text'

export interface Token {
  kind: TokenKind
  text: string
}

const KEYWORDS = new Set(
  (
    'select from where group by order having limit offset distinct as with join inner left right full outer cross on using ' +
    'union all intersect except and or not in is null like ilike between case when then else end asc desc over partition ' +
    'filter true false cast exists any some unnest similar to'
  ).split(' '),
)

/**
 * Splits SQL into display tokens. Presentation only: the server parses and
 * validates every statement; nothing here decides what is allowed.
 * Concatenating the token text always reproduces the input exactly.
 */
export function tokenize(sql: string): Token[] {
  const out: Token[] = []
  const push = (kind: TokenKind, text: string) => {
    if (!text) return
    const last = out[out.length - 1]
    if (last && last.kind === kind && kind === 'text') last.text += text
    else out.push({ kind, text })
  }
  let i = 0
  while (i < sql.length) {
    const c = sql[i]
    if (c === '-' && sql[i + 1] === '-') {
      let j = i
      while (j < sql.length && sql[j] !== '\n') j++
      push('comment', sql.slice(i, j))
      i = j
    } else if (c === '/' && sql[i + 1] === '*') {
      const end = sql.indexOf('*/', i + 2)
      const j = end < 0 ? sql.length : end + 2
      push('comment', sql.slice(i, j))
      i = j
    } else if (c === "'" || c === '"') {
      let j = i + 1
      while (j < sql.length) {
        if (sql[j] === c) {
          if (sql[j + 1] === c) j += 2
          else {
            j++
            break
          }
        } else j++
      }
      push(c === "'" ? 'string' : 'text', sql.slice(i, j))
      i = j
    } else if (/[0-9]/.test(c)) {
      let j = i
      while (j < sql.length && /[0-9._]/.test(sql[j])) j++
      push('number', sql.slice(i, j))
      i = j
    } else if (/[A-Za-z_]/.test(c)) {
      let j = i
      while (j < sql.length && /[A-Za-z0-9_]/.test(sql[j])) j++
      const word = sql.slice(i, j)
      push(KEYWORDS.has(word.toLowerCase()) ? 'keyword' : 'text', word)
      i = j
    } else {
      push('text', c)
      i++
    }
  }
  return out
}

/** Name to use in FROM for a dataset: the dataset name is already lowercase [a-z0-9-], so hyphens need quoting. */
export function relationRef(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name}"`
}
