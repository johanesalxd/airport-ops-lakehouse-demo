# Workshop deck

`airport-ops-lakehouse.md` is a [Marp](https://marp.app/) deck (Markdown). Diagrams
in `assets/` are authored as Mermaid (`*.mmd`) and pre-rendered to SVG (`*.svg`) so
the deck embeds static images (no live Mermaid dependency at present time).

```
airport-ops-lakehouse.md     # the deck (edit this)
assets/
  *.mmd                       # Mermaid sources (edit these for diagrams)
  *.svg                       # rendered output embedded by the deck
```

## Toolchain

Node-based (separate from the Python/`uv` setup used by the pipeline scripts):

- **Node + `npx`** (no global install needed; `npx` fetches the tools).
- **[Marp CLI](https://github.com/marp-team/marp-cli)** — `@marp-team/marp-cli` (deck → PDF/HTML).
- **[Mermaid CLI](https://github.com/mermaid-js/mermaid-cli)** — `@mermaid-js/mermaid-cli` (`mmdc`, `.mmd` → `.svg`).
- **A Chromium/Chrome** for both tools (Puppeteer downloads one on first use).

On a minimal Linux box, Chrome needs these shared libraries:

```bash
sudo apt-get update && sudo apt-get install -y \
  libxkbcommon0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
  libdrm2 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 \
  libpango-1.0-0 libcairo2 libasound2
```

Point the tools at the Chrome binary (adjust the path to your install):

```bash
export CHROME_PATH="$(ls "$HOME"/.cache/puppeteer/chrome/*/chrome-linux64/chrome | head -1)"
```

## Re-render a diagram (after editing a `.mmd`)

`mmdc` needs a Puppeteer config so Chrome launches without a sandbox in CI/containers:

```bash
cat > /tmp/pptr.json <<JSON
{"executablePath": "${CHROME_PATH}", "args": ["--no-sandbox", "--disable-setuid-sandbox"]}
JSON

npx -p @mermaid-js/mermaid-cli mmdc \
  -i assets/dag-stages.mmd -o assets/dag-stages.svg -p /tmp/pptr.json
```

Repeat for each edited diagram (`architecture.mmd`, `medallion.mmd`,
`transform-vs-semantic.mmd`, `dag-stages.mmd`). Commit the regenerated `.svg`.

## Build the deck (PDF)

```bash
CHROME_PATH="${CHROME_PATH}" npx @marp-team/marp-cli \
  airport-ops-lakehouse.md --pdf --allow-local-files -o /tmp/airport-ops-lakehouse.pdf
```

`--allow-local-files` is required so the deck can embed the `assets/*.svg`. Drop
`--pdf` (or use `--html`) for an HTML export. PNG multi-image export is known to be
flaky in headless mode — prefer PDF/HTML.

## Workflow

1. Edit `airport-ops-lakehouse.md` (and/or an `assets/*.mmd`).
2. If a diagram changed, **re-render its SVG** (above).
3. **Rebuild the PDF** to sanity-check it renders.
4. Commit the `.md`, any changed `.mmd`, and the regenerated `.svg`.
