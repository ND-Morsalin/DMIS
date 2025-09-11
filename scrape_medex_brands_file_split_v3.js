/**
 * scrape_medex_brands_split.js
 *
 * - Fetches MedEx brand index pages in batches (default 5 pages at a time).
 * - Retries each page up to MAX_RETRIES (default 3).
 * - Writes each batch's results to a separate file:
 *     medicine_split/page_{start}_to_{end}.json
 *   Each file contains an array of brand objects for those pages.
 * - If a batch file already exists and is non-empty, that batch is skipped (resume-safe).
 * - Minimal console output and a final list of skipped pages.
 *
 * Dependencies:
 *   npm install axios cheerio
 *
 * Usage:
 *   node scrape_medex_brands_split.js
 */

const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs").promises;
const path = require("path");

const BASE = "https://medex.com.bd/brands";
const ORIGIN = "https://medex.com.bd";
const OUT_DIR = path.resolve(process.cwd(), "medicine_split");

const HEADERS = { "User-Agent": "Mozilla/5.0 (compatible; MedExScraperSplit/1.0)" };

// Config (tune if needed)
const MAX_PAGES = 900;      // total pages to attempt
const BATCH_SIZE = 5;       // pages per output file
const MAX_RETRIES = 3;      // per-page retries
const RETRY_DELAY_MS = 1000;
const BATCH_DELAY_MS = 300; // delay between batches

// small helper
function sleep(ms) { return new Promise((res) => setTimeout(res, ms)); }

async function ensureOutDir() {
  try {
    await fs.mkdir(OUT_DIR, { recursive: true });
  } catch (err) {
    // ignore
  }
}

async function fileExistsNonEmpty(filePath) {
  try {
    const st = await fs.stat(filePath);
    return st.size > 0;
  } catch (err) {
    return false;
  }
}

async function fetchPageRaw(page) {
  const url = page > 1 ? `${BASE}?page=${page}` : BASE;
  const resp = await axios.get(url, { headers: HEADERS, timeout: 20000 });
  return { html: resp.data, finalUrl: resp.request?.res?.responseUrl || url };
}

async function fetchWithRetries(page, maxRetries = MAX_RETRIES) {
  let attempt = 0;
  let lastErr = null;
  while (attempt < maxRetries) {
    attempt++;
    try {
      const res = await fetchPageRaw(page);
      return { success: true, page, ...res };
    } catch (err) {
      lastErr = err;
      await sleep(RETRY_DELAY_MS);
    }
  }
  return { success: false, page, error: lastErr ? (lastErr.message || String(lastErr)) : "unknown" };
}

// parsing logic (structure-aware) - same fields: name, strength, generic, company, medicine_type, source_url
function parsePageByStructure(html, pageUrl) {
  const $ = cheerio.load(html);
  const out = [];

  $("a.hoverable-block").each((i, a) => {
    const $a = $(a);
    const href = $a.attr("href") || "";
    const source_url = href.startsWith("http") ? href : ORIGIN + href;

    const $dataRow = $a.find(".data-row");
    if ($dataRow.length === 0) return;

    // medicine_type from dosage-icon img (alt/title)
    let medicine_type = null;
    const $img = $dataRow.find(".md-icon-container img.dosage-icon").first();
    if ($img && $img.length) {
      medicine_type = ($img.attr("alt") || $img.attr("title") || "").trim() || null;
    } else {
      const $img2 = $dataRow.find(".md-icon-container img").first();
      if ($img2 && $img2.length) medicine_type = ($img2.attr("alt") || $img2.attr("title") || "").trim() || null;
    }

    // name: remove child icon elements (span/img) before reading text
    let name = $dataRow.find(".data-row-top").clone().children().remove().end().text().trim();
    if (!name) name = $dataRow.find(".data-row-top").text().trim();

    // strength
    let strength = $dataRow.find(".data-row-strength .grey-ligten").text().trim();
    if (!strength) strength = $dataRow.find(".data-row-strength").text().trim();
    if (strength === "") strength = null;

    // generic
    let generic = null;
    const cols = $dataRow.find(".col-xs-12").toArray().map((el) => $(el));
    if (cols.length >= 3) {
      const candidate = cols[2].text().trim();
      if (candidate && !cols[2].find(".data-row-company").length) generic = candidate;
    }
    if (!generic) {
      for (const $col of cols) {
        const txt = $col.text().trim();
        if (!txt) continue;
        if (txt === name) continue;
        if (txt === strength) continue;
        if ($col.find(".data-row-company").length) continue;
        if ($col.is(".data-row-top") || $col.is(".data-row-strength")) continue;
        generic = txt;
        break;
      }
    }
    if (generic === "") generic = null;

    // company
    let company = $dataRow.find(".data-row-company").text().trim();
    if (!company) {
      if (cols.length) {
        const lastText = cols[cols.length - 1].text().trim();
        if (/(Ltd|Limited|PLC|Pharma|Laboratories|Ltd\.)/i.test(lastText)) company = lastText;
      }
    }
    if (company === "") company = null;

    if (!name) return;

    out.push({
      name,
      strength,
      generic,
      company,
      medicine_type,
      source_url,
    });
  });

  return out;
}

// write batch output to file: page_{start}_to_{end}.json
async function writeBatchFile(startPage, endPage, items) {
  const fname = `page_${startPage}_to_${endPage}.json`;
  const fpath = path.join(OUT_DIR, fname);
  await fs.writeFile(fpath, JSON.stringify(items, null, 2), "utf8");
  return fpath;
}

(async function main() {
  await ensureOutDir();
  const failedPages = [];
  let totalSaved = 0;

  for (let start = 1; start <= MAX_PAGES; start += BATCH_SIZE) {
    const end = Math.min(start + BATCH_SIZE - 1, MAX_PAGES);
    const outFileName = path.join(OUT_DIR, `page_${start}_to_${end}.json`);

    // resume-safety: skip if file already exists & non-empty
    if (await fileExistsNonEmpty(outFileName)) {
      console.log(`Skipping existing file for pages ${start}..${end}`);
      continue;
    }

    // create list of pages for this batch
    const pages = [];
    for (let p = start; p <= end; p++) pages.push(p);

    // fetch concurrently with retries
    const fetchPromises = pages.map((p) => fetchWithRetries(p, MAX_RETRIES));
    const results = await Promise.all(fetchPromises);

    const batchItems = [];
    const batchFailed = [];

    for (const res of results) {
      if (!res) continue;
      if (!res.success) {
        batchFailed.push({ page: res.page, error: res.error || "unknown" });
        continue;
      }
      try {
        const parsed = parsePageByStructure(res.html, res.finalUrl);
        // Optionally add origin page number to each item for traceability
        for (const it of parsed) {
          it._source_page = res.page;
          batchItems.push(it);
        }
      } catch (err) {
        batchFailed.push({ page: res.page, error: "parse_error: " + (err.message || String(err)) });
      }
    }

    // write batch file even if some pages failed (it will contain items from successful pages)
    try {
      await writeBatchFile(start, end, batchItems);
      totalSaved += batchItems.length;
    } catch (err) {
      // if write fails, mark all pages in batch as failed
      for (const p of pages) {
        if (!batchFailed.some(b => b.page === p)) batchFailed.push({ page: p, error: "write_error: " + (err.message || String(err)) });
      }
    }

    // record failed page numbers
    for (const bf of batchFailed) failedPages.push(bf.page);

    // Minimal per-batch log
    console.log(`Batch pages ${start}..${end}: saved ${batchItems.length} items, failed ${batchFailed.length} page(s).`);

    await sleep(BATCH_DELAY_MS);
  }

  const failedUnique = Array.from(new Set(failedPages)).sort((a, b) => a - b);
  console.log("Done.");
  console.log(`Total items saved across files: ${totalSaved}`);
  if (failedUnique.length) {
    console.log("Pages failed and skipped after retries:", failedUnique);
  } else {
    console.log("No pages failed after retries.");
  }
})();
