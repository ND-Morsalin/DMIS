/**
 * scrape_medex_brands_fast.js
 *
 * - Fetches MedEx brand index pages in batches (concurrent, default 5 pages at a time).
 * - Retries each page up to MAX_RETRIES on failure (default 3).
 * - Appends parsed entries for each batch to the output file in one write (fast).
 * - Minimal console output (per-batch summary). Prints failed/skipped pages at end.
 *
 * Dependencies:
 *   npm install axios cheerio
 *
 * Usage:
 *   node scrape_medex_brands_fast.js
 */

const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs").promises;
const path = require("path");

const BASE = "https://medex.com.bd/brands";
const ORIGIN = "https://medex.com.bd";
const OUTFILE = path.resolve(process.cwd(), "bangladesh_medicines_brands_v1.json");
const HEADERS = { "User-Agent": "Mozilla/5.0 (compatible; MedExScraperFast/1.0)" };

// Config
const MAX_PAGES = 900;       // heuristic upper bound (tune if you know exact)
const BATCH_SIZE = 5;        // fetch 5 pages concurrently as you requested
const MAX_RETRIES = 3;       // retry each page up to 3 times
const RETRY_DELAY_MS = 1000; // wait 1s between retries
const BATCH_DELAY_MS = 400;  // small delay between batches

// small helper
function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

async function initOutputFile() {
  try {
    await fs.writeFile(OUTFILE, JSON.stringify([], null, 2), "utf8");
    console.log(`Initialized output file: ${OUTFILE}`);
  } catch (err) {
    console.error("Failed to initialize output file:", err);
    throw err;
  }
}

async function fetchPageRaw(page) {
  const url = page > 1 ? `${BASE}?page=${page}` : BASE;
  const resp = await axios.get(url, { headers: HEADERS, timeout: 20000 });
  return { html: resp.data, finalUrl: resp.request?.res?.responseUrl || url };
}

// retry wrapper
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
      // small backoff
      await sleep(RETRY_DELAY_MS);
    }
  }
  return { success: false, page, error: lastErr ? (lastErr.message || String(lastErr)) : "unknown" };
}

// parse function (same structure-aware parser as before)
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
    const cols = $dataRow.find(".col-xs-12").toArray().map(el => $(el));
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

    out.push({ name, strength, generic, company, medicine_type, source_url });
  });

  return out;
}

// append batch array to output file (single read/write per batch)
async function appendBatchToOutput(items) {
  try {
    let arr = [];
    try {
      const txt = await fs.readFile(OUTFILE, "utf8");
      arr = JSON.parse(txt || "[]");
      if (!Array.isArray(arr)) arr = [];
    } catch (err) {
      arr = [];
    }
    // push items
    for (const it of items) arr.push(it);
    await fs.writeFile(OUTFILE, JSON.stringify(arr, null, 2), "utf8");
  } catch (err) {
    throw err;
  }
}

(async function main() {
  console.log("Starting fast MedEx brand scraper (batch concurrency)...");
  await initOutputFile();

  const failedPages = []; // pages that failed after retries
  let totalSaved = 0;

  for (let page = 1; page <= MAX_PAGES; page += BATCH_SIZE) {
    const batchPages = [];
    for (let p = page; p < page + BATCH_SIZE && p <= MAX_PAGES; p++) batchPages.push(p);

    // start concurrent fetches with retries
    const fetchPromises = batchPages.map(p => fetchWithRetries(p, MAX_RETRIES));
    const results = await Promise.all(fetchPromises);

    // collect parsed items for successful pages
    let batchItems = [];
    let batchFailures = [];

    for (const res of results) {
      if (!res) continue;
      if (!res.success) {
        // failed after retries
        batchFailures.push({ page: res.page, error: res.error || "unknown" });
        continue;
      }
      try {
        const parsed = parsePageByStructure(res.html, res.finalUrl);
        // add metadata: page
        parsed.forEach(it => it._page = res.page);
        batchItems = batchItems.concat(parsed);
      } catch (err) {
        // parsing failure - treat as failed page to retry next time? For now mark as skipped
        batchFailures.push({ page: res.page, error: "parse_error: " + (err.message || String(err)) });
      }
    }

    // append parsed batchItems to file (one write)
    if (batchItems.length > 0) {
      try {
        await appendBatchToOutput(batchItems);
        totalSaved += batchItems.length;
      } catch (err) {
        console.error("Failed to append batch to output file:", err);
        // if append fails, treat pages as failed (so user can retry)
        for (const p of batchPages) {
          if (!batchFailures.some(bf => bf.page === p)) batchFailures.push({ page: p, error: "append_failed: " + (err.message || String(err)) });
        }
      }
    }

    // merge failures into global failedPages
    for (const f of batchFailures) failedPages.push(f.page);

    // batch summary (minimal output)
    console.log(`Pages ${batchPages[0]}..${batchPages[batchPages.length-1]} processed — saved ${batchItems.length} items, failed ${batchFailures.length} page(s).`);

    // small delay between batches
    await sleep(BATCH_DELAY_MS);
  }

  // Remove duplicates in failedPages and sort
  const failedUnique = Array.from(new Set(failedPages)).sort((a,b) => a - b);

  console.log("Finished scraping.");
  console.log(`Total_items_saved: ${totalSaved}`);
  if (failedUnique.length) {
    console.log("Pages failed and skipped after retries:", failedUnique);
  } else {
    console.log("No pages failed after retries.");
  }
})();
