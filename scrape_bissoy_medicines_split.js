const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs").promises;
const path = require("path");

const BASE = "https://www.bissoy.com/medicines";
const ORIGIN = "https://www.bissoy.com";
const OUT_DIR = path.resolve(process.cwd(), "bissoy_medicine_split");

const HEADERS = { "User-Agent": "Mozilla/5.0 (compatible; BissoyScraperSplit/1.0)" };

// Config (tuned for bissoy.com)
const MAX_PAGES = 400;      // total pages to attempt
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

// parsing logic - extract name_en, name_bn, source_url, company, price, generic, strength
function parsePageByStructure(html, pageUrl) {
  const $ = cheerio.load(html);
  const out = [];

  $("ul.space-y-6 > li").each((i, li) => {
    const $li = $(li);

    // source_url
    const $a = $li.find("a[href^='/medicine/']").first();
    const href = $a.attr("href") || "";
    const source_url = href.startsWith("http") ? href : ORIGIN + href;
    if (!source_url || source_url === ORIGIN) return;

    // name_en and name_bn from h3 > a
    const $nameLink = $li.find("h3.text-xl.font-bold > a");
    const nameText = $nameLink.text().trim();
    const [name_en, name_bn] = nameText.split(" | ").map(s => s.trim());
    if (!name_en) return;

    // Extract <p> elements for other fields
    const $paragraphs = $li.find("div > p");
    const pArray = $paragraphs.toArray().map(p => $(p).text().trim());

    // strength (first <p>)
    let strength = pArray[0] || null;
    if (strength === "") strength = null;

    // generic (second <p>)
    let generic = pArray[1] || null;
    if (generic === "") generic = null;

    // company (third <p>)
    let company = pArray[2] || null;
    if (company === "") company = null;

    // price (fourth <p>)
    let price = pArray[3] || null;
    if (price === "") price = null;

    out.push({
      name_en,
      name_bn: name_bn || null,
      source_url,
      company,
      price,
      generic,
      strength,
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
        // Add origin page number to each item for traceability
        for (const it of parsed) {
          it._source_page = res.page;
          batchItems.push(it);
        }
      } catch (err) {
        batchFailed.push({ page: res.page, error: "parse_error: " + (err.message || String(err)) });
      }
    }

    // write batch file even if some pages failed
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