/**
 * scrape_medex_brands.js
 *
 * Node.js scraper for MedEx Bangladesh "brands" index.
 * Parses HTML structure you provided (hoverable-block / data-row-top / data-row-strength / data-row-company).
 *
 * Dependencies:
 *   npm install axios cheerio
 *
 * Usage:
 *   node scrape_medex_brands.js
 *
 * Output:
 *   bangladesh_medicines_brands.json
 */

const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs").promises;
const path = require("path");

const BASE = "https://medex.com.bd/brands";
const ORIGIN = "https://medex.com.bd";
const OUTFILE = path.resolve(
  process.cwd(),
  "bangladesh_medicines_brands_v1.json"
);
const HEADERS = {
  "User-Agent": "Mozilla/5.0 (compatible; MedExScraperJS/1.0)",
};

const MAX_PAGES = 900; // heuristic upper bound
const CONCURRENCY = 3; // parallel fetches per batch
const REQUEST_DELAY_MS = 350; // polite delay (ms)

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function fetchPage(page) {
  const url = page > 1 ? `${BASE}?page=${page}` : BASE;
  try {
    const r = await axios.get(url, { headers: HEADERS, timeout: 20000 });
    return { html: r.data, url: r.request.res.responseUrl || url };
  } catch (err) {
    console.error(`Failed fetching page ${page}:`, err.message || err);
    return null;
  }
}

/**
<a href="https://medex.com.bd/brands/3039/a-card-20-mg-tablet" class="hoverable-block">

  <div class="row data-row">

    <div class="col-xs-12 data-row-top">
      <span class="md-icon-container">
        <img src="https://medex.com.bd/img/dosage-forms/tablet.png" alt="Tablet" title="Tablet" class="dosage-icon"/>
      </span> 
      A-Card
    </div>
    <div class="col-xs-12 data-row-strength">
      <span class="grey-ligten">20 mg</span>
    </div>
    <div class="col-xs-12">
      Isosorbide Mononitrate
    </div>
    <div class="col-xs-12">
      <span class="data-row-company">ACME Laboratories Ltd.</span>
    </div>
  </div>

</a>
 */
function parsePageByStructure(html, pageUrl) {
  const $ = cheerio.load(html);
  const out = [];

  $("a.hoverable-block").each((i, a) => {
    const $a = $(a);
    const href = $a.attr("href") || "";
    const source_url = href.startsWith("http") ? href : ORIGIN + href;

    const $dataRow = $a.find(".data-row");
    if ($dataRow.length === 0) return;

    // name: remove child icon elements (span/img) before reading text
    let name = $dataRow
      .find(".data-row-top")
      .clone()
      .children()
      .remove()
      .end()
      .text()
      .trim();
    if (!name) {
      // fallback: text of .data-row-top
      name = $dataRow.find(".data-row-top").text().trim();
    }

    // strength
    let strength = $dataRow
      .find(".data-row-strength .grey-ligten")
      .text()
      .trim();
    if (!strength) {
      // sometimes the strength might be direct text inside data-row-strength
      strength = $dataRow.find(".data-row-strength").text().trim();
    }
    if (strength === "") strength = null;

    // generic: per structure it's the next .col-xs-12 after strength (index-based)
    // We'll attempt indexing but also fallback to selecting the first .col-xs-12 that isn't top/strength/company
    let generic = null;
    const cols = $dataRow
      .find(".col-xs-12")
      .toArray()
      .map((el) => $(el));
    // typical order: [0]=top, [1]=strength, [2]=generic, [3]=company
    if (cols.length >= 3) {
      // choose index 2 if it's not the company span container
      const candidate = cols[2].text().trim();
      if (candidate && !cols[2].find(".data-row-company").length)
        generic = candidate;
    }
    if (!generic) {
      // fallback: pick first .col-xs-12 whose text isn't name/strength/company
      for (const $col of cols) {
        const txt = $col.text().trim();
        if (!txt) continue;
        if (txt === name) continue;
        if (txt === strength) continue;
        if ($col.find(".data-row-company").length) continue;
        // also skip the data-row-top and data-row-strength elements explicitly
        if ($col.is(".data-row-top") || $col.is(".data-row-strength")) continue;
        generic = txt;
        break;
      }
    }
    if (generic === "") generic = null;

    // company
    let company = $dataRow.find(".data-row-company").text().trim();
    if (!company) {
      // fallback: last col-xs-12 text if it contains company-like keywords
      if (cols.length) {
        const lastText = cols[cols.length - 1].text().trim();
        if (/(Ltd|Limited|PLC|Pharma|Laboratories|Ltd\.)/i.test(lastText))
          company = lastText;
      }
    }
    if (company === "") company = null;

    // ensure at least a name exists
    if (!name) return;

    out.push({
      name,
      strength,
      generic,
      company,
      source_url,
    });
  });

  return out;
}

(async function main() {
  console.log("Starting MedEx brand scraper (structure-aware)...");
  const all = [];
  let consecutiveEmptyBatches = 0;

  for (let page = 1; page <= MAX_PAGES; page += CONCURRENCY) {
    const pages = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const p = page + i;
      if (p > MAX_PAGES) break;
      pages.push(p);
    }

    const batchPromises = pages.map((p, idx) =>
      (async () => {
        await sleep(REQUEST_DELAY_MS * idx);
        const res = await fetchPage(p);
        if (!res) return { page: p, entries: [] };
        const parsed = parsePageByStructure(res.html, res.url);
        return { page: p, entries: parsed };
      })()
    );

    const results = await Promise.all(batchPromises);
    let anyFound = false;
    for (const r of results) {
      if (!r) continue;
      if (r.entries && r.entries.length > 0) {
        anyFound = true;
        for (const e of r.entries) all.push(e);
      }
    }

    if (!anyFound) consecutiveEmptyBatches += 1;
    else consecutiveEmptyBatches = 0;

    // heuristic stop
    if (consecutiveEmptyBatches >= 4) {
      console.log("No more content detected (heuristic). Stopping.");
      break;
    }

    console.log(
      `Fetched pages ${page}..${Math.min(
        page + CONCURRENCY - 1,
        MAX_PAGES
      )} — collected items so far: ${all.length}`
    );
    await sleep(REQUEST_DELAY_MS);
  }

  // dedupe by name + company
  const map = new Map();
  for (const e of all) {
    const key = ((e.name || "") + "||" + (e.company || ""))
      .toLowerCase()
      .trim();
    if (!map.has(key)) map.set(key, e);
  }
  const final = Array.from(map.values());

  try {
    await fs.writeFile(OUTFILE, JSON.stringify(final, null, 2), "utf8");
    console.log(`Wrote ${final.length} unique entries to ${OUTFILE}`);
  } catch (err) {
    console.error("Write failed:", err);
  }
})();
