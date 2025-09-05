/**
 * scrape_medex_brands.js
 *
 * Node.js scraper for MedEx Bangladesh "brands" index.
 *
 * Dependencies:
 *   npm install axios cheerio p-limit
 *
 * Usage:
 *   node scrape_medex_brands.js
 *
 * Output:
 *   bangladesh_medicines_brands.json
 *
 * NOTE: This uses heuristics to parse brand/manufacturer/generic from visible text.
 * Adjust selectors if the MedEx HTML structure changes. Respect robots.txt and TOS.
 */

const axios = require("axios");
const cheerio = require("cheerio");
const pLimit = require("p-limit").default || require("p-limit");
const fs = require("fs").promises;
const path = require("path");

const BASE = "https://medex.com.bd/brands";
const OUTFILE = path.resolve(process.cwd(), "bangladesh_medicines_brands.json");
const HEADERS = { "User-Agent": "Mozilla/5.0 (compatible; MedExScraperJS/1.0)" };

const MAX_PAGES = 900;       // heuristic upper bound
const CONCURRENCY = 3;       // parallel fetches
const REQUEST_DELAY_MS = 500; // polite delay between requests (per concurrent slot)

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

/**
 * Parse a page HTML and return an array of raw lines that likely correspond to brands.
 * We try several selectors and fall back to scanning anchor and list elements.
 */
function parsePage(html, url) {
  const $ = cheerio.load(html);
  const candidates = new Set();

  // Try a few container selectors that commonly hold lists:
  const selectors = [
    ".brand-list", ".brands-list", ".list-group", "#brandList", "ul.list-unstyled",
    ".content", ".main", ".container"
  ];

  for (const sel of selectors) {
    $(sel).find("li, a, p, div").each((i, el) => {
      const text = $(el).text().trim();
      if (text && text.length < 350 && /[A-Za-z০-৯0-9]/.test(text)) {
        candidates.add(text.replace(/\s+/g, " "));
      }
    });
    if (candidates.size > 20) break; // likely found the correct container
  }

  // If not many candidates found, fall back to scanning all anchors / list items
  if (candidates.size < 20) {
    $("a, li, p").each((i, el) => {
      const text = $(el).text().trim();
      if (!text) return;
      // filter out navigation / copyright etc by heuristics
      if (text.length > 300) return;
      if (/^(home|about|contact|privacy|terms)$/i.test(text)) return;
      if (!/[A-Za-z০-৯0-9]/.test(text)) return;
      candidates.add(text.replace(/\s+/g, " "));
    });
  }

  // return as array
  return Array.from(candidates).map((t) => ({ raw: t, source: url }));
}

/**
 * Normalize a raw text line into { brand, generic, manufacturer, source_url } using heuristics.
 */
function normalizeEntry(rawText, sourceUrl) {
  const manufacturersKeywords = ["Ltd", "Ltd.", "Limited", "PLC", "LLP", "Corporation", "Corp", "Co", "Co.", "Company", "Pharma", "Pharmaceuticals", "Pharmaceutical", "BD", "Bangladesh", "Ltd,"];

  // Remove repeated spaces, weird separators
  const text = rawText.replace(/\s+/g, " ").trim();

  // Attempt to detect manufacturer by scanning from the end
  const tokens = text.split(" ");
  let manuStart = -1;
  for (let i = tokens.length - 1; i >= 0; i--) {
    const t = tokens[i].replace(/[,\.]$/, "");
    if (manufacturersKeywords.some((kw) => t.toLowerCase().includes(kw.toLowerCase()))) {
      manuStart = i;
      break;
    }
  }

  let manufacturer = null;
  let brandAndMaybeGeneric = text;

  if (manuStart !== -1) {
    manufacturer = tokens.slice(manuStart).join(" ");
    brandAndMaybeGeneric = tokens.slice(0, manuStart).join(" ");
  }

  // Try to split brand and generic by common separators like ":" or "-" or "–" or by appearance of dosage (mg, mcg, IU)
  let brand = brandAndMaybeGeneric;
  let generic = null;

  // if there's " - " or " — " or ":" separate
  const sepMatches = brandAndMaybeGeneric.match(/\s[-–—:]\s/);
  if (sepMatches) {
    const parts = brandAndMaybeGeneric.split(/\s[-–—:]\s/);
    if (parts.length >= 2) {
      brand = parts[0].trim();
      generic = parts.slice(1).join(" ").trim();
    }
  } else {
    // look for dosage tokens and comma-separated generics near the end
    const dosageIdx = brandAndMaybeGeneric.search(/\b(\d+(\.\d+)?\s*(mg|mcg|g|IU|ml))\b/i);
    if (dosageIdx !== -1) {
      // we will assume brand is up to first dosage or token grouping before manufacturer
      const beforeDosage = brandAndMaybeGeneric.slice(0, dosageIdx).trim();
      // if beforeDosage contains comma + words (likely generic), treat last comma-separated chunk as generic
      if (beforeDosage.includes(",")) {
        const parts = beforeDosage.split(",");
        brand = parts[0].trim();
        generic = parts.slice(1).join(", ").trim();
      } else {
        brand = beforeDosage;
        const after = brandAndMaybeGeneric.slice(dosageIdx).trim();
        if (after.length > 0) {
          // treat '100 mg GenericName' as generic if present after dosage
          const afterWords = after.split(" ").slice(1).join(" ").trim();
          if (afterWords) generic = afterWords;
        }
      }
    } else {
      // as last fallback, if there are comma separated parts, assume pattern "Brand, Generic, Manufacturer"
      const commaParts = brandAndMaybeGeneric.split(",").map(p => p.trim()).filter(Boolean);
      if (commaParts.length >= 2) {
        brand = commaParts[0];
        generic = commaParts.slice(1).join(", ");
      } else {
        // nothing else — keep full as brand
        brand = brandAndMaybeGeneric;
      }
    }
  }

  // cleanup
  if (brand) brand = brand.replace(/^[\-\:\u2013\u2014]+/, "").trim();
  if (generic) generic = generic.replace(/^[\-\:\u2013\u2014]+/, "").trim();

  return {
    brand: brand || null,
    generic: generic || null,
    manufacturer: manufacturer || null,
    source_url: sourceUrl,
  };
}

async function fetchPage(page) {
  const params = page > 1 ? { page } : {};
  const url = page > 1 ? `${BASE}?page=${page}` : BASE;
  try {
    const r = await axios.get(url, { headers: HEADERS, params, timeout: 20000 });
    return { html: r.data, url: r.request.res.responseUrl || url, status: r.status };
  } catch (err) {
    console.error(`Failed fetching page ${page}:`, err.message || err);
    return null;
  }
}

(async function main() {
  console.log("Starting MedEx brand scraper (Node.js)...");
  const limit = pLimit(CONCURRENCY);
  const allRaw = [];
  let consecutiveEmpty = 0;

  for (let page = 1; page <= MAX_PAGES; page += CONCURRENCY) {
    // schedule up to CONCURRENCY pages in parallel
    const tasks = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const p = page + i;
      if (p > MAX_PAGES) break;
      tasks.push(limit(async () => {
        // small per-task delay to spread requests
        await sleep(REQUEST_DELAY_MS * i);
        const res = await fetchPage(p);
        if (!res) return { page: p, entries: [] };
        const parsed = parsePage(res.html, res.url);
        return { page: p, entries: parsed };
      }));
    }
    const results = await Promise.all(tasks);
    let pageFoundAny = false;
    for (const r of results) {
      if (!r) continue;
      if (r.entries && r.entries.length > 0) {
        pageFoundAny = true;
        for (const e of r.entries) {
          allRaw.push(e);
        }
      } else {
        // treat as possibly end page
      }
    }

    // if no useful entries in this batch, increment consecutiveEmpty, else reset
    if (!pageFoundAny) consecutiveEmpty += 1;
    else consecutiveEmpty = 0;

    // stop if many consecutive empty batches (heuristic)
    if (consecutiveEmpty >= 4) {
      console.log("No more content detected (heuristic). Stopping pagination.");
      break;
    }

    // Very small throttling between batches
    await sleep(REQUEST_DELAY_MS);
    console.log(`Fetched pages ${page}..${Math.min(page + CONCURRENCY - 1, MAX_PAGES)} (collected raw lines: ${allRaw.length})`);
  }

  // normalize entries
  console.log("Normalizing entries and deduplicating...");
  const normalized = allRaw.map((r) => normalizeEntry(r.raw, r.source));
  const dedupMap = new Map();
  for (const e of normalized) {
    const key = ((e.brand || "") + "||" + (e.manufacturer || "")).toLowerCase().trim();
    if (!dedupMap.has(key)) dedupMap.set(key, e);
  }
  const final = Array.from(dedupMap.values());

  // write out
  try {
    await fs.writeFile(OUTFILE, JSON.stringify(final, null, 2), "utf8");
    console.log(`Wrote ${final.length} unique entries to ${OUTFILE}`);
  } catch (err) {
    console.error("Failed to write output file:", err);
  }
})();
