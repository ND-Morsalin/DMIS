/**
 * fetch_medex_brand_details.js
 *
 * Reads bangladesh_medicines_brands.json (array, each item must contain `source_url`)
 * Fetches each brand page (one-by-one), parses detailed info, and appends to
 * bangladesh_avilavle_medicine_info_in_details.json as an array element after each fetch.
 *
 * Dependencies:
 *   npm install axios cheerio
 *
 * Notes:
 * - Script is sequential to avoid hammering the server and to make resuming safe.
 * - If the output file already contains an entry with the same source_url, that brand is skipped.
 * - If any fetch/parse fails, an error object is appended (so progress is preserved).
 * - You can adjust REQUEST_DELAY_MS to be more polite.
 */

const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");

const INPUT_FILE = path.resolve(process.cwd(), "bangladesh_medicines_brands_v1.json");
const OUTPUT_FILE = path.resolve(process.cwd(), "bangladesh_avilavle_medicine_info_in_details.json");
const ORIGIN = "https://medex.com.bd";
const USER_AGENT = "Mozilla/5.0 (compatible; MedExDetailFetcher/1.0)";
const REQUEST_DELAY_MS = 500; // delay between requests (ms) — tune if needed

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

async function readJsonFileOrDie(file) {
  try {
    const txt = await fs.readFile(file, "utf8");
    return JSON.parse(txt);
  } catch (err) {
    console.error(`Failed to read/parse ${file}:`, err.message || err);
    throw err;
  }
}

async function ensureOutputFile() {
  try {
    await fs.access(OUTPUT_FILE);
    // exists - try to parse
    const txt = await fs.readFile(OUTPUT_FILE, "utf8");
    // if empty file, initialize
    if (!txt.trim()) {
      await fs.writeFile(OUTPUT_FILE, JSON.stringify([], null, 2), "utf8");
      return [];
    }
    const arr = JSON.parse(txt);
    if (!Array.isArray(arr)) throw new Error("Output file is not an array");
    return arr;
  } catch (err) {
    // create new file
    await fs.writeFile(OUTPUT_FILE, JSON.stringify([], null, 2), "utf8");
    return [];
  }
}

/** Safe HTTP GET with axios */
async function fetchHtml(url) {
  try {
    const resp = await axios.get(url, {
      headers: { "User-Agent": USER_AGENT },
      timeout: 20000,
    });
    return { html: resp.data, finalUrl: resp.request?.res?.responseUrl || url };
  } catch (err) {
    throw new Error(`Fetch failed: ${err.message || err}`);
  }
}

/** Helpers to extract text safely */
function textOrNull($el) {
  if (!$el || $el.length === 0) return null;
  const t = $el.text().trim();
  return t === "" ? null : t;
}

/** Given brand page HTML, parse fields according to your provided structure */
// replace your parseBrandDetail(...) with this function
function parseBrandDetail(html, pageUrl) {
  const $ = cheerio.load(html);

  // Helpers (reuse existing textOrNull if present)
  function textOrNullLocal($el) {
    if (!$el || $el.length === 0) return null;
    // normalize NBSP and trim
    const t = $el.text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
    return t === "" ? null : t;
  }

  // Name & dosage form
  let name = textOrNullLocal($("h1.page-heading-1-l.brand").first());
  if (name) {
    const subtitle = textOrNullLocal($("h1.page-heading-1-l.brand small.h1-subtitle"));
    if (subtitle) name = name.replace(subtitle, "").trim();
  } else {
    name = textOrNullLocal($(".brand").first()) || null;
  }
  const dosage_form = textOrNullLocal($("h1.page-heading-1-l.brand small.h1-subtitle").first());

  // Generic, Strength, Company (same as before)
  const generic = textOrNullLocal($("div[title='Generic Name'] a").first()) || textOrNullLocal($("div[title='Generic Name']").first());
  const strength = textOrNullLocal($("div[title='Strength']").first()) || null;

  let company = textOrNullLocal($("div[title='Manufactured by'] a").first());
  if (!company) {
    const manuDiv = $("div[title='Manufactured by']").first();
    if (manuDiv && manuDiv.length) {
      const anchor = manuDiv.find("a").first();
      if (anchor && anchor.length) company = textOrNullLocal(anchor);
      else {
        const directText = manuDiv.clone().children().remove().end().text().replace(/\u00A0/g, " ").trim();
        company = directText || null;
      }
    }
  }

  // Pack image
  let pack_image = null;
  const mpImg = $(".mp-trigger img").first();
  if (mpImg && mpImg.length) {
    pack_image = mpImg.attr("src") || mpImg.attr("data-src") || null;
    if (pack_image && !pack_image.startsWith("http")) pack_image = ORIGIN + pack_image;
  } else {
    const imgDefer = $(".img-defer").first();
    if (imgDefer && imgDefer.length) pack_image = imgDefer.attr("src") || imgDefer.attr("data-src") || null;
  }

  // Pricing (same approach as before)
  let unit_price = null, strip_price = null, pack_size_info = null;
  $(".packages-wrapper .package-container").each((i, el) => {
    const $pc = $(el);
    const txt = $pc.text().replace(/\s+/g, " ").trim();
    const upMatch = txt.match(/Unit Price:\s*([৳\d\.,]+)/i);
    if (upMatch && upMatch[1]) unit_price = upMatch[1].trim();
    const spMatch = txt.match(/Strip Price:\s*([৳\d\.,]+)/i);
    if (spMatch && spMatch[1]) strip_price = spMatch[1].trim();
    const psi = $pc.find(".pack-size-info").first();
    if (psi && psi.length) pack_size_info = psi.text().trim();
    if (!pack_size_info) {
      const psiMatch = txt.match(/\(([^)]+x[^)]+)\)/i);
      if (psiMatch && psiMatch[1]) pack_size_info = psiMatch[0];
    }
  });

  // Also available & alternate brands link (unchanged)
  const also_available = [];
  $(".btn-sibling-brands").each((i, a) => {
    const $a = $(a);
    also_available.push({
      text: textOrNullLocal($a),
      href: $a.attr("href") ? ($a.attr("href").startsWith("http") ? $a.attr("href") : ORIGIN + $a.attr("href")) : null
    });
  });
  const alternate_brands_link = $("a.btn-teal.prsinf-btn[href*='/brand-names']").attr("href");
  const alternate_brands_url = alternate_brands_link ? (alternate_brands_link.startsWith("http") ? alternate_brands_link : ORIGIN + alternate_brands_link) : null;

  // Extract ordinary sections text (indications, interaction, etc.)
  const sectionIds = [
    "indications","mode_of_action","dosage","interaction","contraindications",
    "side_effects","pregnancy_cat","precautions","pediatric_uses",
    "overdose_effects","drug_classes","storage_conditions","compound_summary","commonly_asked_questions"
  ];
  const sections = {};
  for (const id of sectionIds) {
    const $marker = $(`#${id}`);
    let text = null;
    if ($marker && $marker.length) {
      let $body = $marker.nextAll(".ac-body").first();
      if (!$body || $body.length === 0) $body = $marker.parent().find(".ac-body").first();
      text = textOrNullLocal($body);
    }
    if (text) sections[id] = text;
  }

  // Common questions as before
  const common_questions = [];
  $("#commonly_asked_questions .caq").each((i, el) => {
    const $el = $(el);
    const q = textOrNullLocal($el.find(".caq-q"));
    const a = textOrNullLocal($el.find(".caq-a"));
    if (q || a) common_questions.push({ q, a });
  });

  // Compound summary parsing (same as before)
  const compound = {};
  const $compoundTable = $("#compound_summary .ac-body table").first();
  if ($compoundTable && $compoundTable.length) {
    $compoundTable.find("tr").each((i, tr) => {
      const $tds = $(tr).find("td");
      if ($tds.length >= 2) {
        const key = $tds.eq(0).text().replace(/[:\s]+$/,"").trim();
        const valEl = $tds.eq(1);
        const valText = valEl.text().trim();
        if (/Molecular Formula/i.test(key)) compound.molecular_formula = valText || null;
        else if (/Chemical Structure/i.test(key)) {
          const img = valEl.find("img").first();
          if (img && img.length) compound.chemical_structure = img.attr("src") ? (img.attr("src").startsWith("http") ? img.attr("src") : ORIGIN + img.attr("src")) : null;
          else compound.chemical_structure = valText || null;
        } else {
          compound[key] = valText || null;
        }
      }
    });
  } else {
    const compTxt = textOrNullLocal($("#compound_summary .ac-body").first());
    if (compTxt) compound.summary = compTxt;
  }

  // therapeutic class
  const therapeutic_class = textOrNullLocal($("#drug_classes .ac-body").first()) || null;

  // ---------- NEW: structured DOSAGE extraction ----------
  const dosageArray = [];
  const $dosageBody = $("#dosage").nextAll(".ac-body").first();
  if ($dosageBody && $dosageBody.length) {
    // find all <strong> headings inside the dosage body
    const $strongs = $dosageBody.find("strong");
    if ($strongs && $strongs.length) {
      $strongs.each((i, s) => {
        const $s = $(s);
        const medType = textOrNullLocal($s) || null;

        // collect nodes between this <strong> and next <strong>
        const nodes = $s.nextUntil("strong").toArray();
        const instructions = [];

        // gather text nodes (non-empty) and all <ul>->li texts within the range
        for (const node of nodes) {
          if (!node) continue;
          if (node.type === "text") {
            const t = $(node).text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
            if (t) instructions.push(t);
          } else if (node.tagName && node.tagName.toLowerCase() === "ul") {
            // collect li items
            $(node).find("li").each((j, li) => {
              const liText = textOrNullLocal($(li));
              if (liText) instructions.push(liText);
            });
          } else {
            // there may be nested ULs under tags (e.g., <div><ul>...</ul></div>)
            const $node = $(node);
            $node.find("ul").each((j, ul) => {
              $(ul).find("li").each((k, li) => {
                const liText = textOrNullLocal($(li));
                if (liText) instructions.push(liText);
              });
            });
            // also capture direct text inside tags (e.g., a sentence before the ul)
            const directText = $node.clone().children("ul").remove().end().text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
            if (directText) {
              // avoid duplicating medType itself
              if (directText !== medType) instructions.unshift(directText);
            }
          }
        }

        // If we found any instructions, push the object
        if (instructions.length > 0) {
          dosageArray.push({
            medication_type: medType,
            instructions
          });
        } else {
          // Sometimes a <strong> may be followed by only text or no ul; include an empty instructions array to preserve type
          dosageArray.push({
            medication_type: medType,
            instructions: []
          });
        }
      });
    } else {
      // Fallback: no strongs — take any top-level ULs as one group
      const fallbackInstructions = [];
      $dosageBody.find("ul").each((i, ul) => {
        $(ul).find("li").each((j, li) => {
          const liText = textOrNullLocal($(li));
          if (liText) fallbackInstructions.push(liText);
        });
      });
      if (fallbackInstructions.length) {
        dosageArray.push({
          medication_type: null,
          instructions: fallbackInstructions
        });
      }
    }
  }
  // ---------- END dosage extraction ----------

  // build final object (include dosage structured array)
  const parsed = {
    source_url: pageUrl,
    name: name || null,
    dosage_form: dosage_form || null,
    generic: generic || null,
    strength: strength || null,
    company: company || null,
    pack_image: pack_image || null,
    pricing: {
      unit_price: unit_price,
      strip_price: strip_price,
      pack_size_info: pack_size_info
    },
    also_available: also_available,
    alternate_brands_url: alternate_brands_url,
    // keep raw section texts
    sections: sections,
    // structured dosage (array of { medication_type, instructions })
    dosage: dosageArray,
    common_questions: common_questions,
    compound_summary: compound,
    therapeutic_class: therapeutic_class,
    fetched_at: new Date().toISOString()
  };

  return parsed;
}


/** Append a new object to the output JSON array (read-modify-write). */
async function appendToOutput(newObj) {
  // Read current array
  let arr;
  try {
    const txt = await fs.readFile(OUTPUT_FILE, "utf8");
    arr = JSON.parse(txt || "[]");
    if (!Array.isArray(arr)) arr = [];
  } catch (err) {
    arr = [];
  }
  arr.push(newObj);
  await fs.writeFile(OUTPUT_FILE, JSON.stringify(arr, null, 2), "utf8");
}

function normalizeUrl(u) {
  if (!u) return u;
  if (u.startsWith("http")) return u;
  if (u.startsWith("/")) return ORIGIN + u;
  return ORIGIN + "/" + u;
}

function alreadyProcessed(outputArr, sourceUrl) {
  if (!sourceUrl) return false;
  const su = sourceUrl.trim();
  return outputArr.some(e => (e && (e.source_url === su || e.source_url === normalizeUrl(su))));
}

(async function main() {
  console.log("Reading input and preparing output file...");
  // load input
  let input;
  try {
    input = await readJsonFileOrDie(INPUT_FILE);
  } catch (err) {
    console.error("Cannot continue without input file. Make sure bangladesh_medicines_brands.json exists.");
    process.exit(1);
  }

  // ensure output file exists and load what's there to allow resume
  const outputArr = await ensureOutputFile();

  console.log(`Input items: ${input.length}. Already in output: ${outputArr.length}. Starting fetch from first unprocessed entry...`);
  for (let i = 0; i < input.length; i++) {
    const item = input[i];
    const src = item.source_url || item.source || item.href || item.link;
    const source_url = src ? normalizeUrl(src) : null;
    const prettyIndex = `${i+1}/${input.length}`;

    // skip if already processed (match by source_url)
    if (alreadyProcessed(outputArr, source_url)) {
      console.log(`[${prettyIndex}] Skipping (already present): ${source_url}`);
      continue;
    }

    console.log(`[${prettyIndex}] Fetching: ${source_url}`);
    try {
      const { html, finalUrl } = await fetchHtml(source_url);
      // parse
      const parsed = parseBrandDetail(html, finalUrl || source_url);
      // add some reference fields from the original brand record if present
      parsed.original_record = {
        input_index: i,
        brand_record: item
      };
      // append
      await appendToOutput(parsed);
      // update in-memory outputArr so skip logic works within same run
      outputArr.push(parsed);

      console.log(`[${prettyIndex}] Parsed & saved: ${parsed.name || parsed.source_url}`);
    } catch (err) {
      console.error(`[${prettyIndex}] Error fetching/parsing ${source_url}:`, err.message || err);
      // write an error record so progress is preserved
      const errObj = {
        source_url: source_url,
        error: (err.message || String(err)),
        fetched_at: new Date().toISOString(),
        original_record: item
      };
      try {
        await appendToOutput(errObj);
        outputArr.push(errObj);
      } catch (writeErr) {
        console.error("Failed to write error record:", writeErr);
      }
    }

    // polite delay between requests
    await sleep(REQUEST_DELAY_MS);
  }

  console.log("Done. Output file:", OUTPUT_FILE);
})();
