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
// REPLACE your parseBrandDetail(...) with this function

// Replace your parseBrandDetail(...) with this function

function parseBrandDetail(html, pageUrl) {
  const $ = cheerio.load(html);

  function normText($el) {
    if (!$el || $el.length === 0) return null;
    return $el.text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim() || null;
  }

  // Header: name / dosage_form
  let name = normText($("h1.page-heading-1-l.brand").first());
  if (name) {
    const subtitle = normText($("h1.page-heading-1-l.brand small.h1-subtitle"));
    if (subtitle) name = name.replace(subtitle, "").trim();
  } else {
    name = normText($(".brand").first());
  }
  const dosage_form = normText($("h1.page-heading-1-l.brand small.h1-subtitle").first());

  // Generic / strength / company
  const generic = normText($("div[title='Generic Name'] a").first()) || normText($("div[title='Generic Name']").first());
  const strength = normText($("div[title='Strength']").first()) || null;

  let company = normText($("div[title='Manufactured by'] a").first());
  if (!company) {
    const manuDiv = $("div[title='Manufactured by']").first();
    if (manuDiv && manuDiv.length) {
      const anchor = manuDiv.find("a").first();
      company = anchor && anchor.length ? normText(anchor) : manuDiv.clone().children().remove().end().text().replace(/\u00A0/g, " ").trim() || null;
    }
  }

  // pack image (if present)
  let pack_image = null;
  const mpImg = $(".mp-trigger img, .img-defer").first();
  if (mpImg && mpImg.length) {
    pack_image = mpImg.attr("src") || mpImg.attr("data-src") || null;
    if (pack_image && !pack_image.startsWith("http")) pack_image = (pack_image.startsWith("/") ? "" : "") + pack_image;
  }

  // pricing
  // ---------- Pricing parsing (replace existing pricing logic) ----------
function cleanText($el) {
  if (!$el || $el.length === 0) return null;
  return $el.text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim() || null;
}

const packageEntries = [];
$(".packages-wrapper .package-container").each((i, el) => {
  const $pc = $(el);

  // pack-size-info if present within this package-container
  const psiEl = $pc.find(".pack-size-info").first();
  const psiText = cleanText(psiEl);

  // Direct child spans (exclude pack-size-info span)
  const directSpans = $pc.children("span").not(".pack-size-info").toArray();
  if (directSpans.length >= 2) {
    // typical case: <span>Label:</span><span>৳ 2.25</span>
    const label = cleanText($(directSpans[0]))?.replace(/:$/, "") || null;
    const price = cleanText($(directSpans[1])) || null;
    packageEntries.push({ label, price, pack_size_info: psiText });
  } else if (directSpans.length === 1) {
    // sometimes only a label span is direct; price may be in a nested div
    const label = cleanText($(directSpans[0]))?.replace(/:$/, "") || null;
    // look for nested div span value (e.g., strip price inside <div><span>Strip Price:</span><span>৳ 22.50</span></div>)
    const nestedValueSpan = $pc.find("div span").last();
    const price = cleanText(nestedValueSpan) || null;
    packageEntries.push({ label, price, pack_size_info: psiText });
  }

  // Also capture any nested div blocks that contain their own label & value spans
  $pc.children("div").each((j, d) => {
    const $d = $(d);
    const spans = $d.find("span").toArray();
    if (spans.length >= 2) {
      const label = cleanText($(spans[0]))?.replace(/:$/, "") || null;
      const price = cleanText($(spans[1])) || null;
      // pack-size-info rarely lives inside nested divs; keep null here to avoid duplication
      packageEntries.push({ label, price, pack_size_info: null });
    }
  });
});

// Normalize/dedupe packageEntries (keep order)
const seenPK = new Set();
const packages = [];
for (const p of packageEntries) {
  const key = (p.label || "") + "||" + (p.price || "");
  if (!seenPK.has(key)) {
    seenPK.add(key);
    packages.push(p);
  }
}

// Derive unit_price, strip_price and pack_size_info with fallbacks
let unit_price = null, strip_price = null, pack_size_info = null;
for (const p of packages) {
  const lab = (p.label || "").toLowerCase();
  if (!unit_price && /unit price/i.test(p.label)) unit_price = p.price;
  if (!strip_price && /strip price/i.test(p.label)) strip_price = p.price;
  if (!pack_size_info && p.pack_size_info) pack_size_info = p.pack_size_info;
}
// fallback: if no explicit Unit Price found, prefer the first package price
if (!unit_price && packages.length > 0) unit_price = packages[0].price || null;
// fallback pack_size_info from first package entry if still missing
if (!pack_size_info && packages.length > 0) pack_size_info = packages[0].pack_size_info || null;

// Final pricing object
const pricing = {
  unit_price: unit_price || null,
  strip_price: strip_price || null,
  pack_size_info: pack_size_info || null,
  packages // array of all packages found for this brand page
};
// ----------------------------------------------------------------------


  // flags (.sp-flag)
  const flags = [];
  $(".sp-flag").each((i, el) => {
    const $el = $(el);
    const label = normText($el.find("> div").first());
    // note is often in second div
    const note = normText($el.find("> div").eq(1));
    flags.push({ label, note });
  });

  // also_available and alternate brands
  const also_available = [];
  $(".btn-sibling-brands").each((i, a) => {
    also_available.push({
      text: normText($(a)),
      href: $(a).attr("href") ? ( $(a).attr("href").startsWith("http") ? $(a).attr("href") : ORIGIN + $(a).attr("href") ) : null
    });
  });
  const alternate_brands_link = $("a.btn-teal.prsinf-btn[href*='/brand-names']").attr("href");
  const alternate_brands_url = alternate_brands_link ? (alternate_brands_link.startsWith("http") ? alternate_brands_link : ORIGIN + alternate_brands_link) : null;

  // sections (general textual extraction)
  const sectionIds = ["indications","mode_of_action","dosage","interaction","contraindications",
    "side_effects","pregnancy_cat","precautions","pediatric_uses","overdose_effects",
    "drug_classes","storage_conditions","compound_summary","description","administration"];
  const sections = {};
  for (const id of sectionIds) {
    const $marker = $(`#${id}`);
    if ($marker && $marker.length) {
      // prefer full-str block in 'indications' if present
      if (id === "indications" && $marker.parent().find(".full-str").length) {
        sections[id] = normText($marker.parent().find(".full-str").first());
      } else {
        let $body = $marker.nextAll(".ac-body").first();
        if (!$body || $body.length === 0) $body = $marker.parent().find(".ac-body").first();
        const t = normText($body);
        if (t) sections[id] = t;
      }
    }
  }

  // commonly asked questions -> array of {question, answer}
  const common_questions = [];
  $("#commonly_asked_questions .caq").each((i, el) => {
    const q = normText($(el).find(".caq-q"));
    const a = normText($(el).find(".caq-a"));
    if (q || a) common_questions.push({ question: q, answer: a });
  });
  // fallback if different structure
  if (common_questions.length === 0) {
    $("#commonly_asked_questions .ac-body .caq").each((i, el) => {
      const q = normText($(el).find(".caq-q"));
      const a = normText($(el).find(".caq-a"));
      if (q || a) common_questions.push({ question: q, answer: a });
    });
  }

  // compound summary parse (table or plain)
  const compound = {};
  const $compTable = $("#compound_summary .ac-body table").first();
  if ($compTable && $compTable.length) {
    $compTable.find("tr").each((i, tr) => {
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
        } else compound[key] = valText || null;
      }
    });
  } else {
    const compTxt = normText($("#compound_summary .ac-body").first());
    if (compTxt) compound.summary = compTxt;
  }

  const therapeutic_class = normText($("#drug_classes .ac-body").first()) || null;

  // ---------- Robust DOSAGE parsing ----------
  const dosageArray = [];
  const $dosageBody = $("#dosage").nextAll(".ac-body").first();
  if ($dosageBody && $dosageBody.length) {
    // We'll iterate children in order to preserve paragraphs, ULs and strongs
    const children = $dosageBody.contents().toArray();

    // Helper to push a new group
    function pushGroup(groups, medType, infos, instr) {
      // normalize arrays and dedupe whitespace
      const informations = infos.length ? infos.join(" ").replace(/\s+/g, " ").trim() : null;
      const instructions = instr.map(s => s.replace(/\s+/g, " ").trim()).filter(Boolean);
      groups.push({ medication_type: medType || null, informations: informations, instructions });
    }

    let currMed = null;
    let currInfos = [];
    let currInstr = [];

    // We'll treat <ul> specially: if its <li> items have <strong> as their first child,
    // each <li> becomes its own dosage object; otherwise, <li>s append to current group's instructions.
    for (let idx = 0; idx < children.length; idx++) {
      const node = children[idx];
      if (!node) continue;

      if (node.type === "text") {
        const t = $(node).text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
        if (t) currInfos.push(t);
      } else if (node.type === "tag") {
        const tag = node.tagName.toLowerCase();

        if (tag === "strong") {
          // start a new group: flush previous
          if (currMed !== null || currInfos.length || currInstr.length) {
            pushGroup(dosageArray, currMed, currInfos, currInstr);
            currMed = null; currInfos = []; currInstr = [];
          }
          currMed = normText($(node));
          // continue - following nodes will become info/instructions of this med
        } else if (tag === "ul") {
          // inspect li structure
          const $ul = $(node);
          let liHasStrong = false;
          $ul.children("li").each((i, li) => {
            const $li = $(li);
            const $firstStrong = $li.find("> strong").first();
            if ($firstStrong && $firstStrong.length) liHasStrong = true;
          });

          if (liHasStrong) {
            // flush current group first
            if (currMed !== null || currInfos.length || currInstr.length) {
              pushGroup(dosageArray, currMed, currInfos, currInstr);
              currMed = null; currInfos = []; currInstr = [];
            }
            // each li with strong becomes its own object
            $ul.children("li").each((i, li) => {
              const $li = $(li);
              const $s = $li.find("> strong").first();
              if ($s && $s.length) {
                const medType = normText($s);
                // remove the strong from clone to get the rest text
                const rest = $li.clone().children("strong").remove().end().text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
                const childInstr = [];
                // also collect nested ul li if any
                $li.find("ul").first().find("li").each((j, subli) => {
                  const t = $(subli).text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
                  if (t) childInstr.push(t);
                });
                // if nested lis found, use them; else if rest non-empty, push rest as single instruction
                if (childInstr.length) pushGroup(dosageArray, medType, rest ? [rest] : [], childInstr);
                else pushGroup(dosageArray, medType, rest ? [rest] : [], []);
              } else {
                // li without strong -> treat as generic instruction grouped under null
                const t = $li.text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
                if (t) {
                  // push as its own entry
                  pushGroup(dosageArray, null, null, [t]);
                }
              }
            });
          } else {
            // li do NOT have strong -> append all li text to current group's instructions
            $ul.find("li").each((i, li) => {
              const t = $(li).text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
              if (t) currInstr.push(t);
            });
          }
        } else {
          // other tag (div, p, br container etc.) - treat its text as info (but handle nested ULs)
          const $node = $(node);
          // collect nested ULs first (they append to currInstr)
          $node.find("ul").each((i, ul) => {
            $(ul).find("li").each((j, li) => {
              const t = $(li).text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
              if (t) currInstr.push(t);
            });
          });
          // collect direct text excluding nested UL content
          const direct = $node.clone().children("ul").remove().end().text().replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
          if (direct) currInfos.push(direct);
        }
      } // end tag handling
    } // end children loop

    // flush last group
    if (currMed !== null || currInfos.length || currInstr.length) {
      pushGroup(dosageArray, currMed, currInfos, currInstr);
      currMed = null; currInfos = []; currInstr = [];
    }

    // if no groups created but there is plain textual dosage, add fallback
    if (dosageArray.length === 0) {
      const plain = normText($dosageBody);
      if (plain) dosageArray.push({ medication_type: null, informations: plain, instructions: [] });
    }
  } // end dosageBody present

  // Build final parsed object
  const parsed = {
    source_url: pageUrl,
    name: name || null,
    dosage_form: dosage_form || null,
    generic: generic || null,
    strength: strength || null,
    company: company || null,
    pack_image: pack_image || null,
    pricing: { unit_price, strip_price, pack_size_info },
    flags: flags,
    also_available: also_available,
    alternate_brands_url: alternate_brands_url,
    sections: sections,
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
