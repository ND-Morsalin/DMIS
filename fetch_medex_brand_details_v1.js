// medex_detail_fetcher_with_medicine_id.js
const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");
const crypto = require("crypto");

const INPUT_FILE = path.resolve(process.cwd(), "missing_medicine.json");
const OUTPUT_FILE = path.resolve(
  process.cwd(),
  "medicine_info_in_details.json"
);
const ORIGIN = "https://medex.com.bd";
const USER_AGENT = "Mozilla/5.0 (compatible; MedExDetailFetcher/1.0)";
const REQUEST_DELAY_MS = 500; // delay between requests (ms) — tune if needed

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

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
    const txt = await fs.readFile(OUTPUT_FILE, "utf8");
    if (!txt.trim()) {
      await fs.writeFile(OUTPUT_FILE, JSON.stringify([], null, 2), "utf8");
      return [];
    }
    const arr = JSON.parse(txt);
    if (!Array.isArray(arr)) throw new Error("Output file is not an array");
    return arr;
  } catch (err) {
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

/** --- NEW: medicine id extraction --- */
/** Returns a stable medicine id string for the given input record.
 * priority:
 *  1) item._id.$oid (typical mongo export)
 *  2) item._id
 *  3) item.id
 *  4) deterministic SHA1 from source_url (or item JSON) truncated to 24 chars
 */
function getMedicineId(item, source_url, fallbackIndex) {
  if (!item) {
    // fallback to generated id from source_url or index
    const seed = source_url || String(fallbackIndex || "");
    return crypto.createHash("sha1").update(seed).digest("hex").slice(0, 24);
  }
  if (item._id && typeof item._id === "object") {
    if (item._id.$oid) return String(item._id.$oid);
    // if it's an object with other shape, stringify
    try {
      return String(item._id);
    } catch (e) {}
  }
  if (item._id) return String(item._id);
  if (item.id) return String(item.id);
  // last fallback: deterministic hash
  const seed =
    source_url || JSON.stringify(item) || String(fallbackIndex || "");
  return crypto.createHash("sha1").update(seed).digest("hex").slice(0, 24);
}

/** Given brand page HTML, parse fields according to your provided structure */
function parseBrandDetail(html, pageUrl) {
  const $ = cheerio.load(html);

  function makeAbs(src) {
    if (!src) return null;
    if (/^https?:\/\//i.test(src)) return src;
    if (src.startsWith("/")) return ORIGIN + src;
    return ORIGIN + "/" + src;
  }
  function normText($el) {
    if (!$el || $el.length === 0) return null;
    return (
      $el
        .text()
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim() || null
    );
  }

  // Header: name / dosage_form
  let name = normText($("h1.page-heading-1-l.brand").first());
  if (name) {
    const subtitle = normText($("h1.page-heading-1-l.brand small.h1-subtitle"));
    if (subtitle) name = name.replace(subtitle, "").trim();
  } else {
    name = normText($(".brand").first());
  }
  const dosage_form = normText(
    $("h1.page-heading-1-l.brand small.h1-subtitle").first()
  );

  // Generic / strength / company
  const generic =
    normText($("div[title='Generic Name'] a").first()) ||
    normText($("div[title='Generic Name']").first());
  const strength = normText($("div[title='Strength']").first()) || null;
  let company = normText($("div[title='Manufactured by'] a").first());
  if (!company) {
    const manuDiv = $("div[title='Manufactured by']").first();
    if (manuDiv && manuDiv.length) {
      const anchor = manuDiv.find("a").first();
      company =
        anchor && anchor.length
          ? normText(anchor)
          : manuDiv
              .clone()
              .children()
              .remove()
              .end()
              .text()
              .replace(/\u00A0/g, " ")
              .trim() || null;
    }
  }

  // pack image (if present)
  let pack_image = null;
  const mpImg = $(".mp-trigger img, .img-defer").first();
  if (mpImg && mpImg.length) {
    const src =
      mpImg.attr("src") ||
      mpImg.attr("data-src") ||
      mpImg.attr("data-lazy") ||
      null;
    pack_image = src ? makeAbs(src) : null;
  }

  // ---------- Pricing parsing ----------
  function cleanText($el) {
    if (!$el || $el.length === 0) return null;
    return (
      $el
        .text()
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim() || null
    );
  }
  const packageEntries = [];
  $(".packages-wrapper .package-container").each((i, el) => {
    const $pc = $(el);
    const psiEl = $pc.find(".pack-size-info").first();
    const psiText = cleanText(psiEl);
    const directSpans = $pc.children("span").not(".pack-size-info").toArray();
    if (directSpans.length >= 2) {
      const label = cleanText($(directSpans[0]))?.replace(/:$/, "") || null;
      const price = cleanText($(directSpans[1])) || null;
      packageEntries.push({ label, price, pack_size_info: psiText });
    } else if (directSpans.length === 1) {
      const label = cleanText($(directSpans[0]))?.replace(/:$/, "") || null;
      const nestedValueSpan = $pc.find("div span").last();
      const price = cleanText(nestedValueSpan) || null;
      packageEntries.push({ label, price, pack_size_info: psiText });
    }
    $pc.children("div").each((j, d) => {
      const $d = $(d);
      const spans = $d.find("span").toArray();
      if (spans.length >= 2) {
        const label = cleanText($(spans[0]))?.replace(/:$/, "") || null;
        const price = cleanText($(spans[1])) || null;
        packageEntries.push({ label, price, pack_size_info: null });
      }
    });
  });

  const seenPK = new Set();
  const packages = [];
  for (const p of packageEntries) {
    const key = (p.label || "") + "||" + (p.price || "");
    if (!seenPK.has(key)) {
      seenPK.add(key);
      packages.push(p);
    }
  }

  let unit_price = null,
    strip_price = null,
    pack_size_info = null;
  for (const p of packages) {
    if (!unit_price && /unit price/i.test(p.label || "")) unit_price = p.price;
    if (!strip_price && /strip price/i.test(p.label || ""))
      strip_price = p.price;
    if (!pack_size_info && p.pack_size_info) pack_size_info = p.pack_size_info;
  }
  if (!unit_price && packages.length > 0)
    unit_price = packages[0].price || null;
  if (!pack_size_info && packages.length > 0)
    pack_size_info = packages[0].pack_size_info || null;
  const pricing = {
    unit_price: unit_price || null,
    strip_price: strip_price || null,
    pack_size_info: pack_size_info || null,
    packages,
  };

  // flags (.sp-flag)
  const flags = [];
  $(".sp-flag").each((i, el) => {
    const $el = $(el);
    const label = normText($el.find("> div").first());
    const note = normText($el.find("> div").eq(1));
    flags.push({ label, note });
  });

  // also_available and alternate brands
  const also_available = [];
  $(".btn-sibling-brands").each((i, a) => {
    also_available.push({
      text: normText($(a)),
      href: $(a).attr("href")
        ? $(a).attr("href").startsWith("http")
          ? $(a).attr("href")
          : ORIGIN + $(a).attr("href")
        : null,
    });
  });
  const alternate_brands_link = $(
    "a.btn-teal.prsinf-btn[href*='/brand-names']"
  ).attr("href");
  const alternate_brands_url = alternate_brands_link
    ? alternate_brands_link.startsWith("http")
      ? alternate_brands_link
      : ORIGIN + alternate_brands_link
    : null;

  // parseSectionToStructuredArray (keeps original implementation)
  function parseSectionToStructuredArray(sectionId) {
    const $marker = $(`#${sectionId}`).first();
    let $body = null;
    if ($marker && $marker.length) {
      $body = $marker.next(".ac-body");
      if (!$body || $body.length === 0)
        $body = $marker.nextAll(".ac-body").first();
    }
    if (!$body || $body.length === 0) {
      $body =
        $(`#${sectionId} .ac-body`).first() ||
        $(`#${sectionId}`).parent().find(".ac-body").first();
    }
    const out = [];
    if (!$body || $body.length === 0) return out;
    const children = $body.contents().toArray();
    let currTitle = null;
    let currInfos = [];
    let currItems = [];
    function flushCurrent() {
      const infosText = currInfos
        .join(" ")
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      const itemsArr = currItems
        .map((s) =>
          s
            .replace(/\u00A0/g, " ")
            .replace(/\s+/g, " ")
            .trim()
        )
        .filter(Boolean);
      if (currTitle !== null || infosText || itemsArr.length) {
        out.push({
          title: currTitle || null,
          information: infosText || null,
          items: itemsArr,
        });
      }
      currTitle = null;
      currInfos = [];
      currItems = [];
    }
    for (let i = 0; i < children.length; i++) {
      const node = children[i];
      if (!node) continue;
      if (node.type === "text") {
        const t = $(node)
          .text()
          .replace(/\u00A0/g, " ")
          .replace(/\s+/g, " ")
          .trim();
        if (t) currInfos.push(t);
      } else if (node.type === "tag") {
        const tag = node.tagName.toLowerCase();
        if (tag === "strong") {
          flushCurrent();
          currTitle = normText($(node)) || null;
        } else if (tag === "ul" || tag === "ol") {
          $(node)
            .find("li")
            .each((j, li) => {
              const liText = $(li)
                .text()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              if (liText) currItems.push(liText);
            });
        } else {
          const $node = $(node);
          const clone = $node.clone();
          clone.find("ul, ol").remove();
          const directText = clone
            .text()
            .replace(/\u00A0/g, " ")
            .replace(/\s+/g, " ")
            .trim();
          if (directText) currInfos.push(directText);
          $node.find("ul, ol").each((j, list) => {
            $(list)
              .find("li")
              .each((k, li) => {
                const liText = $(li)
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                if (liText) currItems.push(liText);
              });
          });
        }
      }
    }
    flushCurrent();
    if (out.length === 0) {
      const full = normText($body) || null;
      if (full) out.push({ title: null, information: full, items: [] });
    }
    return out;
  }

  // common_questions
  function extractAnswerArray($container) {
    if (!$container || $container.length === 0) return [];
    const parts = [];
    $container.contents().each((i, node) => {
      if (!node) return;
      if (node.type === "text") {
        const t = $(node)
          .text()
          .replace(/\u00A0/g, " ")
          .replace(/\s+/g, " ")
          .trim();
        if (t) parts.push(t);
      } else if (node.type === "tag") {
        const tag = node.tagName.toLowerCase();
        if (tag === "ul" || tag === "ol") {
          $(node)
            .find("li")
            .each((j, li) => {
              const liText = $(li)
                .text()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              if (liText) parts.push(liText);
            });
        } else {
          const $node = $(node);
          const clone = $node.clone();
          clone.find("ul, ol").remove();
          const textPart = clone
            .text()
            .replace(/\u00A0/g, " ")
            .replace(/\s+/g, " ")
            .trim();
          if (textPart) parts.push(textPart);
          $node.find("ul, ol").each((j, list) => {
            $(list)
              .find("li")
              .each((k, li) => {
                const liText = $(li)
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                if (liText) parts.push(liText);
              });
          });
        }
      }
    });
    return parts.map((s) => s.trim()).filter(Boolean);
  }

  const common_questions = [];
  const $commBlock = $("#commonly_asked_questions").first();
  if ($commBlock && $commBlock.length) {
    $commBlock.find(".ac-body .caq, .caq").each((i, el) => {
      const $el = $(el);
      const q =
        normText($el.find(".caq-q").first()) ||
        normText($el.find("h4").first()) ||
        null;
      const $ansEl = $el.find(".caq-a").first();
      let arr = extractAnswerArray($ansEl);
      if ((!arr || arr.length === 0) && $ansEl && $ansEl.length) {
        const fallback = normText($ansEl);
        if (fallback) arr = [fallback];
      }
      if (q || (arr && arr.length > 0))
        common_questions.push({ question: q, answer: arr });
    });
  }
  if (common_questions.length === 0) {
    $(".caq").each((i, el) => {
      const $el = $(el);
      const q =
        normText($el.find(".caq-q").first()) ||
        normText($el.find("h4").first()) ||
        null;
      const $ansEl = $el.find(".caq-a").first();
      let arr = extractAnswerArray($ansEl);
      if ((!arr || arr.length === 0) && $ansEl && $ansEl.length) {
        const fallback = normText($ansEl);
        if (fallback) arr = [fallback];
      }
      if (q || (arr && arr.length > 0))
        common_questions.push({ question: q, answer: arr });
    });
  }

  // compound_summary
  const compound = { molecular_formula: null, chemical_structure: null };
  const $compMarker = $("#compound_summary").first();
  let $compBody = null;
  if ($compMarker && $compMarker.length) {
    $compBody = $compMarker.next(".ac-body");
    if (!$compBody || $compBody.length === 0)
      $compBody = $compMarker.nextAll(".ac-body").first();
  }
  if (!$compBody || $compBody.length === 0)
    $compBody =
      $("#compound_summary .ac-body").first() ||
      $("#compound_summary").parent().find(".ac-body").first();
  if ($compBody && $compBody.length) {
    const $table = $compBody.find("table").first();
    if ($table && $table.length) {
      $table.find("tr").each((i, tr) => {
        const $tds = $(tr).find("td");
        if ($tds.length >= 2) {
          const rawKey = $tds
            .eq(0)
            .text()
            .replace(/[:\s]+$/, "")
            .trim()
            .toLowerCase();
          const $valTd = $tds.eq(1);
          if (/molecular\s*formula/i.test(rawKey)) {
            const mfText = $valTd
              .text()
              .replace(/\u00A0/g, " ")
              .replace(/\s+/g, " ")
              .trim();
            if (mfText) compound.molecular_formula = mfText;
          } else if (
            /chemical\s*structure/i.test(rawKey) ||
            /structure/i.test(rawKey)
          ) {
            const $img = $valTd.find("img").first();
            if ($img && $img.length) {
              const src =
                $img.attr("src") ||
                $img.attr("data-src") ||
                $img.attr("data-lazy") ||
                null;
              if (src) compound.chemical_structure = makeAbs(src);
            } else {
              const csText = $valTd
                .text()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              if (csText) compound.chemical_structure = csText;
            }
          } else {
            if (
              /molecular/i.test(rawKey) &&
              /formula/i.test(rawKey) &&
              !compound.molecular_formula
            ) {
              const mfText = $tds
                .eq(1)
                .text()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              if (mfText) compound.molecular_formula = mfText;
            }
            if (/structure/i.test(rawKey) && !compound.chemical_structure) {
              const $img = $tds.eq(1).find("img").first();
              if ($img && $img.length) {
                const src =
                  $img.attr("src") ||
                  $img.attr("data-src") ||
                  $img.attr("data-lazy") ||
                  null;
                if (src) compound.chemical_structure = makeAbs(src);
              } else {
                const csText = $tds
                  .eq(1)
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                if (csText) compound.chemical_structure = csText;
              }
            }
          }
        }
      });
    }
    if (!compound.molecular_formula || !compound.chemical_structure) {
      const bodyText = $compBody
        .text()
        .replace(/\u00A0/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      if (!compound.molecular_formula) {
        const mfMatch = bodyText.match(
          /Molecular\s*Formula\s*[:\-]?\s*([A-Za-z0-9\-\+\(\)\/·\s]+)/i
        );
        if (mfMatch && mfMatch[1])
          compound.molecular_formula = mfMatch[1].trim();
      }
      if (!compound.chemical_structure) {
        const $img = $compBody.find("img").first();
        if ($img && $img.length) {
          const src =
            $img.attr("src") ||
            $img.attr("data-src") ||
            $img.attr("data-lazy") ||
            null;
          if (src) compound.chemical_structure = makeAbs(src);
        }
      }
    }
  }
  if (!compound.molecular_formula) compound.molecular_formula = null;
  if (!compound.chemical_structure) compound.chemical_structure = null;

  // therapeutic_class
  let therapeutic_class = null;
  const $drugMarker = $("#drug_classes").first();
  let $drugBody = null;
  if ($drugMarker && $drugMarker.length) {
    $drugBody = $drugMarker.next(".ac-body");
    if (!$drugBody || $drugBody.length === 0)
      $drugBody = $drugMarker.nextAll(".ac-body").first();
  }
  if (!$drugBody || $drugBody.length === 0) {
    $drugBody =
      $("#drug_classes .ac-body").first() ||
      $("#drug_classes").parent().find(".ac-body").first();
  }
  if ($drugBody && $drugBody.length)
    therapeutic_class = normText($drugBody) || null;
  else {
    const altText =
      $drugMarker && $drugMarker.length ? normText($drugMarker.next()) : null;
    therapeutic_class = altText || null;
  }

  // dosage (keeps your robust parsing)
  const dosageArray = [];
  const $dosageBody = $("#dosage").nextAll(".ac-body").first();
  if ($dosageBody && $dosageBody.length) {
    const children = $dosageBody.contents().toArray();
    function pushGroup(groups, medType, infos, instr) {
      const information = infos.length
        ? infos.join(" ").replace(/\s+/g, " ").trim()
        : null;
      const instructions = instr
        .map((s) => s.replace(/\s+/g, " ").trim())
        .filter(Boolean);
      groups.push({
        medication_type: medType || null,
        information: information,
        instructions,
      });
    }
    let currMed = null;
    let currInfos = [];
    let currInstr = [];
    for (let idx = 0; idx < children.length; idx++) {
      const node = children[idx];
      if (!node) continue;
      if (node.type === "text") {
        const t = $(node)
          .text()
          .replace(/\u00A0/g, " ")
          .replace(/\s+/g, " ")
          .trim();
        if (t) currInfos.push(t);
      } else if (node.type === "tag") {
        const tag = node.tagName.toLowerCase();
        if (tag === "strong") {
          if (currMed !== null || currInfos.length || currInstr.length) {
            pushGroup(dosageArray, currMed, currInfos, currInstr);
            currMed = null;
            currInfos = [];
            currInstr = [];
          }
          currMed = normText($(node));
        } else if (tag === "ul") {
          const $ul = $(node);
          let liHasStrong = false;
          $ul.children("li").each((i, li) => {
            const $li = $(li);
            const $firstStrong = $li.find("> strong").first();
            if ($firstStrong && $firstStrong.length) liHasStrong = true;
          });
          if (liHasStrong) {
            if (currMed !== null || currInfos.length || currInstr.length) {
              pushGroup(dosageArray, currMed, currInfos, currInstr);
              currMed = null;
              currInfos = [];
              currInstr = [];
            }
            $ul.children("li").each((i, li) => {
              const $li = $(li);
              const $s = $li.find("> strong").first();
              if ($s && $s.length) {
                const medType = normText($s);
                const rest = $li
                  .clone()
                  .children("strong")
                  .remove()
                  .end()
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                const childInstr = [];
                $li
                  .find("ul")
                  .first()
                  .find("li")
                  .each((j, subli) => {
                    const t = $(subli)
                      .text()
                      .replace(/\u00A0/g, " ")
                      .replace(/\s+/g, " ")
                      .trim();
                    if (t) childInstr.push(t);
                  });
                if (childInstr.length)
                  pushGroup(
                    dosageArray,
                    medType,
                    rest ? [rest] : [],
                    childInstr
                  );
                else pushGroup(dosageArray, medType, rest ? [rest] : [], []);
              } else {
                const t = $li
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                if (t) pushGroup(dosageArray, null, null, [t]);
              }
            });
          } else {
            $ul.find("li").each((i, li) => {
              const t = $(li)
                .text()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();
              if (t) currInstr.push(t);
            });
          }
        } else {
          const $node = $(node);
          $node.find("ul").each((i, ul) => {
            $(ul)
              .find("li")
              .each((j, li) => {
                const t = $(li)
                  .text()
                  .replace(/\u00A0/g, " ")
                  .replace(/\s+/g, " ")
                  .trim();
                if (t) currInstr.push(t);
              });
          });
          const direct = $node
            .clone()
            .children("ul")
            .remove()
            .end()
            .text()
            .replace(/\u00A0/g, " ")
            .replace(/\s+/g, " ")
            .trim();
          if (direct) currInfos.push(direct);
        }
      }
    }
    if (currMed !== null || currInfos.length || currInstr.length) {
      pushGroup(dosageArray, currMed, currInfos, currInstr);
      currMed = null;
      currInfos = [];
      currInstr = [];
    }
    if (dosageArray.length === 0) {
      const plain = normText($dosageBody);
      if (plain)
        dosageArray.push({
          medication_type: null,
          information: plain,
          instructions: [],
        });
    }
  }

  // promoted sections
  const promoteSections = [
    "indications",
    "mode_of_action",
    "interaction",
    "contraindications",
    "side_effects",
    "pregnancy_cat",
    "precautions",
    "pediatric_uses",
    "overdose_effects",
    "storage_conditions",
    "description",
    "administration",
  ];
  const promoted = {};
  for (const sid of promoteSections)
    promoted[sid] = parseSectionToStructuredArray(sid);

  // Build final parsed object
  const parsed = {
    source_url: pageUrl,
    name: name || null,
    dosage_form: dosage_form || null,
    generic: generic || null,
    strength: strength || null,
    company: company || null,
    pack_image: pack_image || null,
    pricing: pricing,
    flags: flags,
    also_available: also_available,
    alternate_brands_url: alternate_brands_url,
    indications: promoted.indications,
    mode_of_action: promoted.mode_of_action,
    interaction: promoted.interaction,
    contraindications: promoted.contraindications,
    side_effects: promoted.side_effects,
    pregnancy_cat: promoted.pregnancy_cat,
    precautions: promoted.precautions,
    pediatric_uses: promoted.pediatric_uses,
    overdose_effects: promoted.overdose_effects,
    storage_conditions: promoted.storage_conditions,
    description: promoted.description,
    administration: promoted.administration,
    compound_summary: compound,
    therapeutic_class: therapeutic_class || null,
    dosage: dosageArray || [],
    common_questions: common_questions || [],
    fetched_at: new Date().toISOString(),
  };

  return parsed;
}

/** Append a new object to the output JSON array (read-modify-write). */
async function appendToOutput(newObj) {
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
  return outputArr.some(
    (e) => e && (e.source_url === su || e.source_url === normalizeUrl(su))
  );
}

(async function main() {
  console.log("Reading input and preparing output file...");
  let input;
  try {
    input = await readJsonFileOrDie(INPUT_FILE);
  } catch (err) {
    console.error(
      "Cannot continue without input file. Make sure bangladesh_medicines_brands.json exists."
    );
    process.exit(1);
  }

  const outputArr = await ensureOutputFile();
  console.log(
    `Input items: ${input.length}. Already in output: ${outputArr.length}. Starting fetch from first unprocessed entry...`
  );
  //! const last_item_input_index =
    //! outputArr[outputArr.length - 1].original_record.input_index;
  //
  // console.log({ last_item_input_index });
  // const deeplyClonedPart = input.slice().map(item => ({ ...item }));
  //

  //! for (let i = last_item_input_index + 1; i < input.length; i++) {
  for (let i = 0; i < input.length; i++) {
    const item = input[i];
    const src = item.source_url || item.source || item.href || item.link;
    const source_url = src ? normalizeUrl(src) : null;
    const prettyIndex = `${i + 1}/${input.length}`;

    // derive medicine_id for this item (NEW: medicine id assignment)
    const medicineId = getMedicineId(item, source_url, i);

    if (alreadyProcessed(outputArr, source_url)) {
      console.log(`[${prettyIndex}] Skipping (already present): ${source_url}`);
      continue;
    }

    console.log(`[${prettyIndex}] Fetching: ${source_url}`);
    try {
      const { html, finalUrl } = await fetchHtml(source_url);
      const parsed = parseBrandDetail(html, finalUrl || source_url);

      // attach medicine_id and a small original_record reference
      parsed.medicine_id = medicineId;
      parsed.original_record = { input_index: i, brand_record: item };

      await appendToOutput(parsed);
      outputArr.push(parsed);
      console.log(
        `[${prettyIndex}] Parsed & saved: ${parsed.name || parsed.source_url}`
      );
    } catch (err) {
      console.error(
        `[${prettyIndex}] Error fetching/parsing ${source_url}:`,
        err.message || err
      );
      // write an error record so progress is preserved and include medicine_id
      const errObj = {
        source_url: source_url,
        medicine_id: medicineId,
        error: err.message || String(err),
        fetched_at: new Date().toISOString(),
        original_record: item,
      };
      try {
        await appendToOutput(errObj);
        outputArr.push(errObj);
      } catch (writeErr) {
        console.error("Failed to write error record:", writeErr);
      }
    }

    await sleep(REQUEST_DELAY_MS);
  }

  console.log("Done. Output file:", OUTPUT_FILE);
})();
