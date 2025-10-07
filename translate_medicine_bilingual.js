/**
 * Bilingual Medicine JSON Translator (English ➜ Bangla)
 * -----------------------------------------------------
 * ✅ Translates in batches (configurable)
 * ✅ Writes incrementally to file (stream-safe)
 * ✅ Handles errors gracefully
 * ✅ UTF-8 safe for Bangla text
 * ✅ Compatible with Node.js v18+
 */

import fs from "fs";
import path from "path";
import translate from "@iamtraction/google-translate";

// 🗂️ File paths
const inputPath = path.resolve("./oushod_Khabo.Medicine.json");
const outputPath = path.resolve("./oushod_Khabo.Medicine_bilingual.json");

// ⚙️ Configuration
const fieldsToTranslate = ["name", "strength", "generic", "company", "medicine_type"];
const batchSize = 10; // translate 10 at a time (safe for free API)
const delayBetweenBatches = 500; // ms pause between batches to avoid rate limits

// 🌐 Translation helper
async function translateText(text) {
  if (!text) return text;
  try {
    const res = await translate(text, { from: "en", to: "bn" });
    return res.text;
  } catch (err) {
    console.error("⚠️ Translation failed:", err.message);
    return text; // fallback to original
  }
}

// 🧩 Translate a single medicine object
async function translateItem(item) {
  const newItem = { ...item };

  for (const field of fieldsToTranslate) {
    const value = item[field];
    if (value) {
      newItem[`${field}_en`] = value;
      newItem[`${field}_bn`] = await translateText(value);
      delete newItem[field];
    }
  }

  return newItem;
}

// 🚀 Main process
async function processFile() {
  console.log("🚀 Starting bilingual translation...");

  const data = JSON.parse(fs.readFileSync(inputPath, "utf-8"));
  const total = data.length;

  // Create stream to write output progressively
  const ws = fs.createWriteStream(outputPath, { flags: "w", encoding: "utf-8" });
  ws.write("[\n");

  for (let i = 0; i < total; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;

    console.log(`🌀 Translating batch ${batchNum} (${i + 1}–${i + batch.length})...`);

    // Translate all items in batch concurrently
    const translatedBatch = await Promise.all(batch.map(translateItem));

    // Write each translated item to the output file
    translatedBatch.forEach((item, idx) => {
      const jsonString = JSON.stringify(item, null, 2);
      const isLastItem = i + idx + 1 >= total;
      ws.write(jsonString + (isLastItem ? "\n" : ",\n"));
    });

    console.log(`✅ Batch ${batchNum} complete (${i + batch.length}/${total})`);
    await new Promise((resolve) => setTimeout(resolve, delayBetweenBatches)); // let I/O settle
  }

  ws.write("]");
  ws.end();
  console.log(`🎉 Translation completed successfully!\n📄 Saved as: ${outputPath}`);
}

processFile().catch((err) => console.error("❌ Fatal error:", err));
