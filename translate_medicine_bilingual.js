/**
 * Bilingual Medicine JSON Translator (English ➜ Bangla)
 * -----------------------------------------------------
 * ✅ Translates in batches (configurable)
 * ✅ Writes incrementally to file after each batch (stream-safe)
 * ✅ Supports resuming from where it left off
 * ✅ Handles errors gracefully
 * ✅ UTF-8 safe for Bangla text
 * ✅ Compatible with Node.js v18+
 */

import fs from "fs/promises"; // Switched to promises for async handling
import path from "path";
import translate from "@iamtraction/google-translate";

// 🗂️ File paths
const inputPath = path.resolve("./oushod_Khabo.Medicine.json");
const outputPath = path.resolve("./oushod_Khabo.Medicine_bilingual.json");

// ⚙️ Configuration
const fieldsToTranslate = ["name", "strength", "generic", "company", "medicine_type"];
const batchSize = 10; // Increased batch size for speed (adjust based on API limits; test to avoid rate limiting)
const delayBetweenBatches = 500; // Reduced delay (ms) between batches for speed; adjust if rate limits are hit

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

// 📂 Ensure output file exists and is a valid array (handles incomplete JSON)
async function ensureOutputFile() {
  try {
    await fs.access(outputPath);
    let txt = await fs.readFile(outputPath, "utf8");
    if (!txt.trim()) {
      await fs.writeFile(outputPath, JSON.stringify([], null, 2), "utf8");
      return [];
    }
    try {
      return JSON.parse(txt);
    } catch {
      // Attempt to fix incomplete JSON (e.g., missing ']')
      let fixed = txt.trim();
      if (fixed.endsWith(",")) {
        fixed = fixed.slice(0, -1);
      } else if (fixed.endsWith(',\n')) {
        fixed = fixed.slice(0, -2);
      }
      if (fixed.startsWith("[")) {
        if (!fixed.endsWith("]")) {
          fixed += "]";
        }
      } else {
        throw new Error("Invalid format");
      }
      try {
        return JSON.parse(fixed);
      } catch (err) {
        console.error("⚠️ Could not parse output file, resetting:", err.message);
        await fs.writeFile(outputPath, JSON.stringify([], null, 2), "utf8");
        return [];
      }
    }
  } catch {
    await fs.writeFile(outputPath, JSON.stringify([], null, 2), "utf8");
    return [];
  }
}

// 🚀 Main process
async function processFile() {
  console.log("🚀 Starting bilingual translation...");

  const inputTxt = await fs.readFile(inputPath, "utf8");
  const data = JSON.parse(inputTxt);
  const total = data.length;

  let existing = await ensureOutputFile();
  let processed = existing.length;

  if (processed >= total) {
    console.log("🎉 Already completed!");
    return;
  }

  console.log(`🔄 Resuming from item ${processed + 1}/${total}`);

  for (let i = processed; i < total; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;

    console.log(`🌀 Translating batch ${batchNum} (${i + 1}–${Math.min(i + batch.length, total)}/${total})...`);

    // Translate all items in batch concurrently
    const translatedBatch = await Promise.all(batch.map(translateItem));

    // Append to existing and write the full array (ensures valid JSON)
    existing.push(...translatedBatch);
    await fs.writeFile(outputPath, JSON.stringify(existing, null, 2), "utf8");

    console.log(`✅ Batch ${batchNum} complete and saved (${i + batch.length}/${total})`);
    await new Promise((resolve) => setTimeout(resolve, delayBetweenBatches)); // Pause to avoid rate limits
  }

  console.log(`🎉 Translation completed successfully!\n📄 Saved as: ${outputPath}`);
}

processFile().catch((err) => console.error("❌ Fatal error:", err));