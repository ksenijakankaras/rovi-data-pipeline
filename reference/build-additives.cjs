// Builds a compact additives file for the app from the Open Food Facts taxonomy.
const fs = require('fs');
const path = require('path');

// Source: https://static.openfoodfacts.org/data/taxonomies/additives.json (download as additives_taxonomy.json)
const src = path.join(__dirname, 'additives_taxonomy.json');
const dest = path.join(__dirname, 'additives.json');
const raw = JSON.parse(fs.readFileSync(src, 'utf8'));

// Taxonomy properties look like { en: "value" }; this reads them safely
const prop = (entry, key) => {
  const v = entry[key];
  if (!v) return undefined;
  if (typeof v === 'string') return v;
  return v.en ?? Object.values(v)[0];
};
const list = (entry, key) =>
  (prop(entry, key) || '').split(',').map((s) => s.trim()).filter(Boolean);

const out = {};
for (const [id, entry] of Object.entries(raw)) {
  if (!/^en:e\d/.test(id)) continue; // keep only E-numbers
  out[id] = {
    name: entry.name?.en || id.slice(3).toUpperCase(),
    nameIt: entry.name?.it,
    classes: list(entry, 'additives_classes'),
    overexposureRisk: prop(entry, 'efsa_evaluation_overexposure_risk'),
    overexposedGroups: list(entry, 'efsa_evaluation_exposure_95th_greater_than_adi'),
    efsaUrl: prop(entry, 'efsa_evaluation_url'),
  };
}

fs.mkdirSync(path.dirname(dest), { recursive: true });
fs.writeFileSync(dest, JSON.stringify(out));
console.log(`Saved ${Object.keys(out).length} additives to reference/additives.json`);
for (const id of ['en:e330', 'en:e102', 'en:e150d']) console.log(id, JSON.stringify(out[id]));