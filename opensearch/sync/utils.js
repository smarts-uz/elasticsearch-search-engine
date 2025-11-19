import fs from 'fs';
import path from 'path';

function normalizeGeo(geo) {
  if (geo === null || geo === undefined) return null;

  if (typeof geo === "string") {
    const value = geo.trim().toLowerCase();
    if (value === "null" || value === "undefined" || value === "") return null;

    try {
      const parsed = JSON.parse(geo);
      if (parsed && "latitude" in parsed && "longitude" in parsed) {
        return { lat: Number(parsed.latitude), lon: Number(parsed.longitude) };
      }
    } catch {
      return null;
    }
  }

  if (typeof geo === "object" && "latitude" in geo && "longitude" in geo) {
    return { lat: Number(geo.latitude), lon: Number(geo.longitude) };
  }

  return null;
}

function normalizeAgent(agent) {
  if (agent === null || agent === undefined) return null;
  if (typeof agent === "string") return { name: agent };
  if (typeof agent === "object") return agent;
  return null;
}

export function normalizeRow(row, schema) {
  const result = {};
  for (const [field, type] of Object.entries(schema)) {
    let value = row[field];

    if (value === null || value === undefined) {
      result[field] = null;
      continue;
    }
    if (typeof value === "string") {
      const val = value.trim().toLowerCase();
      if (val === "null" || val === "undefined" || val === "") {
        result[field] = null;
        continue;
      }
    }

    switch (type) {
      case "geo_point":
        result[field] = normalizeGeo(value);
        break;
      case "agent":
        result[field] = normalizeAgent(value);
        break;
      default:
        result[field] = value;
    }
  }
  return result;
}


// File to store last sync times
const LAST_SYNC_FILE = path.resolve('./lastSyncTimes.json');

export function loadLastSyncTimes() {
  try {
    if (fs.existsSync(LAST_SYNC_FILE)) {
      const data = fs.readFileSync(LAST_SYNC_FILE, 'utf-8');
      const parsed = JSON.parse(data);
      // convert strings to Date objects
      Object.keys(parsed).forEach(key => {
        parsed[key] = new Date(parsed[key]);
      });
      return parsed;
    }
  } catch (err) {
    console.error('❌ Last sync load error:', err);
  }

  // default: 1970-01-01
  const defaults = {};
  TABLES_TO_SYNC.forEach(t => defaults[t] = new Date(0));
  return defaults;
}

export function saveLastSyncTimes(times) {
  try {
    const toSave = {};
    Object.keys(times).forEach(key => {
      toSave[key] = times[key].toISOString();
    });
    fs.writeFileSync(LAST_SYNC_FILE, JSON.stringify(toSave, null, 2));
  } catch (err) {
    console.error('❌ Last sync save error:', err);
  }
}
