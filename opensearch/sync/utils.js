
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
