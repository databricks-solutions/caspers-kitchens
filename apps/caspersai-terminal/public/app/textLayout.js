export function visualLength(value) {
  return Array.from(value).length;
}

export function truncateVisual(value, maxLength) {
  if (maxLength <= 0) {
    return "";
  }

  const chars = Array.from(value);
  if (chars.length <= maxLength) {
    return value;
  }

  if (maxLength === 1) {
    return "…";
  }

  return `${chars.slice(0, maxLength - 1).join("")}…`;
}

export function padVisual(value, width) {
  const trimmed = truncateVisual(value, Math.max(0, width));
  const remaining = Math.max(0, width - visualLength(trimmed));
  return `${trimmed}${" ".repeat(remaining)}`;
}

export function centerVisual(value, width) {
  const trimmed = truncateVisual(value, Math.max(0, width));
  const visible = visualLength(trimmed);
  if (visible >= width) {
    return trimmed;
  }

  const left = Math.floor((width - visible) / 2);
  const right = Math.max(0, width - visible - left);
  return `${" ".repeat(left)}${trimmed}${" ".repeat(right)}`;
}

export function wrapVisual(value, width) {
  if (width <= 0) {
    return [""];
  }

  const normalized = String(value || "").trim();
  if (normalized.length === 0) {
    return [""];
  }

  const words = normalized.split(/\s+/).filter((part) => part.length > 0);
  if (words.length === 0) {
    return [""];
  }

  const lines = [];
  let current = "";

  for (const word of words) {
    const candidate = current.length > 0 ? `${current} ${word}` : word;
    if (visualLength(candidate) <= width) {
      current = candidate;
      continue;
    }

    if (current.length > 0) {
      lines.push(current);
    }

    if (visualLength(word) > width) {
      lines.push(truncateVisual(word, width));
      current = "";
      continue;
    }

    current = word;
  }

  if (current.length > 0) {
    lines.push(current);
  }

  return lines.length > 0 ? lines : [""];
}
