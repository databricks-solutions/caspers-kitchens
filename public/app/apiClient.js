export function api(method, url, body) {
  return fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  }).then(async (response) => {
    const payload = await response.json();
    if (!response.ok || payload.ok === false) {
      const msg = payload?.error?.message || `Request failed (${response.status})`;
      throw new Error(msg);
    }
    return payload.data;
  });
}

export function wsUrlFromPath(path) {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${window.location.host}${path}`;
}
