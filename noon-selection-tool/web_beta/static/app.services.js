(() => {
  async function requestJson(url, { method = "GET", payload, attempt = 0 } = {}) {
    try {
      const response = await fetch(url, {
        method,
        headers: payload === undefined
          ? undefined
          : {
              "Content-Type": "application/json",
            },
        body: payload === undefined ? undefined : JSON.stringify(payload),
      });
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `Request failed: ${url}`);
      }
      return response.json();
    } catch (error) {
      const message = String(error?.message || error || "");
      if (attempt < 1 && /Failed to fetch/i.test(message) && method === "GET") {
        await new Promise((resolve) => window.setTimeout(resolve, 160));
        return requestJson(url, { method, payload, attempt: attempt + 1 });
      }
      throw error;
    }
  }

  async function getJson(url) {
    return requestJson(url, { method: "GET" });
  }

  async function postJson(url, payload = {}, method = "POST") {
    return requestJson(url, { method, payload });
  }

  async function deleteJson(url) {
    return requestJson(url, { method: "DELETE" });
  }

  async function getJsonSafe(url, fallback, label) {
    try {
      return { data: await getJson(url), error: "" };
    } catch (error) {
      return {
        data: fallback,
        error: `${label || url}: ${error.message}`,
      };
    }
  }

  window.WEB_BETA_SERVICES = Object.freeze({
    requestJson,
    getJson,
    postJson,
    deleteJson,
    getJsonSafe,
  });
})();
