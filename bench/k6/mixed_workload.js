import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 400,
  duration: "15m",
};

function eventBody(size) {
  if (size === "small") return { source: "web" };
  if (size === "medium") return { source: "mobile", metadata: { campaign: "spring", locale: "en-US", region: "us-east-1" } };
  return { source: "api", blob: "x".repeat(4096) };
}

export default function () {
  const r = Math.random();
  const size = r < 0.7 ? "small" : r < 0.95 ? "medium" : "large";
  const payload = JSON.stringify({
    tenant_id: "tenant-mixed",
    events: [{ event_type: "user.activity", body: eventBody(size), client_timestamp: "2026-05-01T00:00:00Z" }],
  });
  const res = http.post("http://localhost:8080/v1/events", payload, { headers: { "Content-Type": "application/json" } });
  check(res, { "accepted or throttled": (x) => [202, 429, 503].includes(x.status) });
}
