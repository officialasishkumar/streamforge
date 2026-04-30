import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 600,
  duration: "30m",
};

const payload = JSON.stringify({
  tenant_id: "tenant-sustained",
  events: [{ event_type: "billing.charge_succeeded", body: { amount: 4999 }, client_timestamp: "2026-05-01T00:00:00Z" }],
});

export default function () {
  const res = http.post("http://localhost:8080/v1/events", payload, { headers: { "Content-Type": "application/json" } });
  check(res, { "accepted or throttled": (r) => [202, 429, 503].includes(r.status) });
}
