import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 200,
  duration: "5m",
  thresholds: {
    http_req_duration: ["p(50)<80", "p(95)<180", "p(99)<300"],
  },
};

const payload = JSON.stringify({
  tenant_id: "tenant-a",
  events: [{ event_type: "user.signup", body: { source: "k6" }, client_timestamp: "2026-05-01T00:00:00Z" }],
});

export default function () {
  const res = http.post("http://localhost:8080/v1/events", payload, { headers: { "Content-Type": "application/json" } });
  check(res, { "accepted": (r) => r.status === 202 || r.status === 429 || r.status === 503 });
  sleep(0.001);
}
