import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    burst: {
      executor: "ramping-arrival-rate",
      startRate: 100,
      timeUnit: "1s",
      preAllocatedVUs: 500,
      maxVUs: 2000,
      stages: [
        { target: 5000, duration: "2m" },
        { target: 5000, duration: "1m" },
        { target: 100, duration: "2m" },
      ],
    },
  },
};

const payload = JSON.stringify({
  tenant_id: "tenant-burst",
  events: [{ event_type: "user.signup", body: { source: "k6-burst" }, client_timestamp: "2026-05-01T00:00:00Z" }],
});

export default function () {
  const res = http.post("http://localhost:8080/v1/events", payload, { headers: { "Content-Type": "application/json" } });
  check(res, {
    "backpressure engaged": (r) => [202, 429, 503].includes(r.status),
  });
}
