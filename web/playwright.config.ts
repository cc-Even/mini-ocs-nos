import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  reporter: [["line"]],
  use: {
    baseURL: process.env.OCS_WEB_E2E_BASE_URL ?? "http://127.0.0.1:8080",
    browserName: "chromium",
    headless: true,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  outputDir: "/tmp/mini-ocs-playwright-results",
});
