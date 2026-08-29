import { expect, test } from "@playwright/test";

test("dashboard visualizes live state and performs supported operations", async ({ page }) => {
  const externalRequests: string[] = [];
  const dashboardOrigin = new URL(
    process.env.OCS_WEB_E2E_BASE_URL ?? "http://127.0.0.1:8080",
  ).origin;
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.origin !== dashboardOrigin) {
      externalRequests.push(request.url());
    }
  });

  await page.goto("/");
  await expect(page.getByRole("heading", { name: /Mini OCS/ })).toBeVisible();
  await expect(page.locator(".matrix-cell")).toHaveCount(256);
  await expect(page.locator("#stream-status")).toContainText("synchronized");
  await expect(page.locator("#input-ports .port-card")).toHaveCount(16);
  await expect(page.locator("#output-ports .port-card")).toHaveCount(16);

  await page.locator('input[name="connection-id"]').fill("browser-e2e");
  await page.locator('input[name="input-port"]').fill("7");
  await page.locator('input[name="output-port"]').fill("15");
  await page.locator("#connection-submit").click();

  const row = page.locator('tr[data-connection-id="browser-e2e"]');
  await expect(row).toContainText("ACTIVE");
  await expect(page.locator('[data-input="7"][data-output="15"]')).toHaveClass(/converged/);
  await expect(page.locator("#active-connections")).toHaveText("1");

  await page.locator("#fault-submit").click();
  await expect(page.locator("#operation-status")).toContainText("confirmed by the simulator");
  await page.locator("#fault-clear").click();
  await expect(page.locator("#operation-status")).toContainText("faults cleared");

  await row.getByRole("button", { name: "Delete browser-e2e" }).click();
  await expect(row).toHaveCount(0);
  await expect(page.locator('[data-input="7"][data-output="15"]')).not.toHaveClass(/actual/);
  expect(externalRequests).toEqual([]);
});
