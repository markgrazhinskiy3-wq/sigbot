import { Router, type IRouter, type Request, type Response } from "express";

const router: IRouter = Router();

const _store: Record<string, { candles: unknown[]; updatedAt: number }> = {};

function getKey(): string {
  return process.env.CANDLE_PUSH_KEY || "";
}

router.post("/candles", (req: Request, res: Response) => {
  const key = getKey();
  if (key && req.headers["x-push-key"] !== key) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  const body = req.body as {
    symbol?: string;
    candles?: unknown[];
    batch?: Record<string, unknown[]>;
  };

  let stored = 0;
  if (body.batch && typeof body.batch === "object") {
    for (const [symbol, candles] of Object.entries(body.batch)) {
      if (Array.isArray(candles) && candles.length > 0) {
        _store[symbol] = { candles, updatedAt: Date.now() };
        stored++;
      }
    }
  } else if (body.symbol && Array.isArray(body.candles)) {
    _store[body.symbol] = { candles: body.candles, updatedAt: Date.now() };
    stored = 1;
  } else {
    res.status(400).json({ error: "Expected {symbol, candles} or {batch: {symbol: candles}}" });
    return;
  }

  res.json({ ok: true, stored });
});

router.get("/candles", (_req: Request, res: Response) => {
  const result: Record<string, { candles: unknown[]; age_seconds: number }> = {};
  const now = Date.now();
  for (const [sym, entry] of Object.entries(_store)) {
    result[sym] = {
      candles: entry.candles,
      age_seconds: Math.round((now - entry.updatedAt) / 1000),
    };
  }
  res.json(result);
});

router.get("/candles/:symbol", (req: Request, res: Response) => {
  const sym = req.params.symbol;
  const entry = _store[sym];
  if (!entry) {
    res.status(404).json({ error: "Not found" });
    return;
  }
  res.json({
    symbol: sym,
    candles: entry.candles,
    age_seconds: Math.round((Date.now() - entry.updatedAt) / 1000),
  });
});

export default router;
