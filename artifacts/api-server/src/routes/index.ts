import { Router, type IRouter } from "express";
import healthRouter from "./health";
import candlesRouter from "./candles";

const router: IRouter = Router();

router.use(healthRouter);
router.use(candlesRouter);

export default router;
