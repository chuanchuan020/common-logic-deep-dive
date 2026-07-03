require('dotenv').config();
const express = require('express');
const redis = require('redis');
const mysql = require('mysql2/promise');
const moment = require('moment');
const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const winston = require('winston');
const Joi = require('joi');
const schedule = require('node-schedule');
const amqp = require('amqplib');
const { Kafka } = require('kafkajs');
const client = require('prom-client');

const app = express();
const PORT = Number(process.env.SERVER_PORT) || 3000;
const MQ_TYPE = process.env.MQ_TYPE || 'rabbitmq';

// ===================== 1. Prometheus 监控指标体系 =====================
// 开启默认系统指标（CPU、内存、Node.js 运行时）
client.collectDefaultMetrics({ prefix: 'video_flow_' });

// 1.1 HTTP 请求总计数
const httpRequestsTotal = new client.Counter({
  name: 'video_flow_http_requests_total',
  help: 'HTTP 请求总数',
  labelNames: ['method', 'route', 'status']
});

// 1.2 HTTP 请求耗时直方图
const httpRequestDuration = new client.Histogram({
  name: 'video_flow_http_request_duration_seconds',
  help: 'HTTP 请求耗时分布',
  labelNames: ['method', 'route'],
  buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 2]
});

// 1.3 视频打分耗时直方图
const scoreCalcDuration = new client.Histogram({
  name: 'video_flow_score_calc_duration_seconds',
  help: '流量池综合得分计算耗时',
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1]
});

// 1.4 各流量池视频数量仪表盘
const poolVideoGauge = new client.Gauge({
  name: 'video_flow_pool_video_count',
  help: '各流量池当前视频数量',
  labelNames: ['pool']
});

// 1.5 消息队列消费失败计数
const mqConsumeFailCounter = new client.Counter({
  name: 'video_flow_mq_consume_fail_total',
  help: '消息队列消费失败总数',
  labelNames: ['type']
});

// 全局监控中间件：拦截所有请求统计指标
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = (Date.now() - start) / 1000;
    httpRequestsTotal.labels(req.method, req.route?.path || req.path, res.statusCode).inc();
    httpRequestDuration.labels(req.method, req.route?.path || req.path).observe(duration);
  });
  next();
});

// 暴露 Prometheus 抓取端点
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

// ===================== 2. 日志系统 =====================
const logger = winston.createLogger({
  level: process.env.NODE_ENV === 'production' ? 'warn' : 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.printf(info => `[${info.timestamp}] ${info.level.toUpperCase()}: ${info.message}`)
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/app.log' })
  ]
});

// ===================== 3. 核心存储连接 =====================
// 3.1 Redis 自动重连
const redisClient = redis.createClient({ url: process.env.REDIS_URL });
redisClient.on('error', async (err) => {
  logger.error(`[Redis 异常] ${err.message}，5s 后自动重连`);
  setTimeout(() => redisClient.connect().catch(e => logger.error(`[Redis 重连失败] ${e.message}`)), 5000);
});
redisClient.connect().catch(err => logger.error(`[Redis 启动失败] ${err.message}`));

// 3.2 MySQL 连接池
const dbPool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  connectionLimit: Number(process.env.DB_CONN_LIMIT),
  waitForConnections: true,
  queueLimit: 0,
  charset: 'utf8mb4',
  timezone: '+08:00'
});

// ===================== 4. 消息引擎适配层（RabbitMQ/Kafka 无缝切换） =====================
// 统一生产者接口，业务层完全无感切换MQ
let mqProducer = null;
let mqConsumer = null;

// 4.1 RabbitMQ 实现
async function initRabbitMQ() {
  try {
    const conn = await amqp.connect(process.env.MQ_URL);
    conn.on('error', (err) => {
      logger.error(`[RabbitMQ 异常] ${err.message}，10s 后重连`);
      setTimeout(initRabbitMQ, 10000);
    });
    conn.on('close', () => {
      logger.warn('[RabbitMQ 连接关闭] 触发重连');
      setTimeout(initRabbitMQ, 5000);
    });

    const channel = await conn.createChannel();
    await channel.assertQueue(process.env.MQ_QUEUE, { durable: true });
    channel.prefetch(100);

    // 生产者
    mqProducer = {
      send: (payload) => {
        return channel.sendToQueue(
          process.env.MQ_QUEUE,
          Buffer.from(JSON.stringify(payload)),
          { persistent: true }
        );
      }
    };

    // 消费者
    channel.consume(process.env.MQ_QUEUE, async (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        await handleBehaviorMessage(payload);
        channel.ack(msg);
      } catch (err) {
        mqConsumeFailCounter.labels('rabbitmq').inc();
        logger.error(`[RabbitMQ 消费失败] ${err.message}`);
        channel.nack(msg, false, true);
      }
    });

    logger.info('[消息引擎] RabbitMQ 初始化完成');
  } catch (err) {
    logger.error(`[RabbitMQ 初始化失败] ${err.message}`);
    setTimeout(initRabbitMQ, 10000);
  }
}

// 4.2 Kafka 实现
async function initKafka() {
  try {
    const kafka = new Kafka({
      clientId: process.env.KAFKA_CLIENT_ID,
      brokers: process.env.KAFKA_BROKERS.split(',')
    });

    // 生产者
    const producer = kafka.producer();
    await producer.connect();
    mqProducer = {
      send: (payload) => {
        return producer.send({
          topic: process.env.KAFKA_TOPIC,
          messages: [{
            key: String(payload.videoId), // 按视频ID哈希路由，同视频进同分区
            value: JSON.stringify(payload)
          }]
        });
      }
    };

    // 消费者
    const consumer = kafka.consumer({ groupId: 'video-behavior-group' });
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const payload = JSON.parse(message.value.toString());
          await handleBehaviorMessage(payload);
        } catch (err) {
          mqConsumeFailCounter.labels('kafka').inc();
          logger.error(`[Kafka 消费失败] ${err.message}`);
          throw err; // 抛出异常触发重试
        }
      }
    });

    mqConsumer = consumer;
    logger.info('[消息引擎] Kafka 初始化完成');
  } catch (err) {
    logger.error(`[Kafka 初始化失败] ${err.message}`);
    setTimeout(initKafka, 10000);
  }
}

// 4.3 统一消息处理逻辑（双引擎共用）
async function handleBehaviorMessage(payload) {
  const { type, videoId, extra } = payload;
  const vid = String(videoId);

  switch (type) {
    case 'watch':
      await redisClient.incr(`video:watch:${vid}`);
      if (extra?.isFinish) await redisClient.incr(`video:finish:${vid}`);
      break;
    case 'like':
      await redisClient.incr(`video:like:${vid}`);
      break;
    case 'comment':
      await redisClient.incr(`video:comment:${vid}`);
      break;
    case 'skip':
      await redisClient.incr(`video:skip:${vid}`);
      break;
    case 'report':
      const reportKey = `video:report_list:${vid}`;
      await redisClient.rPush(reportKey, extra?.userIp || 'unknown');
      await redisClient.expire(reportKey, 7 * 24 * 3600);
      // 举报日志异步落库
      dbPool.query(
        'INSERT INTO report_log(video_id, user_ip, reason, create_time) VALUES (?,?,?,?)',
        [vid, extra?.userIp || 'unknown', extra?.reason || '常规举报', moment().format('YYYY-MM-DD HH:mm:ss')]
      ).catch(err => logger.error(`[举报落库失败] ${err.message}`));
      break;
    default:
      logger.warn(`[未知行为类型] ${type}`);
  }
}

// 4.4 对外统一发送接口
function sendBehaviorMsg(payload) {
  if (!mqProducer) {
    logger.warn('[MQ] 引擎未就绪，消息丢弃');
    return false;
  }
  mqProducer.send(payload).catch(err => {
    logger.error(`[MQ 消息投递失败] ${err.message}`);
  });
  return true;
}

// 启动消息引擎
if (MQ_TYPE === 'kafka') {
  initKafka();
} else {
  initRabbitMQ();
}

// ===================== 5. 全局常量与配置 =====================
// 5.1 流量池分层配置
const FLOW_POOL = {
  cold:   { name: '冷启动池', minEval: 50,  smoothMin: 30,  upThreshold: 0.10 },
  level1: { name: '初级池',   minEval: 300, smoothMin: 100, upThreshold: 0.16 },
  level2: { name: '中级池',   minEval: 2000,smoothMin: 500, upThreshold: 0.22 },
  hot:    { name: '爆款池',   minEval: 20000,smoothMin: 2000,upThreshold: 0.28 },
  dead:   { name: '废弃低质池' }
};
const POOL_LIST = ['cold', 'level1', 'level2', 'hot'];

// 5.2 A/B 实验权重配置中心
const EXPERIMENT_CONFIG = {
  // 对照组：线上稳定基准版本
  base: {
    alpha: Number(process.env.ALPHA_WEIGHT),
    beta: Number(process.env.BETA_WEIGHT),
    gamma: Number(process.env.GAMMA_WEIGHT),
    delta: Number(process.env.DELTA_WEIGHT),
    lambda: Number(process.env.LAMBDA)
  },
  // 实验组A：强化互动权重，测试点赞评论对分发的影响
  exp_a: { alpha: 0.4, beta: 0.3, gamma: 0.2, delta: 0.1, lambda: 0.00005 },
  // 实验组B：强化负反馈惩罚，测试秒退对分发的抑制效果
  exp_b: { alpha: 0.5, beta: 0.15, gamma: 0.1, delta: 0.25, lambda: 0.00005 }
};

// 5.3 敏感词库（生产环境建议替换为专业内容审核服务）
const BANNED_WORDS = ['涉黄', '暴力', '刷单', '赌博'];

// ===================== 6. 通用工具函数 =====================
// 6.1 统一响应格式
function respFormat(code, msg, data = null) {
  return { code, msg, data };
}

// 6.2 拉普拉斯平滑：解决小样本极值失真问题
function laplaceSmooth(numerator, denominator, smoothBase = 1) {
  return (Number(numerator) + smoothBase) / (Number(denominator) + smoothBase * 2);
}

// 6.3 A/B 实验分桶算法（稳定哈希，同用户永远命中同一组）
function getABTestBucket(userId) {
  if (!userId) return 'base';
  let hash = 0;
  for (let i = 0; i < userId.length; i++) {
    hash = (hash << 5) - hash + userId.charCodeAt(i);
    hash |= 0;
  }
  const bucket = Math.abs(hash) % 100;
  // 流量划分：60% 对照组，20% 实验A，20% 实验B
  if (bucket < 60) return 'base';
  if (bucket < 80) return 'exp_a';
  return 'exp_b';
}

// ===================== 7. 核心打分模型 =====================
/**
 * 基础综合得分计算（用于全局流量池升降级，固定使用基准权重）
 */
function calcBaseScore(finishCnt, watchCnt, likeCnt, commentCnt, skipCnt, timeDelta) {
  const end = scoreCalcDuration.startTimer();
  
  const finishRate = laplaceSmooth(finishCnt, watchCnt, 2);
  const likeRate = laplaceSmooth(likeCnt, watchCnt, 1);
  const commentRate = laplaceSmooth(commentCnt, watchCnt, 1);
  const skipRate = laplaceSmooth(skipCnt, watchCnt, 2);

  const weight = EXPERIMENT_CONFIG.base;
  let numerator = weight.alpha * finishRate + weight.beta * likeRate + weight.gamma * commentRate - weight.delta * skipRate;
  numerator = Math.max(0, numerator);

  const expPart = Math.exp(-1 * weight.lambda * timeDelta);
  const denominator = 1 + expPart;

  end();
  return numerator / denominator;
}

/**
 * 用户个性化得分计算（用于推荐排序，随A/B分组动态切换权重）
 */
function calcUserScore(finishCnt, watchCnt, likeCnt, commentCnt, skipCnt, timeDelta, userId) {
  const bucket = getABTestBucket(userId);
  const weight = EXPERIMENT_CONFIG[bucket];
  
  const finishRate = laplaceSmooth(finishCnt, watchCnt, 2);
  const likeRate = laplaceSmooth(likeCnt, watchCnt, 1);
  const commentRate = laplaceSmooth(commentCnt, watchCnt, 1);
  const skipRate = laplaceSmooth(skipCnt, watchCnt, 2);

  let numerator = weight.alpha * finishRate + weight.beta * likeRate + weight.gamma * commentRate - weight.delta * skipRate;
  numerator = Math.max(0, numerator);

  const expPart = Math.exp(-1 * weight.lambda * timeDelta);
  const denominator = 1 + expPart;

  return { score: numerator / denominator, bucket };
}

// ===================== 8. 流量池升降级核心逻辑 =====================
async function autoChangePool(videoId) {
  const vid = String(videoId);
  const keys = [
    `video:watch:${vid}`, `video:finish:${vid}`, `video:like:${vid}`,
    `video:comment:${vid}`, `video:skip:${vid}`, `video:pool:${vid}`, `video:create_ts:${vid}`
  ];
  const values = await redisClient.mGet(keys);
  const [watchCnt, finishCnt, likeCnt, commentCnt, skipCnt, nowPool, createTs] = values.map(v => Number(v) || 0);
  const currentPool = values[5] || 'cold';

  if (currentPool === 'dead') return { code: 0, msg: '视频已废弃，停止评估' };

  const poolConfig = FLOW_POOL[currentPool];
  const poolIdx = POOL_LIST.indexOf(currentPool);

  // 样本量不足，跳过评估
  if (watchCnt < poolConfig.smoothMin) {
    return { code: 0, msg: `样本不足(${watchCnt}/${poolConfig.smoothMin})，暂不调整` };
  }

  const timeDelta = Math.floor(Date.now() / 1000) - createTs;
  const totalScore = calcBaseScore(finishCnt, watchCnt, likeCnt, commentCnt, skipCnt, timeDelta);

  // 晋级
  if (poolIdx < POOL_LIST.length - 1 && totalScore >= poolConfig.upThreshold) {
    const nextPool = POOL_LIST[poolIdx + 1];
    const multi = redisClient.multi();
    multi.sRem(`pool:${currentPool}`, vid);
    multi.sAdd(`pool:${nextPool}`, vid);
    multi.set(`video:pool:${vid}`, nextPool);
    await multi.exec();
    logger.info(`[视频晋级] ID:${vid} → ${FLOW_POOL[nextPool].name}，得分:${totalScore.toFixed(4)}`);
    return { code: 1, msg: `晋级至${FLOW_POOL[nextPool].name}`, score: totalScore };
  }

  // 降级/废弃
  const downgradeThreshold = poolIdx > 0 ? FLOW_POOL[POOL_LIST[poolIdx - 1]].upThreshold / 2 : 0.04;
  if (totalScore < downgradeThreshold) {
    const targetPool = poolIdx === 0 ? 'dead' : POOL_LIST[poolIdx - 1];
    const multi = redisClient.multi();
    multi.sRem(`pool:${currentPool}`, vid);
    multi.sAdd(`pool:${targetPool}`, vid);
    multi.set(`video:pool:${vid}`, targetPool);
    await multi.exec();
    logger.warn(`[视频降级] ID:${vid} → ${FLOW_POOL[targetPool].name}，得分:${totalScore.toFixed(4)}`);
    return { code: 2, msg: `移入${FLOW_POOL[targetPool].name}`, score: totalScore };
  }

  return { code: 0, msg: '层级不变', score: totalScore };
}

// ===================== 9. 全局中间件 =====================
app.use(express.json({ limit: '100kb' }));

// 9.1 分布式限流
const watchLimiter = rateLimit({
  windowMs: Number(process.env.WATCH_WINDOW),
  max: Number(process.env.WATCH_LIMIT),
  standardHeaders: true,
  legacyHeaders: false,
  message: respFormat(429, '请求过于频繁，请稍后再试'),
  store: new RedisStore({ sendCommand: (...args) => redisClient.sendCommand(args) })
});

const reportLimiter = rateLimit({
  windowMs: Number(process.env.REPORT_WINDOW),
  max: Number(process.env.REPORT_LIMIT),
  standardHeaders: true,
  legacyHeaders: false,
  message: respFormat(429, '举报操作过于频繁'),
  store: new RedisStore({ sendCommand: (...args) => redisClient.sendCommand(args) })
});

// 9.2 管理员鉴权
function adminAuth(req, res, next) {
  const token = req.headers['authorization'];
  if (token === process.env.ADMIN_TOKEN) return next();
  return res.status(401).json(respFormat(401, '无权限访问'));
}

// 9.3 参数校验
function validateSchema(schema) {
  return (req, res, next) => {
    const { error } = schema.validate(req.body, { abortEarly: false });
    if (error) {
      const errMsg = error.details.map(item => item.message).join('; ');
      return res.status(400).json(respFormat(400, `参数校验失败: ${errMsg}`));
    }
    next();
  };
}

// ===================== 10. 定时任务 =====================
// 10.1 全量视频流量池评估
schedule.scheduleJob(process.env.SCORE_CRON, async () => {
  logger.info('[定时任务] 开始全量流量池评估');
  try {
    for (const pool of POOL_LIST) {
      // 生产环境大数据量请替换为 sScan 分批遍历，避免阻塞 Redis
      const videos = await redisClient.sMembers(`pool:${pool}`);
      for (const vid of videos) {
        await autoChangePool(vid);
      }
    }
    // 更新流量池数量监控指标
    for (const pool of [...POOL_LIST, 'dead']) {
      const count = await redisClient.sCard(`pool:${pool}`);
      poolVideoGauge.labels(pool).set(count);
    }
    logger.info('[定时任务] 全量评估完成，监控指标已更新');
  } catch (err) {
    logger.error(`[定时任务异常] ${err.message}`);
  }
});

// 10.2 废弃池内存 GC
schedule.scheduleJob(process.env.GC_CRON, async () => {
  logger.info('[GC 任务] 开始清理废弃池数据');
  try {
    const deadVideos = await redisClient.sMembers('pool:dead');
    if (deadVideos.length === 0) {
      logger.info('[GC 任务] 无废弃数据，跳过');
      return;
    }

    const multi = redisClient.multi();
    for (const vid of deadVideos) {
      multi.del(`video:watch:${vid}`, `video:finish:${vid}`, `video:like:${vid}`);
      multi.del(`video:comment:${vid}`, `video:skip:${vid}`, `video:create_ts:${vid}`);
      multi.del(`video:pool:${vid}`, `video:report_list:${vid}`);
      multi.sRem('pool:dead', vid);
    }
    await multi.exec();
    logger.info(`[GC 任务] 清理完成，释放 ${deadVideos.length} 个视频资源`);
  } catch (err) {
    logger.error(`[GC 任务异常] ${err.message}`);
  }
});

// ===================== 11. 业务接口 =====================

// 11.1 视频上传发布
const uploadSchema = Joi.object({
  title: Joi.string().min(1).max(255).required(),
  tag: Joi.string().max(255).allow('', null),
  content: Joi.string().allow('', null)
});
app.post('/api/video/upload', validateSchema(uploadSchema), async (req, res) => {
  try {
    const { title, tag, content } = req.body;

    // AI 机审拦截
    const isViolation = BANNED_WORDS.some(word => (title + content).includes(word));
    const initialPool = isViolation ? 'dead' : 'cold';
    const nowTs = Math.floor(Date.now() / 1000);

    // 基础信息落库
    const [result] = await dbPool.query(
      'INSERT INTO video(title, tag, content, create_time) VALUES (?,?,?,?)',
      [title, tag || 'other', content || '', moment().format('YYYY-MM-DD HH:mm:ss')]
    );
    const videoId = String(result.insertId);

    // Redis 初始化状态与索引
    const multi = redisClient.multi();
    multi.sAdd(`pool:${initialPool}`, videoId);
    multi.set(`video:pool:${videoId}`, initialPool);
    multi.set(`video:watch:${videoId}`, 0);
    multi.set(`video:finish:${videoId}`, 0);
    multi.set(`video:like:${videoId}`, 0);
    multi.set(`video:comment:${videoId}`, 0);
    multi.set(`video:skip:${videoId}`, 0);
    multi.set(`video:create_ts:${videoId}`, nowTs);

    // 多标签倒排索引
    if (tag) {
      tag.split(',').map(t => t.trim()).filter(t => t).forEach(t => {
        multi.sAdd(`tag:${t}`, videoId);
      });
    }
    await multi.exec();

    if (isViolation) {
      logger.warn(`[违规拦截] 视频${videoId} 触发敏感词，直接废弃`);
      return res.json(respFormat(201, '发布成功，内容违规已停止推荐', { videoId, pool: 'dead' }));
    }

    logger.info(`[视频发布] ID:${videoId} 进入冷启动池`);
    return res.json(respFormat(200, '发布成功，进入冷启动流量池', { videoId, pool: 'cold' }));
  } catch (err) {
    logger.error(`[上传接口异常] ${err.message}`);
    return res.status(500).json(respFormat(500, '发布失败'));
  }
});

// 11.2 个性化推荐流（支持多标签 + A/B 权重排序）
app.get('/api/video/feed', async (req, res) => {
  try {
    const { tag, userId } = req.query;
    if (!tag) return res.status(400).json(respFormat(400, '请传入兴趣标签'));

    const tagList = tag.split(',').map(t => t.trim()).filter(t => t);
    const tagKeys = tagList.map(t => `tag:${t}`);

    // 多级池召回：优先爆款池，其次中级池
    const hotMatch = await redisClient.sInter(['pool:hot', ...tagKeys]);
    const level2Match = await redisClient.sInter(['pool:level2', ...tagKeys]);
    const candidateIds = [...new Set([...hotMatch, ...level2Match])];

    // 批量获取视频数据，按用户分组权重排序
    const videoList = [];
    for (const vid of candidateIds.slice(0, 50)) { // 取Top50做排序
      const keys = [`video:watch:${vid}`, `video:finish:${vid}`, `video:like:${vid}`, `video:comment:${vid}`, `video:skip:${vid}`, `video:create_ts:${vid}`];
      const values = await redisClient.mGet(keys);
      const [watch, finish, like, comment, skip, createTs] = values.map(v => Number(v) || 0);
      const timeDelta = Math.floor(Date.now() / 1000) - createTs;
      const { score, bucket } = calcUserScore(finish, watch, like, comment, skip, timeDelta, userId);
      videoList.push({ videoId: vid, score, abBucket: bucket });
    }

    // 按得分降序返回
    videoList.sort((a, b) => b.score - a.score);
    return res.json(respFormat(200, '推荐召回成功', {
      total: videoList.length,
      abBucket: getABTestBucket(userId),
      list: videoList
    }));
  } catch (err) {
    logger.error(`[推荐接口异常] ${err.message}`);
    return res.status(500).json(respFormat(500, '推荐失败'));
  }
});

// 11.3 播放埋点
const watchSchema = Joi.object({
  videoId: Joi.number().required(),
  isFinish: Joi.boolean().default(false)
});
app.post('/api/video/watch', watchLimiter, validateSchema(watchSchema), async (req, res) => {
  sendBehaviorMsg({ type: 'watch', videoId: req.body.videoId, extra: { isFinish: req.body.isFinish } });
  return res.json(respFormat(200, '播放记录成功'));
});

// 11.4 点赞埋点
const likeSchema = Joi.object({ videoId: Joi.number().required() });
app.post('/api/video/like', watchLimiter, validateSchema(likeSchema), async (req, res) => {
  sendBehaviorMsg({ type: 'like', videoId: req.body.videoId });
  return res.json(respFormat(200, '点赞成功'));
});

// 11.5 评论埋点
const commentSchema = Joi.object({ videoId: Joi.number().required() });
app.post('/api/video/comment', watchLimiter, validateSchema(commentSchema), async (req, res) => {
  sendBehaviorMsg({ type: 'comment', videoId: req.body.videoId });
  return res.json(respFormat(200, '评论成功'));
});

// 11.6 秒退/划走负反馈埋点
const skipSchema = Joi.object({ videoId: Joi.number().required() });
app.post('/api/video/skip', watchLimiter, validateSchema(skipSchema), async (req, res) => {
  sendBehaviorMsg({ type: 'skip', videoId: req.body.videoId });
  return res.json(respFormat(200, '记录成功'));
});

// 11.7 用户举报
const reportSchema = Joi.object({
  videoId: Joi.number().required(),
  reason: Joi.string().max(255).allow('', null)
});
app.post('/api/video/report', reportLimiter, validateSchema(reportSchema), async (req, res) => {
  const userIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  sendBehaviorMsg({ type: 'report', videoId: req.body.videoId, extra: { userIp, reason: req.body.reason } });
  return res.json(respFormat(200, '举报已提交，我们将尽快核实'));
});

// 11.8 管理员强制下架
const forceDeadSchema = Joi.object({ videoId: Joi.number().required() });
app.post('/api/admin/force_dead', adminAuth, validateSchema(forceDeadSchema), async (req, res) => {
  try {
    const { videoId } = req.body;
    const vid = String(videoId);
    const nowPool = await redisClient.get(`video:pool:${vid}`);

    if (nowPool && nowPool !== 'dead') {
      const multi = redisClient.multi();
      multi.sRem(`pool:${nowPool}`, vid);
      multi.sAdd('pool:dead', vid);
      multi.set(`video:pool:${vid}`, 'dead');
      await multi.exec();
      logger.warn(`[人工干预] 视频${vid} 被管理员强制下架`);
    }
    return res.json(respFormat(200, '操作成功，视频已下架'));
  } catch (err) {
    logger.error(`[强制下架异常] ${err.message}`);
    return res.status(500).json(respFormat(500, '操作失败'));
  }
});

// 11.9 流量池概览
app.get('/api/admin/pool/status', adminAuth, async (req, res) => {
  try {
    const data = {};
    for (const key of [...POOL_LIST, 'dead']) {
      const count = await redisClient.sCard(`pool:${key}`);
      data[key] = { name: FLOW_POOL[key].name, count };
    }
    return res.json(respFormat(200, '查询成功', data));
  } catch (err) {
    return res.status(500).json(respFormat(500, '查询失败'));
  }
});

// 11.10 健康检查
app.get('/health', async (req, res) => {
  let redisOk = false, dbOk = false, mqOk = !!mqProducer;
  try { await redisClient.ping(); redisOk = true; } catch {}
  try { await dbPool.query('SELECT 1'); dbOk = true; } catch {}
  return res.json(respFormat(200, 'ok', {
    redis: redisOk,
    mysql: dbOk,
    mq: { type: MQ_TYPE, status: mqOk }
  }));
});

// ===================== 12. 服务启动与优雅停机 =====================
const server = app.listen(PORT, () => {
  logger.info(`🚀 流量池推荐系统启动成功，端口: ${PORT}`);
  logger.info(`📍 健康检查: http://127.0.0.1:${PORT}/health`);
  logger.info(`📊 监控指标: http://127.0.0.1:${PORT}/metrics`);
});

// 优雅关闭：销毁所有连接
process.on('SIGINT', async () => {
  logger.info('收到关闭信号，开始优雅停机...');
  try {
    schedule.gracefulShutdown();
    await redisClient.quit();
    await dbPool.end();
    if (MQ_TYPE === 'kafka' && mqConsumer) await mqConsumer.disconnect();
    server.close(() => logger.info('服务已安全关闭'));
  } catch (err) {
    logger.error(`停机异常: ${err.message}`);
    process.exit(1);
  }
});