package bullmq

// Lua scripts provide atomic operations on Redis data structures.
// This guarantees job state transitions are race-condition-free.

// addJobScript atomically adds a job to the queue.
// Keys: [wait_key, priority_key, meta_key, id_counter_key, events_key]
// Args: [job_id, job_data_json, priority, delay_ms, lifo (0|1), queue_name, timestamp]
const addJobScript = `
local waitKey      = KEYS[1]
local priorityKey  = KEYS[2]
local jobKey       = KEYS[3]
local idKey        = KEYS[4]
local eventsKey    = KEYS[5]
local delayedKey   = KEYS[6]

local jobId        = ARGV[1]
local jobData      = ARGV[2]
local priority     = tonumber(ARGV[3])
local delayMs      = tonumber(ARGV[4])
local lifo         = ARGV[5] == "1"
local queueName    = ARGV[6]
local timestamp    = tonumber(ARGV[7])

-- Auto-increment job ID if not provided
if jobId == "" then
  jobId = "" .. redis.call("INCR", idKey)
end

-- Store job hash
local dataMap = cjson.decode(jobData)
for k, v in pairs(dataMap) do
  redis.call("HSET", jobKey .. ":" .. jobId, k, tostring(v))
end
redis.call("HSET", jobKey .. ":" .. jobId, "id", jobId)

local processAt = timestamp + delayMs

if delayMs > 0 then
  -- Add to delayed set; score = processAt timestamp
  redis.call("ZADD", delayedKey, processAt, jobId)
  redis.call("PUBLISH", eventsKey, cjson.encode({event="delayed", jobId=jobId, delay=delayMs}))
elseif priority > 0 then
  -- Add to priority sorted set; lower score = higher priority
  redis.call("ZADD", priorityKey, priority, jobId)
  redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId}))
else
  -- Standard FIFO or LIFO add
  if lifo then
    redis.call("LPUSH", waitKey, jobId)
  else
    redis.call("RPUSH", waitKey, jobId)
  end
  redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId}))
end

return jobId
`

// moveToActiveScript atomically moves a job from wait/priority to active.
// Keys: [wait_key, priority_key, active_key, events_key, stalled_key]
// Args: [queue_prefix, timestamp]
const moveToActiveScript = `
local waitKey     = KEYS[1]
local priorityKey = KEYS[2]
local activeKey   = KEYS[3]
local eventsKey   = KEYS[4]
local stalledKey  = KEYS[5]

local queuePrefix = ARGV[1]
local timestamp   = tonumber(ARGV[2])

-- Try priority queue first
local jobId = nil
local priorityJob = redis.call("ZRANGE", priorityKey, 0, 0)
if #priorityJob > 0 then
  jobId = priorityJob[1]
  redis.call("ZREM", priorityKey, jobId)
else
  -- Fall back to standard wait list (LPOP = FIFO since we RPUSH)
  jobId = redis.call("LPOP", waitKey)
end

if not jobId then
  return nil
end

-- Move to active list
redis.call("RPUSH", activeKey, jobId)
-- Track for stall detection
redis.call("SADD", stalledKey, jobId)

-- Update processedOn timestamp
redis.call("HSET", queuePrefix .. ":" .. jobId, "processedOn", timestamp)
redis.call("HINCRBY", queuePrefix .. ":" .. jobId, "attemptsMade", 1)

redis.call("PUBLISH", eventsKey, cjson.encode({event="active", jobId=jobId, prev="waiting"}))

return jobId
`

// moveToCompletedScript atomically moves a job from active to completed.
// Keys: [active_key, completed_key, events_key, job_key]
// Args: [job_id, return_value, keep_jobs (0=all, N=keep N), timestamp]
const moveToCompletedScript = `
local activeKey    = KEYS[1]
local completedKey = KEYS[2]
local eventsKey    = KEYS[3]
local jobKey       = KEYS[4]
local stalledKey   = KEYS[5]

local jobId      = ARGV[1]
local retVal     = ARGV[2]
local keepJobs   = tonumber(ARGV[3])
local timestamp  = tonumber(ARGV[4])

-- Remove from active
redis.call("LREM", activeKey, 0, jobId)
redis.call("SREM", stalledKey, jobId)

-- Update job hash
redis.call("HSET", jobKey, "finishedOn", timestamp)
redis.call("HSET", jobKey, "returnvalue", retVal)

if keepJobs == 0 then
  -- Remove job data immediately
  redis.call("DEL", jobKey)
elseif keepJobs > 0 then
  -- Add to completed set with timestamp score
  redis.call("ZADD", completedKey, timestamp, jobId)
  -- Trim to keep only N most recent
  local count = redis.call("ZCARD", completedKey)
  if count > keepJobs then
    local toRemove = redis.call("ZRANGE", completedKey, 0, count - keepJobs - 1)
    for _, id in ipairs(toRemove) do
      redis.call("DEL", KEYS[4]:gsub(":[^:]+$", ":" .. id))
      redis.call("ZREM", completedKey, id)
    end
  end
else
  -- keepJobs < 0 means keep all
  redis.call("ZADD", completedKey, timestamp, jobId)
end

redis.call("PUBLISH", eventsKey, cjson.encode({event="completed", jobId=jobId, returnvalue=retVal}))
return 1
`

// moveToFailedScript atomically moves a job from active to failed or back to wait for retry.
// Keys: [active_key, failed_key, wait_key, events_key, job_key, delayed_key, stalled_key]
// Args: [job_id, failed_reason, attempts_made, max_attempts, backoff_ms, keep_jobs, timestamp]
const moveToFailedScript = `
local activeKey   = KEYS[1]
local failedKey   = KEYS[2]
local waitKey     = KEYS[3]
local eventsKey   = KEYS[4]
local jobKey      = KEYS[5]
local delayedKey  = KEYS[6]
local stalledKey  = KEYS[7]

local jobId        = ARGV[1]
local failedReason = ARGV[2]
local attemptsMade = tonumber(ARGV[3])
local maxAttempts  = tonumber(ARGV[4])
local backoffMs    = tonumber(ARGV[5])
local keepJobs     = tonumber(ARGV[6])
local timestamp    = tonumber(ARGV[7])

-- Remove from active and stalled tracking
redis.call("LREM", activeKey, 0, jobId)
redis.call("SREM", stalledKey, jobId)

-- Update failedReason in job hash
redis.call("HSET", jobKey, "failedReason", failedReason)
redis.call("HSET", jobKey, "finishedOn", timestamp)

if maxAttempts > 0 and attemptsMade < maxAttempts then
  -- Retry: re-queue with backoff delay
  if backoffMs > 0 then
    local processAt = timestamp + backoffMs
    redis.call("ZADD", delayedKey, processAt, jobId)
    redis.call("PUBLISH", eventsKey, cjson.encode({event="delayed", jobId=jobId, delay=backoffMs}))
  else
    redis.call("RPUSH", waitKey, jobId)
    redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId, prev="failed"}))
  end
  return 0
else
  -- Permanently failed
  if keepJobs == 0 then
    redis.call("DEL", jobKey)
  elseif keepJobs > 0 then
    redis.call("ZADD", failedKey, timestamp, jobId)
    local count = redis.call("ZCARD", failedKey)
    if count > keepJobs then
      local toRemove = redis.call("ZRANGE", failedKey, 0, count - keepJobs - 1)
      for _, id in ipairs(toRemove) do
        redis.call("DEL", KEYS[5]:gsub(":[^:]+$", ":" .. id))
        redis.call("ZREM", failedKey, id)
      end
    end
  else
    redis.call("ZADD", failedKey, timestamp, jobId)
  end
  redis.call("PUBLISH", eventsKey, cjson.encode({event="failed", jobId=jobId, failedReason=failedReason}))
  return 1
end
`

// moveDelayedToWaitScript promotes jobs whose delay has expired to the wait list.
// Keys: [delayed_key, wait_key, priority_key, events_key]
// Args: [queue_prefix, timestamp]
const moveDelayedToWaitScript = `
local delayedKey  = KEYS[1]
local waitKey     = KEYS[2]
local priorityKey = KEYS[3]
local eventsKey   = KEYS[4]
local jobPrefix   = ARGV[1]
local timestamp   = tonumber(ARGV[2])

-- Get all jobs whose delay has expired
local jobs = redis.call("ZRANGEBYSCORE", delayedKey, 0, timestamp)
for _, jobId in ipairs(jobs) do
  redis.call("ZREM", delayedKey, jobId)
  local priority = tonumber(redis.call("HGET", jobPrefix .. ":" .. jobId, "opts") or "0") 
  -- For simplicity, add to wait list (a full impl would parse opts JSON for priority)
  redis.call("RPUSH", waitKey, jobId)
  redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId, prev="delayed"}))
end
return #jobs
`

// retryJobScript manually retries a failed job.
// Keys: [failed_key, wait_key, events_key, job_key]
// Args: [job_id]
const retryJobScript = `
local failedKey = KEYS[1]
local waitKey   = KEYS[2]
local eventsKey = KEYS[3]
local jobKey    = KEYS[4]

local jobId = ARGV[1]

redis.call("ZREM", failedKey, jobId)
redis.call("HDEL", jobKey, "finishedOn", "failedReason")
redis.call("RPUSH", waitKey, jobId)
redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId, prev="failed"}))
return 1
`

// stalledJobsScript detects and recovers stalled (crashed worker) jobs.
// Keys: [stalled_key, active_key, wait_key, failed_key, events_key]
// Args: [max_stall_check_ms, queue_prefix, max_attempts, timestamp]
const stalledJobsScript = `
local stalledKey    = KEYS[1]
local activeKey     = KEYS[2]
local waitKey       = KEYS[3]
local failedKey     = KEYS[4]
local eventsKey     = KEYS[5]
local jobPrefix     = ARGV[1]
local maxAttempts   = tonumber(ARGV[2])
local timestamp     = tonumber(ARGV[3])

local stalled = redis.call("SMEMBERS", stalledKey)
local failedJobs = {}
local restarted = {}

for _, jobId in ipairs(stalled) do
  -- Check if job is actually still in active list
  local inActive = redis.call("LPOS", activeKey, jobId)
  if inActive ~= false then
    local attemptsMade = tonumber(redis.call("HGET", jobPrefix .. ":" .. jobId, "attemptsMade") or "0")
    if maxAttempts > 0 and attemptsMade >= maxAttempts then
      -- Move to failed
      redis.call("LREM", activeKey, 0, jobId)
      redis.call("HSET", jobPrefix .. ":" .. jobId, "failedReason", "job stalled more than allowable limit")
      redis.call("ZADD", failedKey, timestamp, jobId)
      redis.call("PUBLISH", eventsKey, cjson.encode({event="failed", jobId=jobId, failedReason="stalled"}))
      table.insert(failedJobs, jobId)
    else
      -- Re-queue for retry
      redis.call("LREM", activeKey, 0, jobId)
      redis.call("RPUSH", waitKey, jobId)
      redis.call("PUBLISH", eventsKey, cjson.encode({event="waiting", jobId=jobId, prev="active"}))
      table.insert(restarted, jobId)
    end
  end
  redis.call("SREM", stalledKey, jobId)
end

return {failedJobs, restarted}
`
