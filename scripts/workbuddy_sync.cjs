"use strict";
// Reuse WorkBuddy's installed SDK in this separate process.
const fs = require("node:fs");
const Module = require("node:module");
const path = require("node:path");
const { randomUUID, randomBytes, createHash } = require("node:crypto");
const http = require("node:http");
let sending = false;

function admissionId() {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(Date.now(), 0, 6);
  bytes[6] = (bytes[6] & 15) | 112;
  bytes[8] = (bytes[8] & 63) | 128;
  return bytes.toString("hex");
}

function promptReady(state) {
  // These native terminal states all accept PROMPT_SENT to start working.
  return ["idle", "error", "failed", "terminated"].includes(state);
}

function nativeWireText(text) {
  try {
    const payload = JSON.parse(text);
    const fields = ["message", "source", "source_conversation"];
    if (payload && !Array.isArray(payload) && Object.keys(payload).length === fields.length &&
        fields.every(key => typeof payload[key] === "string")) {
      // The remote layer expands raw @skill: mentions. Preserve JSON values,
      // but keep those mentions inert until the receiving AI decodes the body.
      return text.replace(/@/g, "\\u0040");
    }
  } catch (_) {}
  return text;
}

async function consumePermission(input) {
  if (!input.permit || typeof input.permit.ticket !== "string" ||
      !Number.isInteger(input.consumePort) || input.consumePort < 1 || input.consumePort > 65535) {
    throw new Error("原生投递缺少一次性后台许可");
  }
  const body = JSON.stringify({ticket: input.permit.ticket, task_id: input.permit.task_id,
    action: input.permit.action || "task", target: input.target, message_id: input.messageId,
    text_hash: createHash("sha256").update(input.text, "utf8").digest("hex")});
  const permission = await new Promise((resolve, reject) => {
    const req = http.request({hostname: "127.0.0.1", port: input.consumePort,
      path: "/collaboration/consume", method: "POST",
      headers: {"Content-Type": "application/json", "Content-Length": Buffer.byteLength(body)}}, res => {
      let data = "";
      res.on("data", chunk => {
        data += chunk;
        if (Buffer.byteLength(data) > 65536) req.destroy(new Error("投递许可响应过长"));
      });
      res.on("error", reject);
      res.on("aborted", () => reject(new Error("后台许可回执中断，未投递")));
      res.on("end", () => {
        try {
          if (res.statusCode !== 200) throw new Error("后台拒绝最终投递许可");
          resolve(JSON.parse(data));
        } catch (error) { reject(error); }
      });
    });
    req.setTimeout(20000, () => req.destroy(new Error("后台许可核验超时，未投递")));
    req.on("error", reject);
    req.end(body);
  });
  if (permission.allowed !== true || typeof permission.allow_busy !== "boolean") {
    throw new Error("后台未确认最终投递许可");
  }
}

async function main() {
  const input = JSON.parse(fs.readFileSync(0, "utf8"));
  const bundle = new Module(input.sdkFile, module);
  bundle.filename = input.sdkFile;
  bundle.paths = Module._nodeModulePaths(path.dirname(input.sdkFile));
  bundle._compile(
    fs.readFileSync(input.sdkFile, "utf8") +
      "\ninit_lib(); module.exports.channelSdk = lib_exports;",
    input.sdkFile,
  );
  const { sdk } = await bundle.exports.channelSdk.handshake({
    userId: input.userId,
    deviceId: input.clientId,
    clientType: "desktop",
    fetchCredentials: async () => input.credentials,
    onMetric: () => {},
    onTrace: () => {},
    logger: { error: () => {}, info: () => {}, debug: () => {} },
    logLevel: "error",
  });
  const channel = "executor:" + input.deviceId;
  async function request(type, args, timeoutMs = 40000) {
    const reply = await sdk.request(
      channel,
      { type, args, commandId: randomUUID() },
      { timeoutMs },
    );
    const body = reply.payload;
    if (!body || body.success !== true || body.result?.errorCode) {
      const code = body?.result?.errorCode || body?.code || body?.error?.code;
      const error = new Error(body?.result?.message || body?.error?.message ||
        (typeof body?.error === "string" ? body.error : "宿主拒绝请求"));
      // Only explicit native no-admission codes prove no prompt was accepted.
      // Generic rejections and transport failures after submit stay uncertain.
      error.notSent = ["writer_occupied", "busy", "conversation_busy", "conversation_writer_occupied"]
        .includes(typeof code === "string" ? code.toLowerCase() : "");
      throw error;
    }
    return body.result;
  }
  const until = Date.now() + input.waitIdle * 1000;
  let snapshot;
  do {
    snapshot = await request("wb:conversations:get", [input.conversationId]);
    if (snapshot?.info?.id !== input.conversationId) {
      throw new Error("返回的聊天编号不匹配，未发信");
    }
    if (input.managed && input.mode !== "status" && !promptReady(snapshot.info.state) && !input.waitIdle) {
      throw new Error("WorkBuddy 原聊天忙，未提交；不支持忙时插入");
    }
    if (input.mode === "status" || !input.waitIdle || promptReady(snapshot.info.state)) {
      break;
    }
    if (Date.now() >= until) throw new Error("等待对方空闲超时，未发信");
    await new Promise((resolve) => setTimeout(resolve, 3000));
  } while (true);
  if (input.mode === "status") {
    return { state: snapshot.info.state, title: snapshot.info.title };
  }
  if (input.managed) await consumePermission(input);
  // A timeout after this point is ambiguous. Never automatically publish again.
  sending = true;
  const admission = admissionId();
  const wireText = input.confirmDelivery ? nativeWireText(input.text) : input.text;
  const receipt = await request("wb:conversations:sendPrompt", [
    input.conversationId,
    [{ type: "text", text: wireText }],
    // The installed native admission path enqueues a race with another writer.
    // queueDelivery:'steer' is intentionally absent. No submitted RPC is retried.
    { clientRequestId: input.messageId, _expectQueueReceipt: true,
      ...(input.confirmDelivery ? { _meta: { "codebuddy.ai/conversationRequestId": admission } } : {}) },
  ]);
  if (!input.confirmDelivery) {
    return { state: "accepted", route: receipt?.disposition === "queued" ? "native-queue" : "native-sendPrompt" };
  }
  if (receipt?.disposition === "queued" && typeof receipt.item?.id === "string" &&
      receipt.item.id && receipt.item.clientRequestId === input.messageId) {
    return {state: "accepted", route: "native-queue",
      receipt: {state: "queued", stage: "host_queue", native_id: receipt.item.id}};
  }
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt) await new Promise(resolve => setTimeout(resolve, 250));
    try {
      // edge-sync exposes requests, not the desktop-only inputHistory API.
      const history = await request("wb:conversations:requests", [input.conversationId, 0, 3], 3000);
      const matching = Array.isArray(history?.items) ? history.items.filter(entry => entry?.id === admission) : [];
      const entry = matching.length === 1 ? matching[0] : null;
      const content = entry?.userMessage?.content;
      const accepted = Array.isArray(content) && content.length === 1 &&
        content[0]?.type === "text" && content[0].text === wireText;
      if (accepted) return {state: "accepted", route: "native-sendPrompt",
        receipt: {state: "accepted", stage: "native_host", native_id: admission}};
    } catch (_) {
      // Only bounded reads may repeat. No failure can resubmit the prompt.
    }
  }
  return {state: "uncertain", route: "native-sendPrompt"};
}

main().then(
  (result) => {
    console.log(JSON.stringify({ event: "result", result }));
    process.exit(0);
  },
  (error) => {
    console.log(JSON.stringify({
      event: "error",
      state: sending && error.notSent !== true ? "uncertain" : "not_sent",
      error: String(error.message).slice(0, 240),
    }));
    process.exit(1);
  },
);
