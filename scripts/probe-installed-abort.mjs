import { pathToFileURL } from "node:url";
import { join, resolve } from "node:path";

function parseArgs(argv) {
	const rootIndex = argv.indexOf("--root");
	if (rootIndex === -1 || !argv[rootIndex + 1]) {
		throw new Error("Usage: node scripts/probe-installed-abort.mjs --root <global-node_modules-root>");
	}
	return { root: resolve(argv[rootIndex + 1]) };
}

function importFrom(root, packageName, entry = "dist/index.js") {
	return import(pathToFileURL(join(root, ...packageName.split("/"), entry)).href);
}

function assistantMessage(content, stopReason = "stop") {
	return {
		role: "assistant",
		content,
		api: "openai-responses",
		provider: "openai",
		model: "lumicity-abort-probe",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason,
		timestamp: Date.now(),
	};
}

const { root } = parseArgs(process.argv.slice(2));
const [{ Agent }, { EventStream }] = await Promise.all([
	importFrom(root, "@earendil-works/pi-agent-core"),
	importFrom(root, "@earendil-works/pi-ai"),
]);

class ProbeStream extends EventStream {
	constructor() {
		super(
			(event) => event.type === "done" || event.type === "error",
			(event) => {
				if (event.type === "done") return event.message;
				if (event.type === "error") return event.error;
				throw new Error(`Unexpected stream event: ${event.type}`);
			},
		);
	}
}

let providerCalls = 0;
let toolStartedResolve;
const toolStarted = new Promise((resolveStarted) => {
	toolStartedResolve = resolveStarted;
});
const agent = new Agent({
	initialState: {
		tools: [
			{
				name: "never",
				label: "Never",
				description: "Never settles and ignores cancellation",
				parameters: { type: "object", properties: {}, additionalProperties: false },
				execute: async () => {
					toolStartedResolve();
					return new Promise(() => {});
				},
			},
		],
	},
	streamFn: () => {
		providerCalls++;
		const stream = new ProbeStream();
		queueMicrotask(() => {
			stream.push({
				type: "done",
				reason: "toolUse",
				message: assistantMessage(
					[{ type: "toolCall", id: "probe-never", name: "never", arguments: {} }],
					"toolUse",
				),
			});
		});
		return stream;
	},
});

const startedAt = performance.now();
const prompt = agent.prompt("run the never-settling tool");
await toolStarted;
const abortAt = performance.now();
agent.abort("installed-runtime probe");
const settled = await Promise.race([
	prompt.then(() => true),
	new Promise((resolveTimeout) => setTimeout(() => resolveTimeout(false), 150)),
]);
const settledAt = performance.now();
const toolResults = agent.state.messages.filter((message) => message.role === "toolResult");
const validAbortResult =
	toolResults.length === 1 &&
	Array.isArray(toolResults[0].content) &&
	toolResults[0].content.some((block) => block.type === "text" && block.text === "Operation aborted");

const result = {
	ok: settled && !agent.state.isStreaming && providerCalls === 1 && validAbortResult,
	root,
	abortToLogicalIdleMs: Math.round((settledAt - abortAt) * 100) / 100,
	totalMs: Math.round((settledAt - startedAt) * 100) / 100,
	providerCalls,
	toolResultCount: toolResults.length,
	isStreaming: agent.state.isStreaming,
};
console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exitCode = 1;
