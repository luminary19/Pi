import { type AssistantMessage, type AssistantMessageEvent, EventStream } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { describe, expect, it } from "vitest";
import {
	Agent,
	type AgentEvent,
	type AgentMessage,
	type AgentTool,
	type AgentToolResult,
	type AgentToolUpdateCallback,
} from "../src/index.ts";

function createDeferred<T = void>(): {
	promise: Promise<T>;
	resolve: (value: T) => void;
	reject: (reason?: unknown) => void;
} {
	let resolve = (_value: T) => {};
	let reject = (_reason?: unknown) => {};
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

class MockAssistantStream extends EventStream<AssistantMessageEvent, AssistantMessage> {
	constructor() {
		super(
			(event) => event.type === "done" || event.type === "error",
			(event) => {
				if (event.type === "done") return event.message;
				if (event.type === "error") return event.error;
				throw new Error("Unexpected event type");
			},
		);
	}
}

function createUsage() {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function isAssistantMessage(message: AgentMessage | undefined): message is AssistantMessage {
	return message?.role === "assistant" && "stopReason" in message;
}

function createAssistantMessage(
	content: AssistantMessage["content"],
	stopReason: AssistantMessage["stopReason"] = "stop",
): AssistantMessage {
	return {
		role: "assistant",
		content,
		api: "openai-responses",
		provider: "openai",
		model: "mock",
		usage: createUsage(),
		stopReason,
		timestamp: Date.now(),
	};
}

function completedStream(message: AssistantMessage): MockAssistantStream {
	const stream = new MockAssistantStream();
	const reason = message.stopReason === "length" || message.stopReason === "toolUse" ? message.stopReason : "stop";
	queueMicrotask(() => stream.push({ type: "done", reason, message }));
	return stream;
}

async function expectToSettleWithin(work: Promise<unknown>, timeoutMs = 150): Promise<void> {
	const settled = await Promise.race([
		work.then(() => true),
		new Promise<false>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
	]);
	expect(settled).toBe(true);
}

async function expectToRemainPending(work: Promise<unknown>, durationMs = 20): Promise<void> {
	const settled = await Promise.race([
		work.then(() => true),
		new Promise<false>((resolve) => setTimeout(() => resolve(false), durationMs)),
	]);
	expect(settled).toBe(false);
}

describe("bounded cancellation", () => {
	it("logically settles a never-settling tool and tracks physical cleanup", async () => {
		const toolStarted = createDeferred();
		const releaseTool = createDeferred<AgentToolResult<Record<string, never>>>();
		let lateUpdate: AgentToolUpdateCallback<Record<string, never>> | undefined;
		let providerCalls = 0;
		let afterToolCalls = 0;
		const events: AgentEvent[] = [];
		const schema = Type.Object({});
		const tool: AgentTool<typeof schema, Record<string, never>> = {
			name: "never",
			label: "Never",
			description: "Never settles until released by the test",
			parameters: schema,
			execute: async (_toolCallId, _params, _signal, onUpdate) => {
				lateUpdate = onUpdate;
				toolStarted.resolve();
				return releaseTool.promise;
			},
		};
		const agent = new Agent({
			initialState: { tools: [tool] },
			afterToolCall: async () => {
				afterToolCalls++;
				return undefined;
			},
			streamFn: () => {
				providerCalls++;
				return completedStream(
					createAssistantMessage(
						[{ type: "toolCall", id: "call-never", name: "never", arguments: {} }],
						"toolUse",
					),
				);
			},
		});
		agent.subscribe((event) => {
			events.push(event);
		});

		const prompt = agent.prompt("run the tool");
		await toolStarted.promise;
		agent.abort("test abort");
		await expectToSettleWithin(prompt);

		expect(agent.state.isStreaming).toBe(false);
		expect(providerCalls).toBe(1);
		expect(afterToolCalls).toBe(0);
		expect(events.filter((event) => event.type === "tool_execution_end")).toHaveLength(1);
		const toolResults = agent.state.messages.filter((message) => message.role === "toolResult");
		expect(toolResults).toHaveLength(1);
		expect(toolResults[0].content).toEqual([{ type: "text", text: "Operation aborted" }]);

		const eventCount = events.length;
		lateUpdate?.({ content: [], details: {} });
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(events).toHaveLength(eventCount);

		const physicalSettlement = agent.waitForPhysicalSettlement();
		await expectToRemainPending(physicalSettlement);
		releaseTool.resolve({ content: [{ type: "text", text: "late" }], details: {} });
		await expectToSettleWithin(physicalSettlement);
	});

	it("does not start parallel tools prepared before an abort during later preparation", async () => {
		const secondPreparationStarted = createDeferred();
		const releasePreparation = createDeferred();
		const executed: string[] = [];
		let preparationIndex = 0;
		let providerCalls = 0;
		const schema = Type.Object({});
		const createTool = (name: string): AgentTool<typeof schema> => ({
			name,
			label: name,
			description: name,
			parameters: schema,
			execute: async () => {
				executed.push(name);
				return { content: [{ type: "text", text: name }], details: {} };
			},
		});
		const agent = new Agent({
			initialState: { tools: [createTool("first"), createTool("second")] },
			beforeToolCall: async () => {
				preparationIndex++;
				if (preparationIndex === 2) {
					secondPreparationStarted.resolve();
					await releasePreparation.promise;
				}
				return undefined;
			},
			streamFn: () => {
				providerCalls++;
				return completedStream(
					createAssistantMessage(
						[
							{ type: "toolCall", id: "call-first", name: "first", arguments: {} },
							{ type: "toolCall", id: "call-second", name: "second", arguments: {} },
						],
						"toolUse",
					),
				);
			},
		});

		const prompt = agent.prompt("run both");
		await secondPreparationStarted.promise;
		agent.abort("test abort");
		await expectToSettleWithin(prompt);

		expect(executed).toEqual([]);
		expect(providerCalls).toBe(1);
		const toolResults = agent.state.messages.filter((message) => message.role === "toolResult");
		expect(toolResults.map((message) => message.toolCallId)).toEqual(["call-first", "call-second"]);
		expect(toolResults.every((message) => message.isError)).toBe(true);

		const physicalSettlement = agent.waitForPhysicalSettlement();
		await expectToRemainPending(physicalSettlement);
		releasePreparation.resolve();
		await expectToSettleWithin(physicalSettlement);
	});

	it("logically aborts a provider iterator that never yields and rejects stale generation updates", async () => {
		const firstProviderStarted = createDeferred();
		const firstStream = new MockAssistantStream();
		let providerCalls = 0;
		const agent = new Agent({
			streamFn: () => {
				providerCalls++;
				if (providerCalls === 1) {
					firstProviderStarted.resolve();
					return firstStream;
				}
				return completedStream(createAssistantMessage([{ type: "text", text: "second generation" }]));
			},
		});

		const firstPrompt = agent.prompt("first");
		await firstProviderStarted.promise;
		agent.abort("test abort");
		await expectToSettleWithin(firstPrompt);
		const abortedMessage = agent.state.messages.at(-1);
		expect(isAssistantMessage(abortedMessage)).toBe(true);
		if (!isAssistantMessage(abortedMessage)) throw new Error("Expected assistant message");
		expect(abortedMessage.stopReason).toBe("aborted");

		await agent.prompt("second");
		const messagesAfterSecondPrompt = agent.state.messages.slice();
		firstStream.end(createAssistantMessage([{ type: "text", text: "late first generation" }]));
		await expectToSettleWithin(agent.waitForPhysicalSettlement());
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(agent.state.messages).toEqual(messagesAfterSecondPrompt);
		expect(providerCalls).toBe(2);
		const secondMessage = agent.state.messages.at(-1);
		expect(isAssistantMessage(secondMessage)).toBe(true);
		if (!isAssistantMessage(secondMessage)) throw new Error("Expected assistant message");
		expect(secondMessage.content).toEqual([{ type: "text", text: "second generation" }]);
	});

	it("does not let a never-settling lifecycle listener retain logical ownership", async () => {
		const listenerStarted = createDeferred();
		const releaseListener = createDeferred();
		const eventTypes: AgentEvent["type"][] = [];
		const agent = new Agent({
			streamFn: () => completedStream(createAssistantMessage([{ type: "text", text: "unused" }])),
		});
		agent.subscribe(async (event) => {
			eventTypes.push(event.type);
			if (event.type === "agent_start") {
				listenerStarted.resolve();
				await releaseListener.promise;
			}
		});

		const prompt = agent.prompt("hello");
		await listenerStarted.promise;
		agent.requestAbort("test abort");
		agent.requestAbort("duplicate abort");
		await expectToSettleWithin(Promise.all([prompt, agent.waitForLogicalIdle(), agent.waitForIdle()]));
		expect(agent.state.isStreaming).toBe(false);
		expect(eventTypes).toContain("turn_end");
		expect(eventTypes).toContain("agent_end");

		const physicalSettlement = agent.waitForPhysicalSettlement();
		await expectToRemainPending(physicalSettlement);
		releaseListener.resolve();
		await expectToSettleWithin(physicalSettlement);
	});
});
