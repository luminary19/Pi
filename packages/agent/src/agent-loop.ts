/**
 * Agent loop that works with AgentMessage throughout.
 * Transforms to Message[] only at the LLM call boundary.
 */

import {
	type AssistantMessage,
	type AssistantMessageEvent,
	type AssistantMessageEventStream,
	type Context,
	EventStream,
	streamSimple,
	type ToolResultMessage,
	validateToolArguments,
} from "@earendil-works/pi-ai/compat";
import type {
	AgentContext,
	AgentEvent,
	AgentLoopConfig,
	AgentMessage,
	AgentTool,
	AgentToolCall,
	AgentToolResult,
	StreamFn,
} from "./types.ts";

export type AgentEventSink = (event: AgentEvent) => Promise<void> | void;

const ABORT_ERROR_MESSAGE = "Operation aborted";

const EMPTY_USAGE = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

class AgentLoopAbortedError extends Error {
	constructor() {
		super(ABORT_ERROR_MESSAGE);
		this.name = "AgentLoopAbortedError";
	}
}

type AbortableOutcome<T> =
	| { status: "fulfilled"; value: T }
	| { status: "rejected"; reason: unknown }
	| { status: "aborted" };

function registerPhysicalWork(config: AgentLoopConfig, work: PromiseLike<unknown>, label: string): void {
	const promise = Promise.resolve(work);
	// Install an observer even when no owner supplied a physical-work tracker.
	void promise.catch(() => {});
	config.registerPhysicalWork?.(promise, label);
}

function settleAbortable<T>(
	start: () => T | PromiseLike<T>,
	signal: AbortSignal | undefined,
	config: AgentLoopConfig,
	label: string,
): Promise<AbortableOutcome<T>> {
	if (signal?.aborted) {
		return Promise.resolve({ status: "aborted" });
	}

	let work: Promise<T>;
	try {
		work = Promise.resolve(start());
	} catch (reason) {
		return Promise.resolve({ status: "rejected", reason });
	}

	if (!signal) {
		return work.then(
			(value) => ({ status: "fulfilled", value }),
			(reason: unknown) => ({ status: "rejected", reason }),
		);
	}

	return new Promise((resolve) => {
		let settled = false;
		const finish = (outcome: AbortableOutcome<T>) => {
			if (settled) return;
			settled = true;
			signal.removeEventListener("abort", onAbort);
			resolve(outcome);
		};
		const onAbort = () => {
			if (settled) return;
			registerPhysicalWork(config, work, label);
			finish({ status: "aborted" });
		};

		signal.addEventListener("abort", onAbort, { once: true });
		if (signal.aborted) {
			onAbort();
			return;
		}
		void work.then(
			(value) => {
				if (signal.aborted) {
					onAbort();
					return;
				}
				finish({ status: "fulfilled", value });
			},
			(reason: unknown) => finish({ status: "rejected", reason }),
		);
	});
}

async function awaitAbortable<T>(
	start: () => T | PromiseLike<T>,
	signal: AbortSignal | undefined,
	config: AgentLoopConfig,
	label: string,
): Promise<T> {
	const outcome = await settleAbortable(start, signal, config, label);
	if (outcome.status === "aborted") throw new AgentLoopAbortedError();
	if (outcome.status === "rejected") throw outcome.reason;
	return outcome.value;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (signal?.aborted) throw new AgentLoopAbortedError();
}

function createAbortedAssistantMessage(
	config: AgentLoopConfig,
	partialMessage?: AssistantMessage | null,
): AssistantMessage {
	return {
		role: "assistant",
		content: partialMessage?.content.filter((part) => part.type !== "toolCall") ?? [],
		api: partialMessage?.api ?? config.model.api,
		provider: partialMessage?.provider ?? config.model.provider,
		model: partialMessage?.model ?? config.model.id,
		usage: partialMessage?.usage ?? EMPTY_USAGE,
		stopReason: "aborted",
		errorMessage: ABORT_ERROR_MESSAGE,
		timestamp: Date.now(),
	};
}

async function finalizeAbortedAssistantMessage(
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	partialMessage: AssistantMessage | null,
	addedPartial: boolean,
): Promise<AssistantMessage> {
	const abortedMessage = createAbortedAssistantMessage(config, partialMessage);
	if (addedPartial) {
		context.messages[context.messages.length - 1] = abortedMessage;
	} else {
		context.messages.push(abortedMessage);
		await emit({ type: "message_start", message: { ...abortedMessage } });
	}
	await emit({ type: "message_end", message: abortedMessage });
	return abortedMessage;
}

/**
 * Start an agent loop with a new prompt message.
 * The prompt is added to the context and events are emitted for it.
 */
export function agentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	const stream = createAgentStream();

	void runAgentLoop(
		prompts,
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

/**
 * Continue an agent loop from the current context without adding a new message.
 * Used for retries - context already has user message or tool results.
 *
 * **Important:** The last message in context must convert to a `user` or `toolResult` message
 * via `convertToLlm`. If it doesn't, the LLM provider will reject the request.
 * This cannot be validated here since `convertToLlm` is only called once per turn.
 */
export function agentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	const stream = createAgentStream();

	void runAgentLoopContinue(
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

export async function runAgentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): Promise<AgentMessage[]> {
	const newMessages: AgentMessage[] = [...prompts];
	const currentContext: AgentContext = {
		...context,
		messages: [...context.messages, ...prompts],
	};

	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });
	for (const prompt of prompts) {
		await emit({ type: "message_start", message: prompt });
		await emit({ type: "message_end", message: prompt });
	}

	await runLoop(currentContext, newMessages, config, signal, emit, streamFn);
	return newMessages;
}

export async function runAgentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): Promise<AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	const newMessages: AgentMessage[] = [];
	const currentContext: AgentContext = { ...context };

	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });

	await runLoop(currentContext, newMessages, config, signal, emit, streamFn);
	return newMessages;
}

function createAgentStream(): EventStream<AgentEvent, AgentMessage[]> {
	return new EventStream<AgentEvent, AgentMessage[]>(
		(event: AgentEvent) => event.type === "agent_end",
		(event: AgentEvent) => (event.type === "agent_end" ? event.messages : []),
	);
}

/**
 * Main loop logic shared by agentLoop and agentLoopContinue.
 */
async function runLoop(
	initialContext: AgentContext,
	newMessages: AgentMessage[],
	initialConfig: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFn?: StreamFn,
): Promise<void> {
	let currentContext = initialContext;
	let config = initialConfig;
	let firstTurn = true;
	// runAgentLoop/runAgentLoopContinue emit the first turn_start before entering here.
	let turnOpen = true;
	let turnMessage: AssistantMessage | undefined;
	let turnToolResults: ToolResultMessage[] = [];

	try {
		// Check for steering messages at start (user may have typed while waiting).
		let pendingMessages: AgentMessage[] = config.getSteeringMessages
			? await awaitAbortable(config.getSteeringMessages, signal, config, "initial steering poll")
			: [];

		// Outer loop: continues when queued follow-up messages arrive after agent would stop.
		while (true) {
			let hasMoreToolCalls = true;

			// Inner loop: process tool calls and steering messages.
			while (hasMoreToolCalls || pendingMessages.length > 0) {
				throwIfAborted(signal);
				if (!firstTurn) {
					await emit({ type: "turn_start" });
				} else {
					firstTurn = false;
				}
				turnOpen = true;
				turnMessage = undefined;
				turnToolResults = [];
				throwIfAborted(signal);

				// Process pending messages (inject before next assistant response).
				if (pendingMessages.length > 0) {
					for (const message of pendingMessages) {
						throwIfAborted(signal);
						await emit({ type: "message_start", message });
						await emit({ type: "message_end", message });
						currentContext.messages.push(message);
						newMessages.push(message);
					}
					pendingMessages = [];
				}

				throwIfAborted(signal);
				const message = await streamAssistantResponse(currentContext, config, signal, emit, streamFn);
				if (signal?.aborted && message.stopReason !== "aborted") {
					Object.assign(message, createAbortedAssistantMessage(config, message));
					await emit({ type: "message_end", message });
				}
				turnMessage = message;
				newMessages.push(message);

				if (message.stopReason === "error" || message.stopReason === "aborted") {
					await emit({ type: "turn_end", message, toolResults: [] });
					turnOpen = false;
					await emit({ type: "agent_end", messages: newMessages });
					return;
				}

				const toolCalls = message.content.filter((content) => content.type === "toolCall");
				const toolResults: ToolResultMessage[] = [];
				hasMoreToolCalls = false;
				if (toolCalls.length > 0) {
					const executedToolBatch =
						message.stopReason === "length"
							? await failToolCallsFromTruncatedMessage(toolCalls, emit)
							: await executeToolCalls(currentContext, message, config, signal, emit);
					toolResults.push(...executedToolBatch.messages);
					hasMoreToolCalls = !executedToolBatch.terminate;

					for (const result of toolResults) {
						currentContext.messages.push(result);
						newMessages.push(result);
					}
				}
				turnToolResults = toolResults;

				await emit({ type: "turn_end", message, toolResults });
				turnOpen = false;
				if (signal?.aborted) {
					await emit({ type: "agent_end", messages: newMessages });
					return;
				}

				const nextTurnContext = {
					message,
					toolResults,
					context: currentContext,
					newMessages,
				};
				const nextTurnSnapshot = config.prepareNextTurn
					? await awaitAbortable(
							() => config.prepareNextTurn?.(nextTurnContext),
							signal,
							config,
							"prepare next turn",
						)
					: undefined;
				if (nextTurnSnapshot) {
					currentContext = nextTurnSnapshot.context ?? currentContext;
					config = {
						...config,
						model: nextTurnSnapshot.model ?? config.model,
						reasoning:
							nextTurnSnapshot.thinkingLevel === undefined
								? config.reasoning
								: nextTurnSnapshot.thinkingLevel === "off"
									? undefined
									: nextTurnSnapshot.thinkingLevel,
					};
				}

				throwIfAborted(signal);
				if (
					config.shouldStopAfterTurn &&
					(await awaitAbortable(
						() =>
							config.shouldStopAfterTurn?.({
								message,
								toolResults,
								context: currentContext,
								newMessages,
							}),
						signal,
						config,
						"stop-after-turn hook",
					))
				) {
					await emit({ type: "agent_end", messages: newMessages });
					return;
				}

				throwIfAborted(signal);
				pendingMessages = config.getSteeringMessages
					? await awaitAbortable(config.getSteeringMessages, signal, config, "steering poll")
					: [];
			}

			throwIfAborted(signal);
			const followUpMessages = config.getFollowUpMessages
				? await awaitAbortable(config.getFollowUpMessages, signal, config, "follow-up poll")
				: [];
			if (followUpMessages.length > 0) {
				pendingMessages = followUpMessages;
				continue;
			}
			break;
		}

		await emit({ type: "agent_end", messages: newMessages });
	} catch (error) {
		if (!(error instanceof AgentLoopAbortedError) && !signal?.aborted) throw error;
		if (turnOpen) {
			if (!turnMessage) {
				turnMessage = await finalizeAbortedAssistantMessage(currentContext, config, emit, null, false);
				newMessages.push(turnMessage);
			}
			await emit({ type: "turn_end", message: turnMessage, toolResults: turnToolResults });
		}
		await emit({ type: "agent_end", messages: newMessages });
	}
}

/**
 * Stream an assistant response from the LLM.
 * This is where AgentMessage[] gets transformed to Message[] for the LLM.
 */
async function streamAssistantResponse(
	context: AgentContext,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFn?: StreamFn,
): Promise<AssistantMessage> {
	let partialMessage: AssistantMessage | null = null;
	let addedPartial = false;
	let response: AssistantMessageEventStream | undefined;
	let iterator: AsyncIterator<AssistantMessageEvent> | undefined;

	try {
		throwIfAborted(signal);
		let messages = context.messages;
		if (config.transformContext) {
			messages = await awaitAbortable(
				() => config.transformContext?.(messages, signal) ?? messages,
				signal,
				config,
				"context transform",
			);
		}

		throwIfAborted(signal);
		const llmMessages = await awaitAbortable(
			() => config.convertToLlm(messages),
			signal,
			config,
			"message conversion",
		);
		const llmContext: Context = {
			systemPrompt: context.systemPrompt,
			messages: llmMessages,
			tools: context.tools,
		};

		throwIfAborted(signal);
		const resolvedApiKey =
			(config.getApiKey
				? await awaitAbortable(
						() => config.getApiKey?.(config.model.provider),
						signal,
						config,
						"API key resolution",
					)
				: undefined) || config.apiKey;
		const streamFunction = streamFn || streamSimple;
		response = await awaitAbortable(
			() =>
				streamFunction(config.model, llmContext, {
					...config,
					apiKey: resolvedApiKey,
					signal,
				}),
			signal,
			config,
			"provider stream construction",
		);

		throwIfAborted(signal);
		iterator = response[Symbol.asyncIterator]();
		while (true) {
			const next = await awaitAbortable(
				() => iterator?.next() ?? Promise.resolve({ done: true as const, value: undefined }),
				signal,
				config,
				"provider stream iteration",
			);
			if (next.done) break;
			const event = next.value;
			if (!event || typeof event !== "object" || !("type" in event)) continue;

			switch (event.type) {
				case "start":
					partialMessage = event.partial;
					context.messages.push(partialMessage);
					addedPartial = true;
					await emit({ type: "message_start", message: { ...partialMessage } });
					break;

				case "text_start":
				case "text_delta":
				case "text_end":
				case "thinking_start":
				case "thinking_delta":
				case "thinking_end":
				case "toolcall_start":
				case "toolcall_delta":
				case "toolcall_end":
					if (partialMessage) {
						partialMessage = event.partial;
						context.messages[context.messages.length - 1] = partialMessage;
						await emit({
							type: "message_update",
							assistantMessageEvent: event,
							message: { ...partialMessage },
						});
					}
					break;

				case "done":
				case "error": {
					const finalMessage = await awaitAbortable(
						() => response?.result() ?? Promise.reject(new Error("Provider stream unavailable")),
						signal,
						config,
						"provider final result",
					);
					if (addedPartial) {
						context.messages[context.messages.length - 1] = finalMessage;
					} else {
						context.messages.push(finalMessage);
						await emit({ type: "message_start", message: { ...finalMessage } });
					}
					await emit({ type: "message_end", message: finalMessage });
					return finalMessage;
				}
			}
		}

		const finalMessage = await awaitAbortable(
			() => response?.result() ?? Promise.reject(new Error("Provider stream unavailable")),
			signal,
			config,
			"provider final result",
		);
		if (addedPartial) {
			context.messages[context.messages.length - 1] = finalMessage;
		} else {
			context.messages.push(finalMessage);
			await emit({ type: "message_start", message: { ...finalMessage } });
		}
		await emit({ type: "message_end", message: finalMessage });
		return finalMessage;
	} catch (error) {
		if (!(error instanceof AgentLoopAbortedError) && !signal?.aborted) throw error;
		if (response) registerPhysicalWork(config, response.result(), "provider stream settlement");
		if (iterator?.return) {
			try {
				registerPhysicalWork(config, iterator.return(), "provider iterator return");
			} catch (returnError) {
				registerPhysicalWork(config, Promise.reject(returnError), "provider iterator return");
			}
		}
		return finalizeAbortedAssistantMessage(context, config, emit, partialMessage, addedPartial);
	}
}

/**
 * Fail all tool calls from an assistant message that was truncated by the
 * output token limit. Streamed tool-call arguments are finalized with a
 * best-effort JSON salvage parser, so a truncated message can yield tool calls
 * whose arguments parse and validate but are silently incomplete. None of them
 * are safe to execute; report each as an error so the model can re-issue them.
 */
async function failToolCallsFromTruncatedMessage(
	toolCalls: AgentToolCall[],
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const messages: ToolResultMessage[] = [];
	for (const toolCall of toolCalls) {
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});
		const finalized: FinalizedToolCallOutcome = {
			toolCall,
			result: createErrorToolResult(
				`Tool call "${toolCall.name}" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.`,
			),
			isError: true,
			aborted: false,
		};
		await emitToolExecutionEnd(finalized, emit);
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		messages.push(toolResultMessage);
	}
	return { messages, terminate: false };
}

/**
 * Execute tool calls from an assistant message.
 */
async function executeToolCalls(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const toolCalls = assistantMessage.content.filter((c) => c.type === "toolCall");
	const hasSequentialToolCall = toolCalls.some(
		(tc) => currentContext.tools?.find((t) => t.name === tc.name)?.executionMode === "sequential",
	);
	if (config.toolExecution === "sequential" || hasSequentialToolCall) {
		return executeToolCallsSequential(currentContext, assistantMessage, toolCalls, config, signal, emit);
	}
	return executeToolCallsParallel(currentContext, assistantMessage, toolCalls, config, signal, emit);
}

type ExecutedToolCallBatch = {
	messages: ToolResultMessage[];
	terminate: boolean;
};

async function executeToolCallsSequential(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const finalizedCalls: FinalizedToolCallOutcome[] = [];
	const messages: ToolResultMessage[] = [];

	for (let index = 0; index < toolCalls.length; index++) {
		const toolCall = toolCalls[index];
		if (signal?.aborted) {
			for (const unstartedToolCall of toolCalls.slice(index)) {
				const finalized = createAbortedFinalizedToolCall(unstartedToolCall);
				const toolResultMessage = createToolResultMessage(finalized);
				await emitToolResultMessage(toolResultMessage, emit);
				finalizedCalls.push(finalized);
				messages.push(toolResultMessage);
			}
			break;
		}

		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		let finalized: FinalizedToolCallOutcome;
		if (preparation.kind === "immediate") {
			finalized = {
				toolCall,
				result: preparation.result,
				isError: preparation.isError,
				aborted: preparation.aborted,
			};
		} else {
			const executed = await executePreparedToolCall(preparation, signal, emit, config);
			finalized = await finalizeExecutedToolCall(
				currentContext,
				assistantMessage,
				preparation,
				executed,
				config,
				signal,
			);
		}

		await emitToolExecutionEnd(finalized, emit);
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		finalizedCalls.push(finalized);
		messages.push(toolResultMessage);
	}

	return {
		messages,
		terminate: shouldTerminateToolBatch(finalizedCalls),
	};
}

async function executeToolCallsParallel(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallBatch> {
	const finalizedCalls: FinalizedToolCallEntry[] = [];
	let preparedCount = 0;

	for (const toolCall of toolCalls) {
		if (signal?.aborted) break;
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		preparedCount++;
		if (preparation.kind === "immediate") {
			const finalized = {
				toolCall,
				result: preparation.result,
				isError: preparation.isError,
				aborted: preparation.aborted,
			} satisfies FinalizedToolCallOutcome;
			await emitToolExecutionEnd(finalized, emit);
			finalizedCalls.push(finalized);
			if (signal?.aborted) break;
			continue;
		}

		finalizedCalls.push(async () => {
			const executed = await executePreparedToolCall(preparation, signal, emit, config);
			const finalized = await finalizeExecutedToolCall(
				currentContext,
				assistantMessage,
				preparation,
				executed,
				config,
				signal,
			);
			await emitToolExecutionEnd(finalized, emit);
			return finalized;
		});
		if (signal?.aborted) break;
	}

	for (const unstartedToolCall of toolCalls.slice(preparedCount)) {
		finalizedCalls.push(createAbortedFinalizedToolCall(unstartedToolCall));
	}

	// These closures are abort-bounded logical executions. Their underlying physical
	// work is registered separately when abort wins.
	const orderedFinalizedCalls = await Promise.all(
		finalizedCalls.map((entry) => (typeof entry === "function" ? entry() : Promise.resolve(entry))),
	);
	const messages: ToolResultMessage[] = [];
	for (const finalized of orderedFinalizedCalls) {
		const toolResultMessage = createToolResultMessage(finalized);
		await emitToolResultMessage(toolResultMessage, emit);
		messages.push(toolResultMessage);
	}

	return {
		messages,
		terminate: shouldTerminateToolBatch(orderedFinalizedCalls),
	};
}

type PreparedToolCall = {
	kind: "prepared";
	toolCall: AgentToolCall;
	tool: AgentTool<any>;
	args: unknown;
};

type ImmediateToolCallOutcome = {
	kind: "immediate";
	result: AgentToolResult<any>;
	isError: boolean;
	aborted: boolean;
};

type ExecutedToolCallOutcome = {
	result: AgentToolResult<any>;
	isError: boolean;
	aborted: boolean;
};

type FinalizedToolCallOutcome = {
	toolCall: AgentToolCall;
	result: AgentToolResult<any>;
	isError: boolean;
	aborted: boolean;
};

type FinalizedToolCallEntry = FinalizedToolCallOutcome | (() => Promise<FinalizedToolCallOutcome>);

function createAbortedFinalizedToolCall(toolCall: AgentToolCall): FinalizedToolCallOutcome {
	return {
		toolCall,
		result: createErrorToolResult(ABORT_ERROR_MESSAGE),
		isError: true,
		aborted: true,
	};
}

function createAbortedImmediateToolCall(): ImmediateToolCallOutcome {
	return {
		kind: "immediate",
		result: createErrorToolResult(ABORT_ERROR_MESSAGE),
		isError: true,
		aborted: true,
	};
}

function shouldTerminateToolBatch(finalizedCalls: FinalizedToolCallOutcome[]): boolean {
	return finalizedCalls.length > 0 && finalizedCalls.every((finalized) => finalized.result.terminate === true);
}

function prepareToolCallArguments(tool: AgentTool<any>, toolCall: AgentToolCall): AgentToolCall {
	if (!tool.prepareArguments) {
		return toolCall;
	}
	const preparedArguments = tool.prepareArguments(toolCall.arguments);
	if (preparedArguments === toolCall.arguments) {
		return toolCall;
	}
	return {
		...toolCall,
		arguments: preparedArguments as Record<string, any>,
	};
}

async function prepareToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCall: AgentToolCall,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
): Promise<PreparedToolCall | ImmediateToolCallOutcome> {
	if (signal?.aborted) return createAbortedImmediateToolCall();
	const tool = currentContext.tools?.find((candidate) => candidate.name === toolCall.name);
	if (!tool) {
		return {
			kind: "immediate",
			result: createErrorToolResult(`Tool ${toolCall.name} not found`),
			isError: true,
			aborted: false,
		};
	}

	try {
		const preparedToolCall = prepareToolCallArguments(tool, toolCall);
		const validatedArgs = validateToolArguments(tool, preparedToolCall);
		if (config.beforeToolCall) {
			const beforeOutcome = await settleAbortable(
				() =>
					config.beforeToolCall?.(
						{
							assistantMessage,
							toolCall,
							args: validatedArgs,
							context: currentContext,
						},
						signal,
					),
				signal,
				config,
				`beforeToolCall:${toolCall.name}`,
			);
			if (beforeOutcome.status === "aborted") return createAbortedImmediateToolCall();
			if (beforeOutcome.status === "rejected") throw beforeOutcome.reason;
			if (beforeOutcome.value?.block) {
				return {
					kind: "immediate",
					result: createErrorToolResult(beforeOutcome.value.reason || "Tool execution was blocked"),
					isError: true,
					aborted: false,
				};
			}
		}
		if (signal?.aborted) return createAbortedImmediateToolCall();
		return {
			kind: "prepared",
			toolCall,
			tool,
			args: validatedArgs,
		};
	} catch (error) {
		if (signal?.aborted) return createAbortedImmediateToolCall();
		return {
			kind: "immediate",
			result: createErrorToolResult(error instanceof Error ? error.message : String(error)),
			isError: true,
			aborted: false,
		};
	}
}

async function executePreparedToolCall(
	prepared: PreparedToolCall,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	config: AgentLoopConfig,
): Promise<ExecutedToolCallOutcome> {
	const updateEvents: Promise<void>[] = [];
	let acceptingUpdates = true;

	const execution = await settleAbortable(
		() =>
			prepared.tool.execute(prepared.toolCall.id, prepared.args as never, signal, (partialResult) => {
				if (!acceptingUpdates) return;
				const updateEvent = Promise.resolve().then(() =>
					emit({
						type: "tool_execution_update",
						toolCallId: prepared.toolCall.id,
						toolName: prepared.toolCall.name,
						args: prepared.toolCall.arguments,
						partialResult,
					}),
				);
				void updateEvent.catch(() => {});
				updateEvents.push(updateEvent);
			}),
		signal,
		config,
		`tool:${prepared.toolCall.name}`,
	);
	acceptingUpdates = false;

	if (execution.status === "aborted") {
		if (updateEvents.length > 0) {
			registerPhysicalWork(config, Promise.allSettled(updateEvents), `tool updates:${prepared.toolCall.name}`);
		}
		return { result: createErrorToolResult(ABORT_ERROR_MESSAGE), isError: true, aborted: true };
	}

	const updateSettlement = await settleAbortable(
		() => Promise.all(updateEvents),
		signal,
		config,
		`tool updates:${prepared.toolCall.name}`,
	);
	if (updateSettlement.status === "aborted") {
		return { result: createErrorToolResult(ABORT_ERROR_MESSAGE), isError: true, aborted: true };
	}

	if (execution.status === "rejected") {
		return {
			result: createErrorToolResult(
				execution.reason instanceof Error ? execution.reason.message : String(execution.reason),
			),
			isError: true,
			aborted: false,
		};
	}
	if (updateSettlement.status === "rejected") {
		return {
			result: createErrorToolResult(
				updateSettlement.reason instanceof Error
					? updateSettlement.reason.message
					: String(updateSettlement.reason),
			),
			isError: true,
			aborted: false,
		};
	}
	return { result: execution.value, isError: false, aborted: false };
}

async function finalizeExecutedToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	prepared: PreparedToolCall,
	executed: ExecutedToolCallOutcome,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
): Promise<FinalizedToolCallOutcome> {
	if (executed.aborted || signal?.aborted) return createAbortedFinalizedToolCall(prepared.toolCall);
	let result = executed.result;
	let isError = executed.isError;

	if (config.afterToolCall) {
		const afterOutcome = await settleAbortable(
			() =>
				config.afterToolCall?.(
					{
						assistantMessage,
						toolCall: prepared.toolCall,
						args: prepared.args,
						result,
						isError,
						context: currentContext,
					},
					signal,
				),
			signal,
			config,
			`afterToolCall:${prepared.toolCall.name}`,
		);
		if (afterOutcome.status === "aborted") return createAbortedFinalizedToolCall(prepared.toolCall);
		if (afterOutcome.status === "rejected") {
			result = createErrorToolResult(
				afterOutcome.reason instanceof Error ? afterOutcome.reason.message : String(afterOutcome.reason),
			);
			isError = true;
		} else if (afterOutcome.value) {
			result = {
				...result,
				content: afterOutcome.value.content ?? result.content,
				details: afterOutcome.value.details ?? result.details,
				terminate: afterOutcome.value.terminate ?? result.terminate,
			};
			isError = afterOutcome.value.isError ?? isError;
		}
	}

	if (signal?.aborted) return createAbortedFinalizedToolCall(prepared.toolCall);
	return {
		toolCall: prepared.toolCall,
		result,
		isError,
		aborted: false,
	};
}

function createErrorToolResult(message: string): AgentToolResult<any> {
	return {
		content: [{ type: "text", text: message }],
		details: {},
	};
}

async function emitToolExecutionEnd(finalized: FinalizedToolCallOutcome, emit: AgentEventSink): Promise<void> {
	await emit({
		type: "tool_execution_end",
		toolCallId: finalized.toolCall.id,
		toolName: finalized.toolCall.name,
		result: finalized.result,
		isError: finalized.isError,
	});
}

function createToolResultMessage(finalized: FinalizedToolCallOutcome): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId: finalized.toolCall.id,
		toolName: finalized.toolCall.name,
		// Untyped tools (JS extensions) can return results without content; normalize
		// so the null never enters session history or provider payloads.
		content: finalized.result.content ?? [],
		details: finalized.result.details,
		...(finalized.result.addedToolNames?.length ? { addedToolNames: finalized.result.addedToolNames } : {}),
		isError: finalized.isError,
		timestamp: Date.now(),
	};
}

async function emitToolResultMessage(toolResultMessage: ToolResultMessage, emit: AgentEventSink): Promise<void> {
	await emit({ type: "message_start", message: toolResultMessage });
	await emit({ type: "message_end", message: toolResultMessage });
}
