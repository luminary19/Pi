import {
	type ImageContent,
	type Message,
	type Model,
	type SimpleStreamOptions,
	streamSimple,
	type TextContent,
	type ThinkingBudgets,
	type Transport,
} from "@earendil-works/pi-ai/compat";
import { runAgentLoop, runAgentLoopContinue } from "./agent-loop.ts";
import type {
	AfterToolCallContext,
	AfterToolCallResult,
	AgentContext,
	AgentEvent,
	AgentLoopConfig,
	AgentLoopTurnUpdate,
	AgentMessage,
	AgentState,
	AgentTool,
	BeforeToolCallContext,
	BeforeToolCallResult,
	PrepareNextTurnContext,
	QueueMode,
	StreamFn,
	ToolExecutionMode,
} from "./types.ts";

export type { QueueMode } from "./types.ts";

function defaultConvertToLlm(messages: AgentMessage[]): Message[] {
	return messages.filter(
		(message) => message.role === "user" || message.role === "assistant" || message.role === "toolResult",
	);
}

const EMPTY_USAGE = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const DEFAULT_MODEL = {
	id: "unknown",
	name: "unknown",
	api: "unknown",
	provider: "unknown",
	baseUrl: "",
	reasoning: false,
	input: [],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 0,
	maxTokens: 0,
} satisfies Model<any>;

type MutableAgentState = Omit<AgentState, "isStreaming" | "streamingMessage" | "pendingToolCalls" | "errorMessage"> & {
	isStreaming: boolean;
	streamingMessage?: AgentMessage;
	pendingToolCalls: Set<string>;
	errorMessage?: string;
};

function createMutableAgentState(
	initialState?: Partial<Omit<AgentState, "pendingToolCalls" | "isStreaming" | "streamingMessage" | "errorMessage">>,
): MutableAgentState {
	let tools = initialState?.tools?.slice() ?? [];
	let messages = initialState?.messages?.slice() ?? [];

	return {
		systemPrompt: initialState?.systemPrompt ?? "",
		model: initialState?.model ?? DEFAULT_MODEL,
		thinkingLevel: initialState?.thinkingLevel ?? "off",
		get tools() {
			return tools;
		},
		set tools(nextTools: AgentTool<any>[]) {
			tools = nextTools.slice();
		},
		get messages() {
			return messages;
		},
		set messages(nextMessages: AgentMessage[]) {
			messages = nextMessages.slice();
		},
		isStreaming: false,
		streamingMessage: undefined,
		pendingToolCalls: new Set<string>(),
		errorMessage: undefined,
	};
}

/** Options for constructing an {@link Agent}. */
export interface AgentOptions {
	initialState?: Partial<Omit<AgentState, "pendingToolCalls" | "isStreaming" | "streamingMessage" | "errorMessage">>;
	convertToLlm?: (messages: AgentMessage[]) => Message[] | Promise<Message[]>;
	transformContext?: (messages: AgentMessage[], signal?: AbortSignal) => Promise<AgentMessage[]>;
	streamFn?: StreamFn;
	getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	onPayload?: SimpleStreamOptions["onPayload"];
	onResponse?: SimpleStreamOptions["onResponse"];
	beforeToolCall?: (context: BeforeToolCallContext, signal?: AbortSignal) => Promise<BeforeToolCallResult | undefined>;
	afterToolCall?: (context: AfterToolCallContext, signal?: AbortSignal) => Promise<AfterToolCallResult | undefined>;
	prepareNextTurn?: (
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	prepareNextTurnWithContext?: (
		context: PrepareNextTurnContext,
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	steeringMode?: QueueMode;
	followUpMode?: QueueMode;
	sessionId?: string;
	thinkingBudgets?: ThinkingBudgets;
	transport?: Transport;
	maxRetryDelayMs?: number;
	toolExecution?: ToolExecutionMode;
}

class PendingMessageQueue {
	private messages: AgentMessage[] = [];
	public mode: QueueMode;

	constructor(mode: QueueMode) {
		this.mode = mode;
	}

	enqueue(message: AgentMessage): void {
		this.messages.push(message);
	}

	hasItems(): boolean {
		return this.messages.length > 0;
	}

	drain(): AgentMessage[] {
		if (this.mode === "all") {
			const drained = this.messages.slice();
			this.messages = [];
			return drained;
		}

		const first = this.messages[0];
		if (!first) {
			return [];
		}
		this.messages = this.messages.slice(1);
		return [first];
	}

	clear(): void {
		this.messages = [];
	}
}

export interface PhysicalWorkFailure {
	generation: number;
	label: string;
	error: unknown;
	timestamp: number;
}

type RunStatus = "running" | "cancel-requested" | "logically-idle";

class PhysicalWorkTracker {
	private pending = 1;
	private rootFinished = false;
	private resolvePromise = () => {};
	private readonly onFailure: (label: string, error: unknown) => void;
	readonly promise = new Promise<void>((resolve) => {
		this.resolvePromise = resolve;
	});

	constructor(onFailure: (label: string, error: unknown) => void) {
		this.onFailure = onFailure;
	}

	add(work: PromiseLike<unknown>, label: string): void {
		this.pending++;
		void Promise.resolve(work).then(
			() => this.completeOne(),
			(error: unknown) => {
				this.onFailure(label, error);
				this.completeOne();
			},
		);
	}

	finishRoot(): void {
		if (this.rootFinished) return;
		this.rootFinished = true;
		this.completeOne();
	}

	private completeOne(): void {
		this.pending--;
		if (this.pending === 0) this.resolvePromise();
	}
}

type ActiveRun = {
	generation: number;
	status: RunStatus;
	promise: Promise<void>;
	resolve: () => void;
	abortController: AbortController;
	physicalWork: PhysicalWorkTracker;
};

/**
 * Stateful wrapper around the low-level agent loop.
 *
 * `Agent` owns the current transcript, emits lifecycle events, executes tools,
 * and exposes queueing APIs for steering and follow-up messages.
 */
export class Agent {
	private _state: MutableAgentState;
	private readonly listeners = new Set<(event: AgentEvent, signal: AbortSignal) => Promise<void> | void>();
	private readonly steeringQueue: PendingMessageQueue;
	private readonly followUpQueue: PendingMessageQueue;

	public convertToLlm: (messages: AgentMessage[]) => Message[] | Promise<Message[]>;
	public transformContext?: (messages: AgentMessage[], signal?: AbortSignal) => Promise<AgentMessage[]>;
	public streamFn: StreamFn;
	public getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	public onPayload?: SimpleStreamOptions["onPayload"];
	public onResponse?: SimpleStreamOptions["onResponse"];
	public beforeToolCall?: (
		context: BeforeToolCallContext,
		signal?: AbortSignal,
	) => Promise<BeforeToolCallResult | undefined>;
	public afterToolCall?: (
		context: AfterToolCallContext,
		signal?: AbortSignal,
	) => Promise<AfterToolCallResult | undefined>;
	public prepareNextTurn?: (
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	public prepareNextTurnWithContext?: (
		context: PrepareNextTurnContext,
		signal?: AbortSignal,
	) => Promise<AgentLoopTurnUpdate | undefined> | AgentLoopTurnUpdate | undefined;
	private activeRun?: ActiveRun;
	private nextGeneration = 0;
	private readonly physicalSettlements = new Set<Promise<void>>();
	private readonly cleanupFailures: PhysicalWorkFailure[] = [];
	/** Session identifier forwarded to providers for cache-aware backends. */
	public sessionId?: string;
	/** Optional per-level thinking token budgets forwarded to the stream function. */
	public thinkingBudgets?: ThinkingBudgets;
	/** Preferred transport forwarded to the stream function. */
	public transport: Transport;
	/** Optional cap for provider-requested retry delays. */
	public maxRetryDelayMs?: number;
	/** Tool execution strategy for assistant messages that contain multiple tool calls. */
	public toolExecution: ToolExecutionMode;

	constructor(options: AgentOptions = {}) {
		this._state = createMutableAgentState(options.initialState);
		this.convertToLlm = options.convertToLlm ?? defaultConvertToLlm;
		this.transformContext = options.transformContext;
		this.streamFn = options.streamFn ?? streamSimple;
		this.getApiKey = options.getApiKey;
		this.onPayload = options.onPayload;
		this.onResponse = options.onResponse;
		this.beforeToolCall = options.beforeToolCall;
		this.afterToolCall = options.afterToolCall;
		this.prepareNextTurn = options.prepareNextTurn;
		this.prepareNextTurnWithContext = options.prepareNextTurnWithContext;
		this.steeringQueue = new PendingMessageQueue(options.steeringMode ?? "one-at-a-time");
		this.followUpQueue = new PendingMessageQueue(options.followUpMode ?? "one-at-a-time");
		this.sessionId = options.sessionId;
		this.thinkingBudgets = options.thinkingBudgets;
		this.transport = options.transport ?? "auto";
		this.maxRetryDelayMs = options.maxRetryDelayMs;
		this.toolExecution = options.toolExecution ?? "parallel";
	}

	/**
	 * Subscribe to agent lifecycle events.
	 *
	 * Listener promises are awaited in subscription order during normal execution.
	 * Once cancellation is requested, pending and terminal-listener work is tracked
	 * physically without retaining logical ownership of the run.
	 */
	subscribe(listener: (event: AgentEvent, signal: AbortSignal) => Promise<void> | void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	/**
	 * Current agent state.
	 *
	 * Assigning `state.tools` or `state.messages` copies the provided top-level array.
	 */
	get state(): AgentState {
		return this._state;
	}

	/** Controls how queued steering messages are drained. */
	set steeringMode(mode: QueueMode) {
		this.steeringQueue.mode = mode;
	}

	get steeringMode(): QueueMode {
		return this.steeringQueue.mode;
	}

	/** Controls how queued follow-up messages are drained. */
	set followUpMode(mode: QueueMode) {
		this.followUpQueue.mode = mode;
	}

	get followUpMode(): QueueMode {
		return this.followUpQueue.mode;
	}

	/** Queue a message to be injected after the current assistant turn finishes. */
	steer(message: AgentMessage): void {
		this.steeringQueue.enqueue(message);
	}

	/** Queue a message to run only after the agent would otherwise stop. */
	followUp(message: AgentMessage): void {
		this.followUpQueue.enqueue(message);
	}

	/** Remove all queued steering messages. */
	clearSteeringQueue(): void {
		this.steeringQueue.clear();
	}

	/** Remove all queued follow-up messages. */
	clearFollowUpQueue(): void {
		this.followUpQueue.clear();
	}

	/** Remove all queued steering and follow-up messages. */
	clearAllQueues(): void {
		this.clearSteeringQueue();
		this.clearFollowUpQueue();
	}

	/** Returns true when either queue still contains pending messages. */
	hasQueuedMessages(): boolean {
		return this.steeringQueue.hasItems() || this.followUpQueue.hasItems();
	}

	/** Active abort signal for the current run, if any. */
	get signal(): AbortSignal | undefined {
		return this.activeRun?.abortController.signal;
	}

	/** Synchronously and idempotently request cancellation of the active generation. */
	requestAbort(reason: unknown = new Error("User aborted")): void {
		const run = this.activeRun;
		if (!run || run.status !== "running") return;
		run.status = "cancel-requested";
		run.abortController.abort(reason);
	}

	/** Abort the current run, if one is active. */
	abort(reason?: unknown): void {
		this.requestAbort(reason);
	}

	/** Resolve when the active generation becomes logically idle. */
	waitForLogicalIdle(): Promise<void> {
		return this.activeRun?.promise ?? Promise.resolve();
	}

	/** Compatibility alias for logical idle. */
	waitForIdle(): Promise<void> {
		return this.waitForLogicalIdle();
	}

	/** Resolve when all currently tracked detached work has physically settled. */
	async waitForPhysicalSettlement(): Promise<void> {
		while (this.physicalSettlements.size > 0) {
			await Promise.all(this.physicalSettlements);
		}
	}

	/** Failures observed from detached physical work. */
	get physicalWorkFailures(): readonly PhysicalWorkFailure[] {
		return this.cleanupFailures.slice();
	}

	/** Clear transcript state, runtime state, and queued messages. */
	reset(): void {
		this._state.messages = [];
		this._state.isStreaming = false;
		this._state.streamingMessage = undefined;
		this._state.pendingToolCalls = new Set<string>();
		this._state.errorMessage = undefined;
		this.clearFollowUpQueue();
		this.clearSteeringQueue();
	}

	/** Start a new prompt from text, a single message, or a batch of messages. */
	async prompt(message: AgentMessage | AgentMessage[]): Promise<void>;
	async prompt(input: string, images?: ImageContent[]): Promise<void>;
	async prompt(input: string | AgentMessage | AgentMessage[], images?: ImageContent[]): Promise<void> {
		if (this.activeRun) {
			throw new Error(
				"Agent is already processing a prompt. Use steer() or followUp() to queue messages, or wait for completion.",
			);
		}
		const messages = this.normalizePromptInput(input, images);
		await this.runPromptMessages(messages);
	}

	/** Continue from the current transcript. The last message must be a user or tool-result message. */
	async continue(): Promise<void> {
		if (this.activeRun) {
			throw new Error("Agent is already processing. Wait for completion before continuing.");
		}

		const lastMessage = this._state.messages[this._state.messages.length - 1];
		if (!lastMessage) {
			throw new Error("No messages to continue from");
		}

		if (lastMessage.role === "assistant") {
			const queuedSteering = this.steeringQueue.drain();
			if (queuedSteering.length > 0) {
				await this.runPromptMessages(queuedSteering, { skipInitialSteeringPoll: true });
				return;
			}

			const queuedFollowUps = this.followUpQueue.drain();
			if (queuedFollowUps.length > 0) {
				await this.runPromptMessages(queuedFollowUps);
				return;
			}

			throw new Error("Cannot continue from message role: assistant");
		}

		await this.runContinuation();
	}

	private normalizePromptInput(
		input: string | AgentMessage | AgentMessage[],
		images?: ImageContent[],
	): AgentMessage[] {
		if (Array.isArray(input)) {
			return input;
		}

		if (typeof input !== "string") {
			return [input];
		}

		const content: Array<TextContent | ImageContent> = [{ type: "text", text: input }];
		if (images && images.length > 0) {
			content.push(...images);
		}
		return [{ role: "user", content, timestamp: Date.now() }];
	}

	private async runPromptMessages(
		messages: AgentMessage[],
		options: { skipInitialSteeringPoll?: boolean } = {},
	): Promise<void> {
		await this.runWithLifecycle(async (run) => {
			await runAgentLoop(
				messages,
				this.createContextSnapshot(),
				this.createLoopConfig(run, options),
				(event) => this.processEvents(event, run),
				run.abortController.signal,
				this.streamFn,
			);
		});
	}

	private async runContinuation(): Promise<void> {
		await this.runWithLifecycle(async (run) => {
			await runAgentLoopContinue(
				this.createContextSnapshot(),
				this.createLoopConfig(run),
				(event) => this.processEvents(event, run),
				run.abortController.signal,
				this.streamFn,
			);
		});
	}

	private createContextSnapshot(): AgentContext {
		return {
			systemPrompt: this._state.systemPrompt,
			messages: this._state.messages.slice(),
			tools: this._state.tools.slice(),
		};
	}

	private createLoopConfig(run: ActiveRun, options: { skipInitialSteeringPoll?: boolean } = {}): AgentLoopConfig {
		let skipInitialSteeringPoll = options.skipInitialSteeringPoll === true;
		const signal = run.abortController.signal;
		return {
			model: this._state.model,
			reasoning: this._state.thinkingLevel === "off" ? undefined : this._state.thinkingLevel,
			sessionId: this.sessionId,
			onPayload: this.onPayload,
			onResponse: this.onResponse,
			transport: this.transport,
			thinkingBudgets: this.thinkingBudgets,
			maxRetryDelayMs: this.maxRetryDelayMs,
			toolExecution: this.toolExecution,
			beforeToolCall: this.beforeToolCall,
			afterToolCall: this.afterToolCall,
			registerPhysicalWork: (work, label) => run.physicalWork.add(work, label),
			prepareNextTurn:
				this.prepareNextTurnWithContext || this.prepareNextTurn
					? async (context) => {
							if (this.prepareNextTurnWithContext) {
								return await this.prepareNextTurnWithContext(context, signal);
							}
							return await this.prepareNextTurn?.(signal);
						}
					: undefined,
			convertToLlm: this.convertToLlm,
			transformContext: this.transformContext,
			getApiKey: this.getApiKey,
			getSteeringMessages: async () => {
				if (skipInitialSteeringPoll) {
					skipInitialSteeringPoll = false;
					return [];
				}
				return this.steeringQueue.drain();
			},
			getFollowUpMessages: async () => this.followUpQueue.drain(),
		};
	}

	private async runWithLifecycle(executor: (run: ActiveRun) => Promise<void>): Promise<void> {
		if (this.activeRun) {
			throw new Error("Agent is already processing.");
		}

		const generation = ++this.nextGeneration;
		const abortController = new AbortController();
		let resolvePromise = () => {};
		const promise = new Promise<void>((resolve) => {
			resolvePromise = resolve;
		});
		const physicalWork = new PhysicalWorkTracker((label, error) => {
			this.cleanupFailures.push({ generation, label, error, timestamp: Date.now() });
		});
		const run: ActiveRun = {
			generation,
			status: "running",
			promise,
			resolve: resolvePromise,
			abortController,
			physicalWork,
		};
		this.activeRun = run;
		this.physicalSettlements.add(physicalWork.promise);
		void physicalWork.promise.then(() => this.physicalSettlements.delete(physicalWork.promise));

		this._state.isStreaming = true;
		this._state.streamingMessage = undefined;
		this._state.errorMessage = undefined;

		try {
			await executor(run);
		} catch (error) {
			await this.handleRunFailure(error, abortController.signal.aborted, run);
		} finally {
			physicalWork.finishRoot();
			this.finishRun(run);
		}
	}

	private async handleRunFailure(error: unknown, aborted: boolean, run: ActiveRun): Promise<void> {
		if (this.activeRun !== run) return;
		const failureMessage = {
			role: "assistant",
			content: [{ type: "text", text: "" }],
			api: this._state.model.api,
			provider: this._state.model.provider,
			model: this._state.model.id,
			usage: EMPTY_USAGE,
			stopReason: aborted ? "aborted" : "error",
			errorMessage: error instanceof Error ? error.message : String(error),
			timestamp: Date.now(),
		} satisfies AgentMessage;
		await this.processEvents({ type: "message_start", message: failureMessage }, run);
		await this.processEvents({ type: "message_end", message: failureMessage }, run);
		await this.processEvents({ type: "turn_end", message: failureMessage, toolResults: [] }, run);
		await this.processEvents({ type: "agent_end", messages: [failureMessage] }, run);
	}

	private finishRun(run: ActiveRun): void {
		if (this.activeRun !== run) return;
		run.status = "logically-idle";
		this._state.isStreaming = false;
		this._state.streamingMessage = undefined;
		this._state.pendingToolCalls = new Set<string>();
		run.resolve();
		this.activeRun = undefined;
	}

	/** Reduce state for one generation, then notify listeners under its cancellation boundary. */
	private async processEvents(event: AgentEvent, run: ActiveRun): Promise<void> {
		if (this.activeRun !== run) return;
		switch (event.type) {
			case "message_start":
				this._state.streamingMessage = event.message;
				break;

			case "message_update":
				this._state.streamingMessage = event.message;
				break;

			case "message_end":
				this._state.streamingMessage = undefined;
				this._state.messages.push(event.message);
				break;

			case "tool_execution_start": {
				const pendingToolCalls = new Set(this._state.pendingToolCalls);
				pendingToolCalls.add(event.toolCallId);
				this._state.pendingToolCalls = pendingToolCalls;
				break;
			}

			case "tool_execution_end": {
				const pendingToolCalls = new Set(this._state.pendingToolCalls);
				pendingToolCalls.delete(event.toolCallId);
				this._state.pendingToolCalls = pendingToolCalls;
				break;
			}

			case "turn_end":
				if (event.message.role === "assistant" && event.message.errorMessage) {
					this._state.errorMessage = event.message.errorMessage;
				}
				break;

			case "agent_end":
				this._state.streamingMessage = undefined;
				break;
		}

		const signal = run.abortController.signal;
		const abortedAtEntry = signal.aborted;
		for (const listener of this.listeners) {
			if (this.activeRun !== run) return;
			let listenerWork: Promise<void>;
			try {
				listenerWork = Promise.resolve(listener(event, signal));
			} catch (error) {
				listenerWork = Promise.reject(error);
			}
			if (abortedAtEntry) {
				run.physicalWork.add(listenerWork, `event listener:${event.type}`);
				continue;
			}
			const outcome = await this.awaitListener(listenerWork, run, event.type);
			if (outcome === "aborted") return;
		}
	}

	private awaitListener(
		work: Promise<void>,
		run: ActiveRun,
		eventType: AgentEvent["type"],
	): Promise<"settled" | "aborted"> {
		const signal = run.abortController.signal;
		if (signal.aborted) {
			run.physicalWork.add(work, `event listener:${eventType}`);
			return Promise.resolve("aborted");
		}
		return new Promise((resolve, reject) => {
			let settled = false;
			const finish = (outcome: "settled" | "aborted") => {
				if (settled) return;
				settled = true;
				signal.removeEventListener("abort", onAbort);
				resolve(outcome);
			};
			const onAbort = () => {
				if (settled) return;
				run.physicalWork.add(work, `event listener:${eventType}`);
				finish("aborted");
			};
			signal.addEventListener("abort", onAbort, { once: true });
			if (signal.aborted) {
				onAbort();
				return;
			}
			void work.then(
				() => finish("settled"),
				(error: unknown) => {
					if (settled) return;
					settled = true;
					signal.removeEventListener("abort", onAbort);
					reject(error);
				},
			);
		});
	}
}
