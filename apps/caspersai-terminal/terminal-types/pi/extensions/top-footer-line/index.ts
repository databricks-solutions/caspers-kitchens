import type { AssistantMessage } from "@mariozechner/pi-ai";
import type { ContextUsage, ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import { truncateToWidth } from "@mariozechner/pi-tui";

const WIDGET_KEY = "top-footer-line";

const ICONS = {
	pi: "π",
	cpu: "󰍛",
	folder: "󰉋",
	branch: "",
	context: "󰘚",
} as const;

type Totals = {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	cost: number;
	usingSubscription: boolean;
};

function formatTokens(count: number): string {
	if (count < 1000) return `${count}`;
	if (count < 10000) return `${(count / 1000).toFixed(1)}k`;
	if (count < 1000000) return `${Math.round(count / 1000)}k`;
	if (count < 10000000) return `${(count / 1000000).toFixed(1)}M`;
	return `${Math.round(count / 1000000)}M`;
}

function toHomePath(path: string): string {
	const home = process.env.HOME || process.env.USERPROFILE;
	if (home && path.startsWith(home)) {
		return `~${path.slice(home.length)}`;
	}
	return path;
}

function truncateMiddle(text: string, maxLen: number): string {
	if (text.length <= maxLen) return text;
	if (maxLen <= 3) return text.slice(0, maxLen);
	const left = Math.ceil((maxLen - 3) / 2);
	const right = Math.floor((maxLen - 3) / 2);
	return `${text.slice(0, left)}...${text.slice(text.length - right)}`;
}

function getTotals(ctx: ExtensionContext): Totals {
	let input = 0;
	let output = 0;
	let cacheRead = 0;
	let cacheWrite = 0;
	let cost = 0;

	for (const entry of ctx.sessionManager.getEntries()) {
		if (entry.type === "message" && entry.message.role === "assistant") {
			const msg = entry.message as AssistantMessage;
			input += msg.usage.input;
			output += msg.usage.output;
			cacheRead += msg.usage.cacheRead;
			cacheWrite += msg.usage.cacheWrite;
			cost += msg.usage.cost.total;
		}
	}

	return {
		input,
		output,
		cacheRead,
		cacheWrite,
		cost,
		usingSubscription: ctx.model ? ctx.modelRegistry.isUsingOAuth(ctx.model) : false,
	};
}

function formatContextUsage(usage: ContextUsage | undefined): string {
	if (!usage || usage.contextWindow <= 0) return "?";
	if (usage.percent === null) return `?/${formatTokens(usage.contextWindow)}`;
	return `${usage.percent.toFixed(1)}%/${formatTokens(usage.contextWindow)}`;
}

export default function topFooterLineExtension(pi: ExtensionAPI) {
	let modelId = "no-model";
	let cwd = toHomePath(process.cwd());
	let branch = "no-git";
	let contextUsage: ContextUsage | undefined;
	let totals: Totals = {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		cost: 0,
		usingSubscription: false,
	};
	let requestRender: (() => void) | undefined;

	const refresh = (ctx: ExtensionContext) => {
		modelId = ctx.model?.id ?? "no-model";
		cwd = toHomePath(ctx.cwd);
		contextUsage = ctx.getContextUsage();
		totals = getTotals(ctx);
		requestRender?.();
	};

	const installUi = (ctx: ExtensionContext) => {
		if (!ctx.hasUI) return;

		ctx.ui.setWidget(
			WIDGET_KEY,
			(tui, theme) => {
				requestRender = () => tui.requestRender();

				return {
					render(width: number): string[] {
						const sep = theme.fg("dim", " > ");
						const thinking = pi.getThinkingLevel();
						const shortCwd = truncateMiddle(cwd, 38);

						const contextRaw = formatContextUsage(contextUsage);
						const contextColored =
							contextUsage?.percent !== null && contextUsage?.percent !== undefined
								? contextUsage.percent > 90
									? theme.fg("error", contextRaw)
									: contextUsage.percent > 70
										? theme.fg("warning", contextRaw)
										: theme.fg("dim", contextRaw)
								: theme.fg("dim", contextRaw);

						const tokenParts: string[] = [];
						if (totals.input) tokenParts.push(`↑${formatTokens(totals.input)}`);
						if (totals.output) tokenParts.push(`↓${formatTokens(totals.output)}`);
						if (totals.cacheRead) tokenParts.push(`R${formatTokens(totals.cacheRead)}`);
						if (totals.cacheWrite) tokenParts.push(`W${formatTokens(totals.cacheWrite)}`);

						const cost = `${totals.cost.toFixed(3)}${totals.usingSubscription ? " (sub)" : ""}`;
						const info = tokenParts.length > 0 ? `${cost} • ${tokenParts.join(" ")}` : cost;

						const segments = [
							theme.fg("accent", ICONS.pi),
							`${theme.fg("accent", ICONS.cpu)} ${theme.fg("text", modelId)}`,
							theme.fg("muted", `think:${thinking}`),
							`${theme.fg("accent", ICONS.folder)} ${theme.fg("text", shortCwd)}`,
							`${theme.fg("accent", ICONS.branch)} ${theme.fg("text", branch)}`,
							`${theme.fg("accent", ICONS.context)} ${contextColored}`,
							`${theme.fg("muted", "$")}${theme.fg("text", ` ${info}`)}`,
						];

						return [truncateToWidth(segments.join(sep), width)];
					},
					invalidate(): void {},
				};
			},
			{ placement: "aboveEditor" },
		);

		// Hide built-in footer so the single-line widget above the editor is the only status line.
		ctx.ui.setFooter((tui, _theme, footerData) => {
			branch = footerData.getGitBranch() || "no-git";
			const unsubscribe = footerData.onBranchChange(() => {
				branch = footerData.getGitBranch() || "no-git";
				tui.requestRender();
			});

			return {
				render(): string[] {
					return [];
				},
				invalidate(): void {},
				dispose: unsubscribe,
			};
		});
	};

	pi.on("session_start", async (_event, ctx) => {
		installUi(ctx);
		refresh(ctx);
	});

	pi.on("session_switch", async (_event, ctx) => {
		installUi(ctx);
		refresh(ctx);
	});

	pi.on("model_select", async (_event, ctx) => {
		refresh(ctx);
	});

	pi.on("turn_end", async (_event, ctx) => {
		refresh(ctx);
	});

	pi.on("agent_end", async (_event, ctx) => {
		refresh(ctx);
	});

	pi.on("session_compact", async (_event, ctx) => {
		refresh(ctx);
	});
}
