export function renderLauncherScreen({
  session,
  picker,
  sessionTypes,
  sessionTypesModel,
  textLayout,
}) {
  const { centerVisual, padVisual, wrapVisual } = textLayout;

  const mode = picker.mode || "home";

  const terminalCols = Math.max(40, session.terminal.cols || 80);
  const terminalRows = Math.max(16, session.terminal.rows || 24);
  const maxBoxWidth = Math.max(30, terminalCols - 2);
  const boxWidth = Math.min(84, maxBoxWidth);
  const innerWidth = Math.max(24, boxWidth - 4);

  const colorize = (text, tone) => {
    if (tone === "title") {
      return `\u001b[1;96m${text}\u001b[0m`;
    }
    if (tone === "accent") {
      return `\u001b[94m${text}\u001b[0m`;
    }
    if (tone === "muted") {
      return `\u001b[90m${text}\u001b[0m`;
    }
    return text;
  };

  const frameLine = (text, tone = "plain", options = {}) => {
    const reserveRight = Math.max(0, Number(options.reserveRight || 0));
    const coreWidth = Math.max(0, innerWidth - reserveRight);
    const padded = `${padVisual(text, coreWidth)}${" ".repeat(reserveRight)}`;
    return `│ ${colorize(padded, tone)} │`;
  };

  const maxBoxHeight = Math.max(12, terminalRows - 2);
  const maxInnerHeight = Math.max(10, maxBoxHeight - 2);

  const lines = [];
  lines.push(`┌${"─".repeat(boxWidth - 2)}┐`);

  if (mode === "home") {
    const introTitle = picker.replaceOnSelect
      ? "Launch a terminal session"
      : "Launch another terminal session";

    const introBody = picker.replaceOnSelect
      ? "Welcome. This app provides PTY-backed tabs for Databricks Apps with per-session auth switching."
      : "Pick a profile to open a new tab. Profiles can bootstrap their own CLI via launch.sh.";

    const introTips = "Tip: switch auth per tab via badge, or run dbx-auth in-shell.";

    const wrappedIntro = [...wrapVisual(introBody, innerWidth), ...wrapVisual(introTips, innerWidth)].slice(0, 3);

    const headerLines = [
      { text: centerVisual("Databricks App Terminal", innerWidth), tone: "title" },
      { text: centerVisual(introTitle, innerWidth), tone: "accent" },
      { text: centerVisual("Multi-session terminal runtime", innerWidth), tone: "muted" },
      { text: "", tone: "plain" },
      ...wrappedIntro.map((line) => ({ text: line, tone: "muted" })),
      { text: "", tone: "plain" },
      { text: "Profiles", tone: "accent" },
    ];

    const footerLineCount = 4;
    const typeLineBudget = Math.max(2, maxInnerHeight - headerLines.length - footerLineCount);
    const visibleSlots = Math.max(1, Math.floor(typeLineBudget / 2));

    let startIndex = 0;
    if (sessionTypes.length > visibleSlots) {
      startIndex = picker.selectedIndex - Math.floor(visibleSlots / 2);
      startIndex = Math.max(0, startIndex);
      startIndex = Math.min(startIndex, sessionTypes.length - visibleSlots);
    }

    const endIndex = Math.min(sessionTypes.length, startIndex + visibleSlots);

    for (const line of headerLines) {
      lines.push(frameLine(line.text, line.tone));
    }

    for (let index = startIndex; index < endIndex; index += 1) {
      const type = sessionTypes[index];
      const isSelected = index === picker.selectedIndex;
      const marker = isSelected ? "❯" : " ";
      const shortcut = index < 9 ? String(index + 1) : " ";
      const defaultSuffix = type.default ? " · default" : "";
      const summary = `${marker} [${shortcut}] ${sessionTypesModel.typeLogo(type)} ${type.name}${defaultSuffix}`;
      const detail = `    ${type.id}${type.description ? ` · ${type.description}` : ""}`;

      lines.push(frameLine(summary, isSelected ? "accent" : "plain", { reserveRight: 1 }));
      lines.push(frameLine(detail, "muted", { reserveRight: 1 }));
    }

    const showing = sessionTypes.length > visibleSlots
      ? `Showing ${startIndex + 1}-${endIndex} of ${sessionTypes.length}`
      : `Showing ${sessionTypes.length} profile${sessionTypes.length === 1 ? "" : "s"}`;

    const action = picker.replaceOnSelect
      ? "Selection replaces this launcher tab"
      : "Selection opens a new tab";

    const escapeHint = picker.blocking ? "Esc back" : "Esc close";

    lines.push(frameLine(""));
    lines.push(frameLine(showing, "muted"));
    lines.push(frameLine(action, "muted"));
    lines.push(frameLine(`↑/↓ or j/k navigate · Enter launch · ? help · a about · ${escapeHint}`, "muted"));
  } else if (mode === "help") {
    const helpEscText = picker.blocking
      ? "  Esc              Return to profile list"
      : "  Esc              Close launcher";

    const helpRows = [
      { text: centerVisual("Launcher Help", innerWidth), tone: "title" },
      { text: centerVisual("Keyboard-first terminal UX", innerWidth), tone: "muted" },
      { text: "", tone: "plain" },
      { text: "Navigation", tone: "accent" },
      { text: "  ↑/↓ or j/k      Move profile selection", tone: "plain" },
      { text: "  1..9             Quick launch by row index", tone: "plain" },
      { text: "  Enter            Launch selected profile", tone: "plain" },
      { text: "", tone: "plain" },
      { text: "Panels", tone: "accent" },
      { text: "  ?                Toggle this help panel", tone: "plain" },
      { text: "  a                Toggle About panel", tone: "plain" },
      { text: helpEscText, tone: "plain" },
      { text: "", tone: "plain" },
      { text: "Enter/Backspace returns to profile list.", tone: "muted" },
    ];

    for (const row of helpRows.slice(0, maxInnerHeight)) {
      lines.push(frameLine(row.text, row.tone));
    }
  } else {
    const aboutLines = [
      { text: centerVisual("About Databricks App Terminal", innerWidth), tone: "title" },
      { text: centerVisual("Terminal runtime for Databricks Apps", innerWidth), tone: "muted" },
      { text: "", tone: "plain" },
    ];

    const copy = [
      "This launcher lets you choose a terminal profile before starting a backend shell session.",
      `Configured profiles in this app: ${sessionTypes.length}.`,
      "Each profile can run custom launch.sh setup while sharing the same core auth/session substrate.",
      "Per-tab auth can be switched via badge or dbx-auth command.",
    ];

    for (const paragraph of copy) {
      for (const line of wrapVisual(paragraph, innerWidth)) {
        aboutLines.push({ text: line, tone: "plain" });
      }
      aboutLines.push({ text: "", tone: "plain" });
    }

    const aboutEscText = picker.blocking ? "Esc returns to profile list." : "Esc closes launcher.";
    aboutLines.push({ text: `Press Enter/Backspace to return. ${aboutEscText}`, tone: "muted" });

    for (const row of aboutLines.slice(0, maxInnerHeight)) {
      lines.push(frameLine(row.text, row.tone));
    }
  }

  lines.push(`└${"─".repeat(boxWidth - 2)}┘`);

  const frameHeight = lines.length;
  const topPad = Math.max(0, Math.floor((terminalRows - frameHeight) / 2));
  const leftPad = Math.max(0, Math.floor((terminalCols - boxWidth) / 2));

  const outputLines = [];
  for (let index = 0; index < topPad; index += 1) {
    outputLines.push("");
  }

  const leftPadText = " ".repeat(leftPad);
  for (const line of lines) {
    outputLines.push(`${leftPadText}${line}`);
  }

  return `\u001b[2J\u001b[H${outputLines.join("\r\n")}`;
}
