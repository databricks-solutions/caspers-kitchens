import { useEffect, useRef, useState } from "react"; // useRef kept for endRef scroll anchor
import { marked } from "marked";

// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------
type Theme = "light" | "dark";
const THEME_KEY = "invoice-terminal-theme";

function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem(THEME_KEY) as Theme | null;
    if (stored) return stored;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });
  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    document.documentElement.classList.toggle("light", theme !== "dark");
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);
  return [theme, () => setTheme((t) => (t === "light" ? "dark" : "light"))];
}

// ---------------------------------------------------------------------------
// Data — loaded dynamically from the server manifest
// ---------------------------------------------------------------------------
interface DocRef {
  id: string;           // display ID: "VFP-0089" for invoices, "CK-VFP-2023-001" for contracts
  fullId?: string;      // full ID printed on the document: "INV-VFP-2024-0089"
  type: "invoices" | "contracts";
  file: string;         // actual PDF filename in UC Volume
  label: string;        // same as id for display
  supplier?: string;    // short supplier name
}

interface ManifestInvoice {
  short_id: string;
  invoice_id: string;
  supplier_id: string;
  supplier_name: string;
  invoice_date: string;
  due_date: string;
  status: string;
  total_due: number;
  pdf_filename: string;
}

interface ManifestContract {
  contract_id: string;
  supplier_id: string;
  supplier_name: string;
  category: string;
  pdf_filename: string;
}

interface Manifest {
  invoices: ManifestInvoice[];
  contracts: ManifestContract[];
}

// Mutable maps — populated once the manifest loads
let INVOICE_REFS: Record<string, DocRef> = {};
let CONTRACT_REFS: Record<string, DocRef> = {};

function buildRefs(manifest: Manifest) {
  INVOICE_REFS = {};
  for (const inv of manifest.invoices) {
    INVOICE_REFS[inv.short_id] = {
      id: inv.short_id,
      fullId: inv.invoice_id,
      type: "invoices",
      file: inv.pdf_filename,
      label: inv.short_id,
      supplier: inv.supplier_name.replace(/ (LLC|Inc\.|Co\.|Ltd\.?)$/i, "").trim(),
    };
  }
  CONTRACT_REFS = {};
  for (const con of manifest.contracts) {
    // Key by supplier_id so "VFP contract" regex hits
    CONTRACT_REFS[con.supplier_id] = {
      id: con.contract_id,
      type: "contracts",
      file: con.pdf_filename,
      label: con.supplier_name.replace(/ (LLC|Inc\.|Co\.|Ltd\.?)$/i, "").trim(),
      supplier: con.supplier_name.replace(/ (LLC|Inc\.|Co\.|Ltd\.?)$/i, "").trim(),
    };
  }
}

const SAMPLE_QUESTIONS = [
  "Which invoices are currently past due or disputed?",
  "Show me all invoice exceptions ranked by recoverable amount",
  "Does PCM-0061 match the contracted price for brisket?",
  "What does the HPC contract say about volume discounts?",
  "Is invoice HPC-0103 compliant with our contract?",
  "Give me a full AP summary with all credit memo opportunities",
];

function pdfUrl(ref: DocRef) {
  return `/api/invoice/pdf?type=${ref.type}&file=${encodeURIComponent(ref.file)}`;
}

function extractRefs(text: string): DocRef[] {
  const seen = new Set<string>();
  const refs: DocRef[] = [];
  for (const [id, ref] of Object.entries(INVOICE_REFS)) {
    if (!seen.has(id) && new RegExp(`\\b${id}\\b`).test(text)) { seen.add(id); refs.push(ref); }
  }
  for (const [sid, ref] of Object.entries(CONTRACT_REFS)) {
    const key = `c-${sid}`;
    if (!seen.has(key) && new RegExp(`\\b${sid}\\b[^\\n]{0,30}contract|contract[^\\n]{0,30}\\b${sid}\\b`, "i").test(text)) {
      seen.add(key); refs.push(ref);
    }
  }
  return refs;
}

// ---------------------------------------------------------------------------
// Markdown
// ---------------------------------------------------------------------------
marked.setOptions({ breaks: true });
function Markdown({ text }: { text: string }) {
  const html = marked.parse(text, { async: false }) as string;
  // eslint-disable-next-line react/no-danger
  return <div className="md" dangerouslySetInnerHTML={{ __html: html }} />;
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------
const SESSION_KEY = "invoice-terminal-session-id";
function getOrCreateSessionId() {
  let id = localStorage.getItem(SESSION_KEY);
  if (!id) { id = crypto.randomUUID(); localStorage.setItem(SESSION_KEY, id); }
  return id;
}

function persistSession(id: string) {
  localStorage.setItem(SESSION_KEY, id);
}

type Tab = "chat" | "documents" | "history";

interface Turn {
  turn_id?: number;
  turn_index: number;
  user_message: string;
  agent_response: string;
  latency_ms?: number;
  refs?: DocRef[];
}

interface SessionSummary {
  session_id: string;
  started_at: string;
  turn_count: number;
  first_question: string;
  last_activity: string;
}

// ---------------------------------------------------------------------------
// Shared design tokens (applied inline for portability)
// ---------------------------------------------------------------------------
const S = {
  card: {
    background: "var(--card)",
    border: "1px solid var(--border)",
    borderRadius: "12px",
    boxShadow: "var(--shadow-sm)",
  },
  pill: (active: boolean) => ({
    display: "inline-flex" as const,
    alignItems: "center" as const,
    gap: "4px",
    fontSize: "0.75rem",
    fontWeight: active ? 600 : 500,
    padding: "3px 11px",
    borderRadius: "999px",
    border: `1px solid ${active ? "var(--primary)" : "var(--border)"}`,
    background: active ? "color-mix(in srgb, var(--primary) 10%, transparent)" : "transparent",
    color: active ? "var(--primary)" : "var(--muted-foreground)",
    cursor: "pointer",
    transition: "all 0.15s",
  }),
  iconBtn: {
    width: "34px",
    height: "34px",
    borderRadius: "8px",
    border: "1px solid var(--border)",
    background: "var(--muted)",
    cursor: "pointer",
    display: "flex" as const,
    alignItems: "center" as const,
    justifyContent: "center" as const,
    fontSize: "15px",
    flexShrink: 0 as const,
    transition: "background 0.15s",
  },
};

// ---------------------------------------------------------------------------
// Tab bar
// ---------------------------------------------------------------------------
const TABS: { id: Tab; label: string; icon: string }[] = [
  { id: "chat",      label: "Chat",      icon: "💬" },
  { id: "documents", label: "Documents", icon: "📄" },
  { id: "history",   label: "History",   icon: "🕐" },
];

function TabBar({ active, onSelect }: { active: Tab; onSelect: (t: Tab) => void }) {
  return (
    <div style={{ display: "flex", borderBottom: "1px solid var(--border)", background: "var(--card)", paddingLeft: "8px" }}>
      {TABS.map((t) => {
        const isActive = active === t.id;
        return (
          <button key={t.id} type="button" onClick={() => onSelect(t.id)} style={{
            display: "flex",
            alignItems: "center",
            gap: "6px",
            padding: "12px 18px",
            fontSize: "0.875rem",
            fontWeight: isActive ? 600 : 400,
            color: isActive ? "var(--foreground)" : "var(--muted-foreground)",
            background: "none",
            border: "none",
            borderBottom: isActive ? "2px solid var(--primary)" : "2px solid transparent",
            marginBottom: "-1px",
            cursor: "pointer",
            transition: "color 0.15s",
            letterSpacing: "-0.01em",
          }}>
            <span style={{ fontSize: "13px" }}>{t.icon}</span>
            {t.label}
          </button>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Ref chips
// ---------------------------------------------------------------------------
function RefChip({ ref_, active, onClick }: { ref_: DocRef; active: boolean; onClick: () => void }) {
  return (
    <button type="button" onClick={onClick} style={S.pill(active)}>
      {ref_.type === "invoices" ? "📄" : "📋"} {ref_.label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Chat tab
// ---------------------------------------------------------------------------
function ChatTab({ turns, loading, error, input, setInput, sendMessage, activePdf, onOpenDoc }: {
  turns: Turn[];
  loading: boolean;
  error: string | null;
  input: string;
  setInput: (v: string) => void;
  sendMessage: (override?: string) => Promise<void>;
  activePdf: DocRef | null;
  onOpenDoc: (ref: DocRef) => void;
}) {
  const endRef = useRef<HTMLDivElement>(null);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [turns, loading]);

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, overflow: "hidden" }}>
      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: "28px 20px" }}>
        <div style={{ maxWidth: "740px", margin: "0 auto" }}>

          {/* Empty state */}
          {turns.length === 0 && !loading && (
            <div style={{ paddingTop: "32px", textAlign: "center" }}>
              <div style={{
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                width: "52px",
                height: "52px",
                borderRadius: "14px",
                background: "color-mix(in srgb, var(--primary) 10%, transparent)",
                fontSize: "24px",
                marginBottom: "16px",
              }}>🧾</div>
              <h2 style={{ margin: "0 0 6px", fontSize: "1.05rem", fontWeight: 600, letterSpacing: "-0.02em" }}>
                Procurement Assistant
              </h2>
              <p style={{ margin: "0 0 28px", color: "var(--muted-foreground)", fontSize: "0.875rem" }}>
                Ask about invoices, contracts, exceptions, or supplier spend.
              </p>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", maxWidth: "580px", margin: "0 auto" }}>
                {SAMPLE_QUESTIONS.map((q) => (
                  <button key={q} type="button" onClick={() => void sendMessage(q)} style={{
                    textAlign: "left",
                    padding: "12px 14px",
                    borderRadius: "10px",
                    border: "1px solid var(--border)",
                    background: "var(--card)",
                    fontSize: "0.8rem",
                    lineHeight: 1.45,
                    cursor: "pointer",
                    color: "var(--foreground)",
                    fontFamily: "inherit",
                    boxShadow: "var(--shadow-xs)",
                    transition: "border-color 0.15s, box-shadow 0.15s, transform 0.1s",
                  }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.borderColor = "var(--primary)";
                      e.currentTarget.style.boxShadow = "var(--shadow-sm)";
                      e.currentTarget.style.transform = "translateY(-1px)";
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.borderColor = "var(--border)";
                      e.currentTarget.style.boxShadow = "var(--shadow-xs)";
                      e.currentTarget.style.transform = "translateY(0)";
                    }}
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Turns */}
          {turns.map((turn, idx) => (
            <div key={`${turn.turn_index}-${idx}`} className="animate-fade-in" style={{ marginBottom: "24px" }}>
              {/* User bubble */}
              <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "10px" }}>
                <div style={{
                  maxWidth: "68%",
                  padding: "11px 16px",
                  borderRadius: "20px 20px 4px 20px",
                  background: "var(--primary)",
                  color: "var(--primary-foreground)",
                  fontSize: "0.875rem",
                  lineHeight: 1.55,
                  fontWeight: 400,
                  boxShadow: "var(--shadow-sm)",
                }}>
                  {turn.user_message}
                </div>
              </div>

              {/* Agent bubble */}
              {turn.agent_response && (
                <div>
                  <div style={{
                    maxWidth: "86%",
                    ...S.card,
                    borderRadius: "4px 20px 20px 20px",
                    padding: "14px 18px",
                    boxShadow: "var(--shadow-sm)",
                  }}>
                    <Markdown text={turn.agent_response} />
                    {turn.latency_ms != null && (
                      <div style={{
                        marginTop: "12px",
                        paddingTop: "10px",
                        borderTop: "1px solid var(--border)",
                        fontSize: "0.72rem",
                        color: "var(--muted-foreground)",
                        display: "flex",
                        alignItems: "center",
                        gap: "6px",
                      }}>
                        <span>⚡</span>
                        <span>{(turn.latency_ms / 1000).toFixed(1)}s · logged to Lakebase</span>
                      </div>
                    )}
                  </div>
                  {turn.refs && turn.refs.length > 0 && (
                    <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", marginTop: "8px", paddingLeft: "2px" }}>
                      {turn.refs.map((ref) => (
                        <RefChip key={ref.file} ref_={ref} active={activePdf?.file === ref.file} onClick={() => onOpenDoc(ref)} />
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}

          {/* Loading */}
          {loading && (
            <div className="animate-fade-in" style={{ marginBottom: "24px" }}>
              <div style={{
                display: "inline-flex",
                alignItems: "center",
                gap: "10px",
                padding: "12px 18px",
                borderRadius: "4px 20px 20px 20px",
                ...S.card,
                fontSize: "0.875rem",
                color: "var(--muted-foreground)",
              }}>
                <span className="typing-dot" />
                <span className="typing-dot" />
                <span className="typing-dot" />
                <span style={{ marginLeft: "2px" }}>Consulting procurement supervisor…</span>
              </div>
            </div>
          )}

          {/* Error */}
          {error && (
            <div style={{
              padding: "12px 16px",
              borderRadius: "10px",
              border: "1px solid var(--destructive)",
              color: "var(--destructive)",
              fontSize: "0.875rem",
              background: "color-mix(in srgb, var(--destructive) 8%, transparent)",
              marginBottom: "16px",
            }}>
              {error}
            </div>
          )}

          <div ref={endRef} />
        </div>
      </div>

      {/* Input bar */}
      <div style={{ borderTop: "1px solid var(--border)", padding: "16px 20px", background: "var(--card)" }}>
        <div style={{
          maxWidth: "740px",
          margin: "0 auto",
          display: "flex",
          gap: "10px",
          alignItems: "center",
          background: "var(--background)",
          border: "1px solid var(--border)",
          borderRadius: "14px",
          padding: "6px 6px 6px 16px",
          boxShadow: "var(--shadow-xs)",
          transition: "border-color 0.15s, box-shadow 0.15s",
        }}
          onFocusCapture={(e) => {
            e.currentTarget.style.borderColor = "var(--primary)";
            e.currentTarget.style.boxShadow = "0 0 0 3px color-mix(in srgb, var(--primary) 12%, transparent)";
          }}
          onBlurCapture={(e) => {
            e.currentTarget.style.borderColor = "var(--border)";
            e.currentTarget.style.boxShadow = "var(--shadow-xs)";
          }}
        >
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void sendMessage(); } }}
            placeholder="Ask about invoices, contracts, or supplier spend…"
            disabled={loading}
            style={{
              flex: 1,
              background: "none",
              border: "none",
              outline: "none",
              fontSize: "0.875rem",
              color: "var(--foreground)",
              fontFamily: "inherit",
              padding: "6px 0",
            }}
          />
          <button
            type="button"
            onClick={() => void sendMessage()}
            disabled={loading || !input.trim()}
            style={{
              padding: "8px 18px",
              borderRadius: "10px",
              border: "none",
              background: loading || !input.trim() ? "var(--muted)" : "var(--primary)",
              color: loading || !input.trim() ? "var(--muted-foreground)" : "var(--primary-foreground)",
              fontSize: "0.875rem",
              fontWeight: 600,
              cursor: loading || !input.trim() ? "not-allowed" : "pointer",
              fontFamily: "inherit",
              transition: "background 0.15s, color 0.15s",
              flexShrink: 0,
              letterSpacing: "-0.01em",
            }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Documents tab
// ---------------------------------------------------------------------------
function SidebarSection({ title }: { title: string }) {
  return (
    <p style={{
      margin: "16px 12px 6px",
      fontSize: "0.67rem",
      fontWeight: 700,
      textTransform: "uppercase",
      letterSpacing: "0.08em",
      color: "var(--muted-foreground)",
    }}>{title}</p>
  );
}

function DocSidebarItem({ ref_, selected, onSelect }: { ref_: DocRef; selected: DocRef | null; onSelect: (r: DocRef) => void }) {
  const isActive = selected?.file === ref_.file;
  return (
    <button type="button" onClick={() => onSelect(ref_)} style={{
      display: "block",
      width: "100%",
      textAlign: "left",
      padding: "8px 12px 8px 14px",
      borderRadius: "8px",
      border: "none",
      borderLeft: isActive ? "3px solid var(--primary)" : "3px solid transparent",
      background: isActive ? "color-mix(in srgb, var(--primary) 8%, transparent)" : "transparent",
      color: isActive ? "var(--primary)" : "var(--foreground)",
      cursor: "pointer",
      transition: "all 0.12s",
      fontFamily: "inherit",
    }}
      onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = "var(--muted)"; }}
      onMouseLeave={(e) => { if (!isActive) e.currentTarget.style.background = "transparent"; }}
    >
      {/* Short ID — matches what the agent says */}
      <span style={{ fontFamily: "ui-monospace, monospace", fontWeight: isActive ? 700 : 600, fontSize: "0.83rem" }}>
        {ref_.id}
      </span>
      {/* Full document ID — matches what's printed on the PDF */}
      {ref_.fullId && ref_.fullId !== ref_.id && (
        <span style={{ display: "block", fontSize: "0.68rem", marginTop: "1px", fontWeight: 400,
          color: isActive ? "color-mix(in srgb, var(--primary) 60%, transparent)" : "var(--muted-foreground)",
          fontFamily: "ui-monospace, monospace" }}>
          {ref_.fullId}
        </span>
      )}
      {ref_.supplier && (
        <span style={{ display: "block", fontSize: "0.7rem", marginTop: "1px", fontWeight: 400,
          color: isActive ? "color-mix(in srgb, var(--primary) 70%, transparent)" : "var(--muted-foreground)" }}>
          {ref_.supplier}
        </span>
      )}
    </button>
  );
}

function DocumentsTab({ initialDoc, onDocOpened, manifest }: {
  initialDoc: DocRef | null;
  onDocOpened: (r: DocRef) => void;
  manifest: Manifest | null;
}) {
  const [selected, setSelected] = useState<DocRef | null>(initialDoc);
  const [query, setQuery] = useState("");
  useEffect(() => { if (initialDoc) setSelected(initialDoc); }, [initialDoc]);

  const select = (ref: DocRef) => { setSelected(ref); onDocOpened(ref); };

  const q = query.trim().toLowerCase();

  const invoiceRefs: DocRef[] = (manifest
    ? manifest.invoices.map((inv) => INVOICE_REFS[inv.short_id]).filter(Boolean)
    : []
  ).filter((ref) =>
    !q ||
    ref.id.toLowerCase().includes(q) ||
    (ref.fullId ?? "").toLowerCase().includes(q) ||
    (ref.supplier ?? "").toLowerCase().includes(q)
  );

  const contractRefs: DocRef[] = (manifest
    ? manifest.contracts.map((con) => CONTRACT_REFS[con.supplier_id]).filter(Boolean)
    : []
  ).filter((ref) =>
    !q ||
    ref.id.toLowerCase().includes(q) ||
    ref.label.toLowerCase().includes(q) ||
    (ref.supplier ?? "").toLowerCase().includes(q)
  );

  const sidebarContent = !manifest ? (
    <div style={{ padding: "24px 16px", color: "var(--muted-foreground)", fontSize: "0.8rem" }}>
      Loading…
    </div>
  ) : (
    <>
      <SidebarSection title="Invoices" />
      {invoiceRefs.length === 0 && q ? (
        <p style={{ margin: "4px 14px 8px", fontSize: "0.75rem", color: "var(--muted-foreground)" }}>No matches</p>
      ) : invoiceRefs.map((ref) => (
        <DocSidebarItem key={ref.id} ref_={ref} selected={selected} onSelect={select} />
      ))}
      <SidebarSection title="Contracts" />
      {contractRefs.length === 0 && q ? (
        <p style={{ margin: "4px 14px 8px", fontSize: "0.75rem", color: "var(--muted-foreground)" }}>No matches</p>
      ) : contractRefs.map((ref) => (
        <DocSidebarItem key={ref.id} ref_={ref} selected={selected} onSelect={select} />
      ))}
    </>
  );

  return (
    <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
      {/* Sidebar */}
      <div style={{ width: "210px", flexShrink: 0, borderRight: "1px solid var(--border)", display: "flex", flexDirection: "column", background: "var(--card)" }}>
        {/* Search */}
        <div style={{ padding: "10px 10px 6px", borderBottom: "1px solid var(--border)", flexShrink: 0 }}>
          <div style={{
            display: "flex",
            alignItems: "center",
            gap: "6px",
            background: "var(--muted)",
            border: "1px solid var(--border)",
            borderRadius: "8px",
            padding: "5px 10px",
          }}>
            <span style={{ fontSize: "12px", color: "var(--muted-foreground)", flexShrink: 0 }}>🔍</span>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search invoices…"
              style={{
                background: "none",
                border: "none",
                outline: "none",
                fontSize: "0.8rem",
                color: "var(--foreground)",
                fontFamily: "inherit",
                width: "100%",
                minWidth: 0,
              }}
            />
            {query && (
              <button type="button" onClick={() => setQuery("")} style={{
                background: "none", border: "none", cursor: "pointer",
                color: "var(--muted-foreground)", fontSize: "12px", padding: 0, flexShrink: 0,
              }}>✕</button>
            )}
          </div>
        </div>
        {/* List */}
        <div style={{ overflowY: "auto", flex: 1, paddingBottom: "16px" }}>
          {sidebarContent}
        </div>
      </div>

      {/* PDF viewer */}
      <div style={{ display: "flex", flexDirection: "column", flex: 1, overflow: "hidden", background: "var(--background)" }}>
        {selected ? (
          <>
            <div style={{
              padding: "12px 20px",
              borderBottom: "1px solid var(--border)",
              background: "var(--card)",
              display: "flex",
              alignItems: "center",
              gap: "10px",
              boxShadow: "var(--shadow-xs)",
            }}>
              <span style={{ fontFamily: "ui-monospace, monospace", fontWeight: 700, fontSize: "0.9rem", letterSpacing: "-0.01em" }}>
                {selected.label}
              </span>
              <span style={{
                fontSize: "0.72rem",
                fontWeight: 600,
                padding: "2px 9px",
                borderRadius: "999px",
                background: "var(--muted)",
                color: "var(--muted-foreground)",
                textTransform: "uppercase",
                letterSpacing: "0.04em",
              }}>
                {selected.type === "invoices" ? "Invoice" : "Contract"}
              </span>
              {selected.supplier && (
                <span style={{ fontSize: "0.8rem", color: "var(--muted-foreground)" }}>· {selected.supplier}</span>
              )}
            </div>
            <iframe key={selected.file} src={pdfUrl(selected)} title={selected.label}
              style={{ flex: 1, border: "none", width: "100%", height: "100%" }} />
          </>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", flex: 1, textAlign: "center", padding: "40px", color: "var(--muted-foreground)" }}>
            <div style={{ fontSize: "3rem", opacity: 0.15, marginBottom: "14px" }}>📄</div>
            <p style={{ fontSize: "0.875rem", maxWidth: "300px", lineHeight: 1.6 }}>
              Select an invoice or contract from the sidebar, or click a reference chip in the Chat tab.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// History tab
// ---------------------------------------------------------------------------
async function fetchTurns(sessionId: string): Promise<Turn[]> {
  const res = await fetch(`/api/invoice/conversations?session_id=${encodeURIComponent(sessionId)}`);
  const data = (await res.json()) as { turns: Turn[] };
  return (data.turns ?? []).map((t) => ({ ...t, refs: t.agent_response ? extractRefs(t.agent_response) : [] }));
}

function HistoryTab({ currentSessionId, onRestore }: {
  currentSessionId: string;
  onRestore: (sessionId: string) => void;
}) {
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [turnCache, setTurnCache] = useState<Record<string, Turn[]>>({});
  const [loadingId, setLoadingId] = useState<string | null>(null);

  useEffect(() => {
    void (async () => {
      try {
        const res = await fetch("/api/invoice/sessions");
        if (!res.ok) return;
        const data = (await res.json()) as { sessions: SessionSummary[] };
        setSessions(data.sessions ?? []);
      } catch { /* ignore */ } finally { setLoading(false); }
    })();
  }, []);

  const getOrFetch = async (id: string) => {
    if (turnCache[id]) return turnCache[id];
    setLoadingId(id);
    try {
      const turns = await fetchTurns(id);
      setTurnCache((c) => ({ ...c, [id]: turns }));
      return turns;
    } finally { setLoadingId(null); }
  };

  const toggle = async (id: string) => {
    if (expandedId === id) { setExpandedId(null); return; }
    setExpandedId(id);
    await getOrFetch(id);
  };

  const restore = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    onRestore(id);
  };

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1, color: "var(--muted-foreground)", fontSize: "0.875rem" }}>
      Loading sessions…
    </div>
  );

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: "28px 20px" }}>
      <div style={{ maxWidth: "700px", margin: "0 auto" }}>
        {sessions.length === 0 ? (
          <div style={{ textAlign: "center", paddingTop: "60px", color: "var(--muted-foreground)" }}>
            <div style={{ fontSize: "2.5rem", opacity: 0.15, marginBottom: "14px" }}>🕐</div>
            <p style={{ fontSize: "0.875rem" }}>No sessions yet. Start a conversation!</p>
          </div>
        ) : (
          <>
            <p style={{ margin: "0 0 16px", fontSize: "0.75rem", color: "var(--muted-foreground)", fontWeight: 500 }}>
              {sessions.length} session{sessions.length !== 1 ? "s" : ""}
            </p>
            {sessions.map((s) => {
              const isCurrent = s.session_id === currentSessionId;
              const isExpanded = expandedId === s.session_id;
              const isLoadingThis = loadingId === s.session_id;
              const cachedTurns = turnCache[s.session_id];
              const date = new Date(s.last_activity || s.started_at).toLocaleString(undefined, {
                month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
              });
              return (
                <div key={s.session_id} style={{
                  marginBottom: "8px",
                  ...S.card,
                  border: `1px solid ${isCurrent ? "var(--primary)" : "var(--border)"}`,
                  overflow: "hidden",
                  transition: "box-shadow 0.15s",
                }}>
                  {/* Row */}
                  <div
                    role="button" tabIndex={0}
                    onClick={() => void toggle(s.session_id)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") void toggle(s.session_id); }}
                    style={{ display: "flex", alignItems: "center", padding: "14px 16px", cursor: "pointer", gap: "10px", userSelect: "none" }}
                  >
                    <span style={{
                      fontSize: "0.65rem", color: "var(--muted-foreground)",
                      transform: isExpanded ? "rotate(90deg)" : "rotate(0deg)",
                      transition: "transform 0.15s", flexShrink: 0,
                    }}>▶</span>

                    <div style={{ minWidth: 0, flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "7px", marginBottom: "3px", flexWrap: "wrap" }}>
                        <span style={{ fontFamily: "ui-monospace, monospace", fontSize: "0.75rem", color: "var(--muted-foreground)", fontWeight: 500 }}>
                          {s.session_id.slice(0, 8)}…
                        </span>
                        {isCurrent && (
                          <span style={{
                            fontSize: "0.68rem", padding: "1px 8px", borderRadius: "999px",
                            background: "color-mix(in srgb, var(--primary) 12%, transparent)",
                            color: "var(--primary)", fontWeight: 700, letterSpacing: "0.02em",
                          }}>current</span>
                        )}
                        <span style={{ fontSize: "0.72rem", color: "var(--muted-foreground)" }}>
                          {s.turn_count} turn{s.turn_count !== 1 ? "s" : ""} · {date}
                        </span>
                      </div>
                      <p style={{ margin: 0, fontSize: "0.875rem", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", fontWeight: 500 }}>
                        {s.first_question ?? "—"}
                      </p>
                    </div>

                    {!isCurrent && (
                      <button type="button" onClick={(e) => restore(e, s.session_id)}
                        style={{
                          flexShrink: 0,
                          padding: "6px 14px",
                          fontSize: "0.8rem",
                          fontWeight: 600,
                          borderRadius: "8px",
                          border: "1px solid var(--border)",
                          background: "var(--muted)",
                          color: "var(--foreground)",
                          cursor: "pointer",
                          fontFamily: "inherit",
                          transition: "all 0.12s",
                          whiteSpace: "nowrap",
                          letterSpacing: "-0.01em",
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.background = "color-mix(in srgb, var(--primary) 10%, transparent)";
                          e.currentTarget.style.color = "var(--primary)";
                          e.currentTarget.style.borderColor = "var(--primary)";
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.background = "var(--muted)";
                          e.currentTarget.style.color = "var(--foreground)";
                          e.currentTarget.style.borderColor = "var(--border)";
                        }}
                      >
                        ↩ Restore
                      </button>
                    )}
                  </div>

                  {/* Expanded turns */}
                  {isExpanded && (
                    <div style={{ borderTop: "1px solid var(--border)", background: "var(--muted)", padding: "16px", maxHeight: "440px", overflowY: "auto" }}>
                      {isLoadingThis ? (
                        <p style={{ margin: 0, fontSize: "0.8rem", color: "var(--muted-foreground)" }}>Loading…</p>
                      ) : (cachedTurns ?? []).map((t, i) => (
                        <div key={i} style={{ marginBottom: "18px" }}>
                          <p style={{ margin: "0 0 6px", fontSize: "0.7rem", fontWeight: 700, color: "var(--muted-foreground)", textTransform: "uppercase", letterSpacing: "0.06em" }}>
                            Turn {t.turn_index + 1}
                          </p>
                          <div style={{ padding: "9px 13px", borderRadius: "8px", background: "var(--card)", border: "1px solid var(--border)", fontSize: "0.875rem", marginBottom: "6px", fontWeight: 500 }}>
                            {t.user_message}
                          </div>
                          {t.agent_response && (
                            <div style={{ padding: "10px 14px", borderRadius: "8px", background: "var(--background)", border: "1px solid var(--border)", boxShadow: "var(--shadow-xs)" }}>
                              <Markdown text={t.agent_response} />
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Root App
// ---------------------------------------------------------------------------
function App() {
  const [theme, toggleTheme] = useTheme();
  const [activeTab, setActiveTab] = useState<Tab>("chat");
  const [sessionId, setSessionId] = useState(getOrCreateSessionId);
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activePdf, setActivePdf] = useState<DocRef | null>(null);
  const [docToOpen, setDocToOpen] = useState<DocRef | null>(null);

  // Load manifest once on mount — builds the invoice/contract ref maps
  useEffect(() => {
    void (async () => {
      try {
        const res = await fetch("/api/invoice/manifest");
        if (!res.ok) return;
        const data = (await res.json()) as Manifest;
        buildRefs(data);
        setManifest(data);
        // Re-extract refs for any turns already loaded before manifest arrived
        setTurns((prev) =>
          prev.map((t) => ({
            ...t,
            refs: t.agent_response ? extractRefs(t.agent_response) : [],
          })),
        );
      } catch { /* use empty maps if manifest unavailable */ }
    })();
  }, []);

  // Fetch turns whenever the active session changes (including on restore)
  useEffect(() => {
    void (async () => {
      try {
        const res = await fetch(`/api/invoice/conversations?session_id=${encodeURIComponent(sessionId)}`);
        if (!res.ok) { setTurns([]); return; }
        const data = (await res.json()) as { turns: Turn[] };
        setTurns((data.turns ?? []).map((t) => ({ ...t, refs: t.agent_response ? extractRefs(t.agent_response) : [] })));
      } catch { setTurns([]); }
    })();
  }, [sessionId]); // re-runs on restore — replaces turns cleanly

  const sendMessage = async (msg?: string) => {
    const message = (msg ?? input).trim();
    if (!message || loading) return;
    setInput("");
    setError(null);
    setTurns((p) => [...p, { turn_index: p.length, user_message: message, agent_response: "" }]);
    setLoading(true);
    try {
      const res = await fetch("/api/invoice/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId, message, user_agent: navigator.userAgent }),
      });
      if (!res.ok) {
        const b = (await res.json()) as { error?: string; message?: string };
        throw new Error(b.message ?? b.error ?? `HTTP ${res.status}`);
      }
      const data = (await res.json()) as { response: string; turn_index: number; latency_ms: number };
      const refs = extractRefs(data.response);
      setTurns((p) => {
        const u = [...p];
        u[u.length - 1] = { turn_index: data.turn_index, user_message: message, agent_response: data.response, latency_ms: data.latency_ms, refs };
        return u;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setTurns((p) => p.slice(0, -1));
    } finally {
      setLoading(false);
    }
  };

  const handleOpenDoc = (ref: DocRef) => {
    setActivePdf(ref);
    setDocToOpen(ref);
    setActiveTab("documents");
  };

  const handleRestore = (id: string) => {
    persistSession(id);
    setSessionId(id);  // triggers useEffect → fetches & replaces turns cleanly
    setError(null);
    setInput("");
    setActiveTab("chat");
  };

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", background: "var(--background)", overflow: "hidden" }}>
      {/* ── Header ── */}
      <div style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "0 20px",
        height: "56px",
        background: "var(--card)",
        borderBottom: "1px solid var(--border)",
        boxShadow: "var(--shadow-xs)",
        flexShrink: 0,
        zIndex: 10,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          {/* Logo mark */}
          <div style={{
            width: "30px",
            height: "30px",
            borderRadius: "8px",
            background: "var(--primary)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: "15px",
            flexShrink: 0,
          }}>🧾</div>
          <div>
            <div style={{ fontSize: "0.9rem", fontWeight: 700, letterSpacing: "-0.02em", lineHeight: 1.2 }}>
              Caspers Procurement
            </div>
            <div style={{ fontSize: "0.68rem", color: "var(--muted-foreground)", letterSpacing: "0.01em" }}>
              Session {sessionId.slice(0, 8)}…
            </div>
          </div>
        </div>

        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          <button type="button" onClick={toggleTheme} title={theme === "dark" ? "Light mode" : "Dark mode"} style={S.iconBtn}>
            {theme === "dark" ? "☀️" : "🌙"}
          </button>
          <button type="button" onClick={() => {
            const id = crypto.randomUUID();
            persistSession(id);
            setSessionId(id);
            setError(null); setInput("");
          }} style={{
            padding: "7px 14px",
            borderRadius: "8px",
            border: "1px solid var(--border)",
            background: "var(--muted)",
            color: "var(--foreground)",
            fontSize: "0.8rem",
            fontWeight: 600,
            cursor: "pointer",
            fontFamily: "inherit",
            letterSpacing: "-0.01em",
            transition: "background 0.15s",
          }}
            onMouseEnter={(e) => { e.currentTarget.style.background = "var(--secondary)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = "var(--muted)"; }}
          >
            + New session
          </button>
        </div>
      </div>

      {/* ── Tabs ── */}
      <TabBar active={activeTab} onSelect={setActiveTab} />

      {/* ── Content ── */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {activeTab === "chat" && (
          <ChatTab turns={turns} loading={loading} error={error} input={input} setInput={setInput}
            sendMessage={sendMessage} activePdf={activePdf} onOpenDoc={handleOpenDoc} />
        )}
        {activeTab === "documents" && (
          <DocumentsTab initialDoc={docToOpen} onDocOpened={setActivePdf} manifest={manifest} />
        )}
        {activeTab === "history" && (
          <HistoryTab currentSessionId={sessionId} onRestore={handleRestore} />
        )}
      </div>
    </div>
  );
}

export default App;
