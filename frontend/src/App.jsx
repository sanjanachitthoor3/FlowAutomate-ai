import { useState } from "react";
import ChatWindow from "./components/ChatWindow";
import InputBox from "./components/InputBox";

function App() {
  const [messages, setMessages]           = useState([]);
  const [lastInstruction, setLastInstruction] = useState("");
  const [previewPlan, setPreviewPlan]     = useState(null);
  const [errorMode, setErrorMode] = useState("fail");

  // ── STEP 1: Preview request ──────────────────────────────────────────────
  const handleSend = async (text) => {
    setLastInstruction(text);

    setMessages((prev) => [
      ...prev,
      { role: "user", text },
      { role: "bot", text: "Processing..." },
    ]);

    try {
      const response = await fetch("/run-workflow", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ instruction: text, files: [], preview: true }),
      });

      if (!response.ok) throw new Error(`Server error: ${response.status}`);

      const data = await response.json();

      // Store the plan so Approve can send it back
      setPreviewPlan(data.validated_plan);

      // Replace "Processing..." with preview message
      setMessages((prev) => [
        ...prev.slice(0, -1),
        {
          role: "bot",
          type: "workflow_preview",
          data: {
            plan:         data.validated_plan,
            explanations: data.explanations,
          },
        },
      ]);
    } catch {
      setMessages((prev) => [
        ...prev.slice(0, -1),
        { role: "bot", text: "Error connecting to backend." },
      ]);
    }
  };

  // ── STEP 2: Approve → execute ────────────────────────────────────────────
  const handleApprove = async () => {
    // Swap preview message with typing indicator
    setMessages((prev) => [
      ...prev.slice(0, -1),
      { role: "bot", text: "Processing..." },
    ]);

    try {
      const response = await fetch("/run-workflow", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          instruction:    lastInstruction,
          files:          [],
          preview:        false,
          validated_plan: previewPlan,
          error_mode:     errorMode,
        }),
      });

      if (!response.ok) throw new Error(`Server error: ${response.status}`);

      const data = await response.json();

      setMessages((prev) => [
        ...prev.slice(0, -1),
        {
          role: "bot",
          type: "workflow",
          data: {
            plan:         data.validated_plan,
            explanations: data.explanations,
            result:       data.result,
          },
        },
      ]);
    } catch {
      setMessages((prev) => [
        ...prev.slice(0, -1),
        { role: "bot", text: "Error connecting to backend." },
      ]);
    } finally {
      setPreviewPlan(null);
    }
  };

  // ── Cancel: remove preview ───────────────────────────────────────────────
  const handleCancel = () => {
    setPreviewPlan(null);
    setMessages((prev) => [
      ...prev.slice(0, -1),
      { role: "bot", text: "Workflow cancelled." },
    ]);
  };

  return (
    <div className="h-screen flex bg-[#0f1117] text-white overflow-hidden" style={{ fontFamily: "'DM Sans', 'Segoe UI', sans-serif" }}>

      {/* ── Sidebar ── */}
      <aside className="w-64 flex-none flex flex-col bg-[#16181f] border-r border-white/5">

        <div className="px-5 py-5 flex items-center gap-3 border-b border-white/5">
          <div className="w-8 h-8 rounded-xl flex items-center justify-center" style={{ background: "linear-gradient(135deg, #6366f1, #4f46e5)" }}>
            <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-white" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
            </svg>
          </div>
          <span className="text-sm font-semibold text-white tracking-tight">FlowAutomate AI</span>
        </div>

        <div className="px-3 pt-4">
          <button className="w-full flex items-center gap-2.5 px-3 py-2.5 rounded-xl text-sm text-white/70 hover:text-white hover:bg-white/5 border border-white/10 hover:border-white/20 transition-all">
            <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 5v14M5 12h14" />
            </svg>
            New workflow
          </button>
        </div>

        <div className="px-5 pt-6 pb-2">
          <span className="text-[10px] font-semibold uppercase tracking-widest text-white/25">Recent</span>
        </div>

        <div className="px-3 flex flex-col gap-0.5">
          {["Clean sales data", "Rename batch files", "Generate Q3 report"].map((item) => (
            <button key={item} className="text-left px-3 py-2 rounded-lg text-xs text-white/40 hover:text-white/70 hover:bg-white/5 truncate transition-all">
              {item}
            </button>
          ))}
        </div>

        <div className="mt-auto px-4 py-4 border-t border-white/5 flex items-center gap-2.5">
          <div className="w-6 h-6 rounded-full bg-indigo-500/20 flex items-center justify-center">
            <svg viewBox="0 0 24 24" fill="none" className="w-3 h-3 text-indigo-400" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="8" r="4" /><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7" />
            </svg>
          </div>
          <div>
            <p className="text-xs text-white/60 leading-none">User</p>
            <p className="text-[10px] text-white/25 mt-0.5">Free plan</p>
          </div>
          <span className="ml-auto text-[10px] bg-indigo-500/15 text-indigo-400 px-2 py-0.5 rounded-full font-medium border border-indigo-500/20">Beta</span>
        </div>
      </aside>

      {/* ── Main panel ── */}
      <div className="flex-1 flex flex-col min-w-0">
        <header className="flex-none flex items-center justify-between px-6 py-4 border-b border-white/5">
          <div>
            <h1 className="text-sm font-semibold text-white">Workflow Chat</h1>
            <p className="text-xs text-white/30 mt-0.5">Describe your automation in plain language</p>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
            <span className="text-xs text-white/30">Ready</span>
          </div>
        </header>

        {/* Pass approve/cancel handlers down through ChatWindow via message props */}
        <ChatWindow messages={messages} onApprove={handleApprove} onCancel={handleCancel} errorMode={errorMode}setErrorMode={setErrorMode} />
        <InputBox onSend={handleSend} />
      </div>
    </div>
  );
}

export default App;
