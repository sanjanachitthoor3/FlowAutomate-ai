import { useState } from "react";

function InputBox({ onSend }) {
  const [input, setInput] = useState("");

  const handleSend = () => {
    if (!input.trim()) return;
    onSend(input);
    setInput("");
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const canSend = input.trim().length > 0;

  return (
    <div className="flex-none px-6 py-4 border-t border-white/5 bg-[#0f1117]">
      <div className="max-w-3xl mx-auto">

        {/* Input row */}
        <div className="flex items-center gap-3 bg-[#1a1d2e] border border-white/10 rounded-2xl px-4 py-3 focus-within:border-indigo-500/50 focus-within:shadow-lg transition-all" style={{ boxShadow: "0 0 0 0px #6366f100" }}>

          {/* Workflow icon inside input */}
          <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-white/20 flex-none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="18" cy="5" r="3" /><circle cx="6" cy="12" r="3" /><circle cx="18" cy="19" r="3" />
            <path d="M8.59 13.51l6.83 3.98M15.41 6.51l-6.82 3.98" />
          </svg>

          <input
            className="flex-1 bg-transparent text-sm text-white placeholder-white/25 outline-none"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Describe your workflow instruction..."
          />

          {/* Character hint */}
          {input.length > 0 && (
            <span className="text-[10px] text-white/20 flex-none">{input.length}</span>
          )}

          {/* Send button */}
          <button
            onClick={handleSend}
            disabled={!canSend}
            className="flex-none w-8 h-8 rounded-xl flex items-center justify-center transition-all"
            style={canSend
              ? { background: "linear-gradient(135deg, #6366f1, #4f46e5)", boxShadow: "0 4px 12px rgba(99,102,241,0.4)" }
              : { background: "rgba(255,255,255,0.05)" }
            }
          >
            <svg viewBox="0 0 24 24" fill="none" className={`w-3.5 h-3.5 ${canSend ? "text-white" : "text-white/20"}`} stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M22 2L11 13" /><path d="M22 2L15 22l-4-9-9-4 20-7z" />
            </svg>
          </button>
        </div>

        {/* Footer hint */}
        <p className="text-center text-[11px] text-white/15 mt-2.5">
          Press <kbd className="font-mono bg-white/5 border border-white/10 px-1.5 py-0.5 rounded text-[10px]">Enter</kbd> to send · <kbd className="font-mono bg-white/5 border border-white/10 px-1.5 py-0.5 rounded text-[10px]">Shift+Enter</kbd> for new line
        </p>
      </div>
    </div>
  );
}

export default InputBox;
