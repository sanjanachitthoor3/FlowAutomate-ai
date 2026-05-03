import { useEffect, useRef } from "react";
import Message from "./Message";

function ChatWindow({ messages, onApprove, onCancel, errorMode, setErrorMode }) {
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="max-w-3xl mx-auto px-6 py-8">

        {messages.length === 0 && (
          <div className="flex flex-col items-center justify-center h-[55vh] gap-4 text-center">
            <div className="relative mb-2">
              <div className="absolute inset-0 rounded-full blur-2xl opacity-30" style={{ background: "radial-gradient(circle, #6366f1, transparent 70%)", transform: "scale(2)" }} />
              <div className="relative w-16 h-16 rounded-2xl flex items-center justify-center shadow-2xl" style={{ background: "linear-gradient(135deg, #6366f1, #4f46e5)" }}>
                <svg viewBox="0 0 24 24" fill="none" className="w-8 h-8 text-white" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                </svg>
              </div>
            </div>
            <h2 className="text-2xl font-semibold text-white tracking-tight">Welcome to FlowAutomate AI</h2>
            <p className="text-sm text-white/40 max-w-sm leading-relaxed">
              Automate workflows using natural language. Describe what you want to do — FlowAutomate handles the rest.
            </p>
            <div className="flex flex-wrap justify-center gap-2 mt-3">
              {["Clean data.csv and summarize", "Rename all files in /reports", "Generate a weekly summary"].map((s) => (
                <span key={s} className="text-xs px-3 py-1.5 rounded-full border border-white/10 text-white/40 bg-white/5">
                  {s}
                </span>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, index) => (
          <Message
            key={index}
            role={msg.role}
            text={msg.text}
            type={msg.type}
            data={msg.data}
            onApprove={onApprove}
            onCancel={onCancel}
            errorMode={errorMode}
            setErrorMode={setErrorMode}
          />
        ))}

        <div ref={bottomRef} />
      </div>
    </div>
  );
}

export default ChatWindow;
