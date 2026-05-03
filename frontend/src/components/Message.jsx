function WorkflowContent({ data }) {
  const plan         = data?.plan ?? [];
  const explanations = data?.explanations ?? [];
  const status       = data?.result?.status;
  const isSuccess    = status === "success";

  return (
    <div className="flex flex-col gap-3 text-sm">

      {plan.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-white/30 mb-1.5">Plan</p>
          <ol className="flex flex-col gap-1">
            {plan.map((step, i) => (
              <li key={i} className="flex items-center gap-2 text-white/80">
                <span
                  className="flex-none w-5 h-5 rounded-md flex items-center justify-center text-[10px] font-bold text-indigo-300"
                  style={{ background: "rgba(99,102,241,0.15)" }}
                >
                  {i + 1}
                </span>
                <span className="font-mono text-xs text-indigo-300">{step.tool}</span>
              </li>
            ))}
          </ol>
        </div>
      )}

      {explanations.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-white/30 mb-1.5">Explanation</p>
          <ul className="flex flex-col gap-1">
            {explanations.map((line, i) => (
              <li key={i} className="flex items-start gap-2 text-white/65 text-xs leading-relaxed">
                <span className="mt-1.5 flex-none w-1 h-1 rounded-full bg-white/25" />
                {line}
              </li>
            ))}
          </ul>
        </div>
      )}

      {status && (
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-white/30 mb-1.5">
            Result
          </p>

          <span
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold ${
              isSuccess
                ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/25"
                : "bg-red-500/15 text-red-400 border border-red-500/25"
            }`}
          >
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                isSuccess ? "bg-emerald-400" : "bg-red-400"
              }`}
            />
            {status}
          </span>

          {/* ✅ OUTPUT FILES */}
          {data.result?.results?.map((r, i) => (
            <div key={i} className="text-xs text-white/50 mt-2">
              Output:{" "}
              <a
                href={`${import.meta.env.VITE_API_URL}/download/${r.file.split("/").pop()}`}
                target="_blank"
                className="text-indigo-400 hover:underline"
              >
                {r.file.split("/").pop()}
              </a>
            </div>
          ))}
        </div>
      )}

    </div>
  );
}

function WorkflowPreviewContent({ data, onApprove, onCancel, errorMode, setErrorMode }) {
  const plan         = data?.plan         ?? [];
  const explanations = data?.explanations ?? [];

  return (
    <div className="flex flex-col gap-3 text-sm">

      {/* Preview label */}
      <div className="flex items-center gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-amber-400/80">Preview</span>
        <span className="flex-1 h-px bg-amber-400/15" />
      </div>

      {/* Plan */}
      {plan.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-white/30 mb-1.5">Plan</p>
          <ol className="flex flex-col gap-1">
            {plan.map((step, i) => (
              <li key={i} className="flex items-center gap-2 text-white/80">
                <span className="flex-none w-5 h-5 rounded-md flex items-center justify-center text-[10px] font-bold text-indigo-300"
                  style={{ background: "rgba(99,102,241,0.15)" }}>
                  {i + 1}
                </span>
                <span className="font-mono text-xs text-indigo-300">{step.tool}</span>
              </li>
            ))}
          </ol>
        </div>
      )}

      {/* Explanation */}
      {explanations.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest text-white/30 mb-1.5">Explanation</p>
          <ul className="flex flex-col gap-1">
            {explanations.map((line, i) => (
              <li key={i} className="flex items-start gap-2 text-white/65 text-xs leading-relaxed">
                <span className="mt-1.5 flex-none w-1 h-1 rounded-full bg-white/25" />
                {line}
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="flex items-center gap-2">
        <span className="text-[10px] text-white/40">If error occurs:</span>
        <select
            value={errorMode}
            onChange={(e) => setErrorMode(e.target.value)}
            className="bg-[#1a1d2e] border border-white/10 text-xs rounded-lg px-2 py-1 text-white/70"
        >
            <option value="fail">Fail</option>
            <option value="retry">Retry</option>
            <option value="skip">Skip</option>
         </select>
        </div>

      {/* Approve / Cancel buttons */}
      <div className="flex items-center gap-2 pt-1">
        <button
          onClick={onApprove}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold text-white transition-all hover:opacity-90 active:scale-95"
          style={{ background: "linear-gradient(135deg, #6366f1, #4f46e5)" }}
        >
          <svg viewBox="0 0 24 24" fill="none" className="w-3 h-3" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M20 6L9 17l-5-5" />
          </svg>
          Approve
        </button>

        <button
          onClick={onCancel}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold text-white/50 border border-white/10 hover:text-white/80 hover:border-white/20 transition-all active:scale-95"
        >
          <svg viewBox="0 0 24 24" fill="none" className="w-3 h-3" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M18 6L6 18M6 6l12 12" />
          </svg>
          Cancel
        </button>
      </div>
    </div>
  );
}


function Message({ role, text, type, data, onApprove, onCancel, errorMode, setErrorMode }) {
  const isUser        = role === "user";
  const isProcessing  = text === "Processing...";
  const isWorkflow    = type === "workflow";
  const isPreview     = type === "workflow_preview";

  return (
    <div
      className={`flex items-end gap-3 mb-5 ${isUser ? "justify-end" : "justify-start"}`}
      style={{ animation: "fadeSlideIn 0.2s ease forwards" }}
    >
      {/* ── Bot avatar ── */}
      {!isUser && (
        <div className="flex-none w-8 h-8 rounded-xl flex items-center justify-center shadow-lg mb-0.5" style={{ background: "linear-gradient(135deg, #6366f1, #4f46e5)" }}>
          <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-white" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
          </svg>
        </div>
      )}

      {/* ── Bubble ── */}
      <div className="flex flex-col gap-1 max-w-sm">
        <span className={`text-[10px] font-medium tracking-wide ${isUser ? "text-right text-white/25" : "text-white/25"}`}>
          {isUser ? "You" : "FlowAutomate AI"}
        </span>

        <div
          className={`px-4 py-3 rounded-2xl text-sm leading-relaxed shadow-lg ${
            isUser ? "text-white rounded-br-sm" : "text-white/85 rounded-bl-sm border border-white/8"
          }`}
          style={isUser
            ? { background: "linear-gradient(135deg, #6366f1, #4f46e5)" }
            : { background: "#1e2130", borderColor: isPreview ? "rgba(251,191,36,0.15)" : "rgba(255,255,255,0.06)" }
          }
        >
          {isProcessing ? (
            <span className="flex items-center gap-1 py-0.5">
              {[0, 1, 2].map((i) => (
                <span
                  key={i}
                  className="w-1.5 h-1.5 rounded-full bg-white/40"
                  style={{ animation: `bounce 1.2s ease-in-out ${i * 0.2}s infinite` }}
                />
              ))}
            </span>
          ) : isPreview ? (
            <WorkflowPreviewContent data={data} onApprove={onApprove} onCancel={onCancel} errorMode={errorMode} setErrorMode={setErrorMode}/>
          ) : isWorkflow ? (
            <WorkflowContent data={data} />
          ) : (
            text
          )}
        </div>
      </div>

      {/* ── User avatar ── */}
      {isUser && (
        <div className="flex-none w-8 h-8 rounded-xl bg-white/10 border border-white/10 flex items-center justify-center mb-0.5">
          <svg viewBox="0 0 24 24" fill="none" className="w-4 h-4 text-white/50" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="8" r="4" /><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7" />
          </svg>
        </div>
      )}

      <style>{`
        @keyframes fadeSlideIn {
          from { opacity: 0; transform: translateY(8px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes bounce {
          0%, 80%, 100% { transform: translateY(0); opacity: 0.4; }
          40%            { transform: translateY(-4px); opacity: 1; }
        }
      `}</style>
    </div>
  );
}

export default Message;
