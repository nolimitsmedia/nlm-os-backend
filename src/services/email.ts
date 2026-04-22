type SendMailArgs = {
  to: string | string[];
  subject: string;
  text: string;
  html?: string;
};

function env(name: string, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function boolEnv(name: string, fallback = false) {
  const v = env(name);
  if (!v) return fallback;
  return ["1", "true", "yes", "on"].includes(v.toLowerCase());
}

function getMailgunConfig() {
  const apiKey = env("MAILGUN_API_KEY");
  const domain = env("MAILGUN_DOMAIN");
  const fromEmail = env("MAILGUN_FROM_EMAIL");
  const fromName = env("MAILGUN_FROM_NAME", "No Limits Media");
  const enabled =
    boolEnv("MAILGUN_ENABLED", true) && !!apiKey && !!domain && !!fromEmail;

  return { apiKey, domain, fromEmail, fromName, enabled };
}

function normalizeRecipients(value: string | string[]) {
  return Array.isArray(value)
    ? value.map((item) => String(item || "").trim()).filter(Boolean)
    : [String(value || "").trim()].filter(Boolean);
}

function escapeHtml(value: string) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

export async function sendEmail(args: SendMailArgs) {
  const cfg = getMailgunConfig();
  const to = normalizeRecipients(args.to);

  if (!to.length) {
    throw new Error("Email recipient is required");
  }

  if (!cfg.enabled) {
    return { ok: false, skipped: true, reason: "mailgun_not_configured" };
  }

  const endpoint = `https://api.mailgun.net/v3/${cfg.domain}/messages`;
  const auth = Buffer.from(`api:${cfg.apiKey}`).toString("base64");

  const form = new URLSearchParams();
  form.set("from", `${cfg.fromName} <${cfg.fromEmail}>`);
  form.set("to", to.join(", "));
  form.set("subject", args.subject);
  form.set("text", args.text || "");
  form.set(
    "html",
    args.html ||
      `<div style="font-family:Arial,sans-serif;line-height:1.5;color:#0f172a;white-space:pre-wrap;">${escapeHtml(args.text || "")}</div>`,
  );

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });

  const raw = await res.text();
  let data: any = null;

  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    data = raw;
  }

  if (!res.ok) {
    throw new Error(
      typeof data === "object" && data?.message
        ? data.message
        : `Mailgun request failed (${res.status})`,
    );
  }

  return {
    ok: true,
    id: data?.id || null,
    message: data?.message || "Queued. Thank you.",
  };
}

export async function sendInviteEmail(args: {
  to: string;
  invitedName?: string;
  invitedByName?: string;
  role?: string;
  setupUrl: string;
}) {
  const invitedName = String(args.invitedName || "").trim();
  const invitedByName = String(args.invitedByName || "No Limits Media").trim();
  const role = String(args.role || "staff").trim();

  const subject = "You’ve been invited to NLM OS";
  const text = [
    invitedName ? `Hi ${invitedName},` : "Hi,",
    "",
    `${invitedByName} invited you to join NLM OS as ${role}.`,
    "",
    "Use the link below to set up your account:",
    args.setupUrl,
    "",
    "If you did not expect this invite, you can safely ignore this email.",
  ].join("\n");

  const html = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.6;">
      <h2 style="margin:0 0 12px;color:#002c55;">You’ve been invited to NLM OS</h2>
      <p>${invitedName ? `Hi ${escapeHtml(invitedName)},` : "Hi,"}</p>
      <p><strong>${escapeHtml(invitedByName)}</strong> invited you to join NLM OS as <strong>${escapeHtml(role)}</strong>.</p>
      <p>
        <a href="${args.setupUrl}" style="display:inline-block;padding:12px 18px;border-radius:10px;background:#002c55;color:#ffffff;text-decoration:none;font-weight:700;">
          Set up your account
        </a>
      </p>
      <p style="word-break:break-all;">${escapeHtml(args.setupUrl)}</p>
      <p>If you did not expect this invite, you can safely ignore this email.</p>
    </div>
  `;

  return sendEmail({
    to: args.to,
    subject,
    text,
    html,
  });
}

export async function sendPasswordResetEmail(args: {
  to: string;
  name?: string;
  resetUrl: string;
}) {
  const displayName = String(args.name || "").trim();
  const subject = "Reset your NLM OS password";

  const text = [
    displayName ? `Hi ${displayName},` : "Hi,",
    "",
    "We received a request to reset your NLM OS password.",
    "",
    "Use the link below to set a new password:",
    args.resetUrl,
    "",
    "If you did not request a password reset, you can safely ignore this email.",
  ].join("\n");

  const html = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.6;">
      <h2 style="margin:0 0 12px;color:#002c55;">Reset your NLM OS password</h2>
      <p>${displayName ? `Hi ${escapeHtml(displayName)},` : "Hi,"}</p>
      <p>We received a request to reset your NLM OS password.</p>
      <p>
        <a href="${args.resetUrl}" style="display:inline-block;padding:12px 18px;border-radius:10px;background:#002c55;color:#ffffff;text-decoration:none;font-weight:700;">
          Reset password
        </a>
      </p>
      <p style="word-break:break-all;">${escapeHtml(args.resetUrl)}</p>
      <p>If you did not request a password reset, you can safely ignore this email.</p>
    </div>
  `;

  return sendEmail({
    to: args.to,
    subject,
    text,
    html,
  });
}

export async function sendTaskShareEmail(args: {
  to: string;
  taskTitle: string;
  clientName?: string;
  sharedByName?: string;
  shareUrl: string;
  message?: string;
}) {
  const subject = `Task shared with you: ${args.taskTitle}`;

  const text = [
    `${args.sharedByName || "A teammate"} shared a task with you.`,
    "",
    `Task: ${args.taskTitle}`,
    args.clientName ? `Client: ${args.clientName}` : "",
    "",
    args.message ? `Message:\n${args.message}\n` : "",
    "Open the task here:",
    args.shareUrl,
  ]
    .filter(Boolean)
    .join("\n");

  const html = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.6;">
      <h2 style="margin:0 0 12px;color:#002c55;">Task shared with you</h2>
      <p><strong>${escapeHtml(args.sharedByName || "A teammate")}</strong> shared a task with you.</p>
      <p><strong>Task:</strong> ${escapeHtml(args.taskTitle)}</p>
      ${
        args.clientName
          ? `<p><strong>Client:</strong> ${escapeHtml(args.clientName)}</p>`
          : ""
      }
      ${
        args.message
          ? `<p><strong>Message:</strong><br/>${escapeHtml(args.message)}</p>`
          : ""
      }
      <p>
        <a href="${args.shareUrl}" style="display:inline-block;padding:12px 18px;border-radius:10px;background:#002c55;color:#ffffff;text-decoration:none;font-weight:700;">
          Open task
        </a>
      </p>
      <p style="word-break:break-all;">${escapeHtml(args.shareUrl)}</p>
    </div>
  `;

  return sendEmail({
    to: args.to,
    subject,
    text,
    html,
  });
}

export async function sendTaskAssignedEmail(args: {
  to: string;
  assigneeName?: string;
  taskTitle: string;
  clientName?: string;
  assignedByName?: string;
  shareUrl: string;
  clickupUrl?: string | null;
}) {
  const subject = `Task assigned to you: ${args.taskTitle}`;

  const text = [
    args.assigneeName ? `Hi ${args.assigneeName},` : "Hi,",
    "",
    `${args.assignedByName || "A teammate"} assigned a task to you.`,
    "",
    `Task: ${args.taskTitle}`,
    args.clientName ? `Client: ${args.clientName}` : "",
    "",
    "Open in NLM OS:",
    args.shareUrl,
    args.clickupUrl ? `\nOpen in ClickUp:\n${args.clickupUrl}` : "",
  ]
    .filter(Boolean)
    .join("\n");

  const html = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.6;">
      <h2 style="margin:0 0 12px;color:#002c55;">Task assigned to you</h2>
      <p>${escapeHtml(args.assigneeName ? `Hi ${args.assigneeName},` : "Hi,")}</p>
      <p><strong>${escapeHtml(args.assignedByName || "A teammate")}</strong> assigned a task to you.</p>
      <p><strong>Task:</strong> ${escapeHtml(args.taskTitle)}</p>
      ${
        args.clientName
          ? `<p><strong>Client:</strong> ${escapeHtml(args.clientName)}</p>`
          : ""
      }
      <p>
        <a href="${args.shareUrl}" style="display:inline-block;padding:12px 18px;border-radius:10px;background:#002c55;color:#ffffff;text-decoration:none;font-weight:700;">
          Open in NLM OS
        </a>
      </p>
      ${
        args.clickupUrl
          ? `<p><a href="${args.clickupUrl}" style="color:#2563eb;text-decoration:none;font-weight:700;">Open in ClickUp</a></p>`
          : ""
      }
    </div>
  `;

  return sendEmail({
    to: args.to,
    subject,
    text,
    html,
  });
}
