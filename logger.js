const fs = require("fs");
const path = require("path");

let initialized = false;
let logStream = null;
let originalConsole = null;

const colors = { reset: "\x1b[0m", red: "\x1b[31m", yellow: "\x1b[33m" };

function fmt(args) {
  return args
    .map((a) => {
      if (typeof a === "string") return a;
      if (a instanceof Error) return a.stack || a.message;
      try {
        return JSON.stringify(a);
      } catch {
        return String(a);
      }
    })
    .join(" ");
}

function tsDhaka() {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Dhaka",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
    timeZoneName: "short",
  }).format(new Date());
}

function initLogger(baseName = "app") {
  if (initialized) return; // prevent double init

  const LOG_DIR = path.join(__dirname, "logs");
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  } catch {}
  const LOG_FILE = path.join(LOG_DIR, `${baseName}.txt`);
  logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });

  originalConsole = {
    log: console.log,
    warn: console.warn,
    error: console.error,
  };

  function write(line, color) {
    if (!logStream) return;
    if (color) logStream.write(color + line + colors.reset + "\n");
    else logStream.write(line + "\n");
  }

  console.log = (...args) => {
    const line = `[${tsDhaka()}] INFO: ${fmt(args)}`;
    originalConsole.log(line);
    write(line);
  };
  console.warn = (...args) => {
    const line = `[${tsDhaka()}] WARN: ${fmt(args)}`;
    originalConsole.warn(colors.yellow + line + colors.reset);
    write(line, colors.yellow);
  };
  console.error = (...args) => {
    const line = `[${tsDhaka()}] ERROR: ${fmt(args)}`;
    originalConsole.error(colors.red + line + colors.reset);
    write(line, colors.red);
  };

  process.on("exit", () => {
    try {
      logStream && logStream.end();
    } catch {}
  });
  initialized = true;
}

module.exports = { initLogger };
