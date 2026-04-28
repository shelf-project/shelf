import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { PollingProvider } from "./polling";
import { ShortcutsProvider } from "./shortcuts";
import { ThemeProvider } from "./theme";
import "./styles.css";

const container = document.getElementById("root");
if (!container) {
  throw new Error("shelfd ui: missing #root element");
}
createRoot(container).render(
  <React.StrictMode>
    <ThemeProvider>
      <PollingProvider>
        <ShortcutsProvider>
          <App />
        </ShortcutsProvider>
      </PollingProvider>
    </ThemeProvider>
  </React.StrictMode>,
);
