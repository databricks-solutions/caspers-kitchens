import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.tsx";

document.documentElement.classList.remove("dark");
document.documentElement.classList.add("light");
document.body.classList.remove("dark");
document.body.classList.add("light");

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
