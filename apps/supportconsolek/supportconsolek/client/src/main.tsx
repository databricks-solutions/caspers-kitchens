import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App.tsx';
import { ErrorBoundary } from './ErrorBoundary.tsx';

// Force a consistent light theme regardless of host dark-mode defaults.
document.documentElement.classList.remove("dark");
document.documentElement.classList.add("light");
document.body.classList.remove("dark");
document.body.classList.add("light");

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </StrictMode>
);
