import { createContext, useContext, type ReactNode } from 'react';
import { useSignalR, type SignalRState } from '../hooks/useSignalR';

const SignalRContext = createContext<SignalRState & { reconnect: () => Promise<void> } | null>(null);

export function SignalRProvider({ children }: { children: ReactNode }) {
  const signalR = useSignalR();
  return <SignalRContext.Provider value={signalR}>{children}</SignalRContext.Provider>;
}

export function useSignalRContext() {
  const context = useContext(SignalRContext);
  if (!context) {
    throw new Error('useSignalRContext must be used within SignalRProvider');
  }
  return context;
}
