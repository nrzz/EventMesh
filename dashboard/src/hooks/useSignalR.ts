import { useCallback, useEffect, useRef, useState } from 'react';
import {
  HubConnection,
  HubConnectionBuilder,
  HubConnectionState,
  LogLevel,
} from '@microsoft/signalr';
import { getDashboardSettings } from '../services/api';
import type { MetricsSnapshot, OverviewInfo } from '../types/api';

export interface SignalRState {
  connected: boolean;
  connectionState: HubConnectionState | 'disconnected';
  overview: OverviewInfo | null;
  metrics: MetricsSnapshot | null;
  lastEventAt: string | null;
  error: string | null;
}

const initialState: SignalRState = {
  connected: false,
  connectionState: 'disconnected',
  overview: null,
  metrics: null,
  lastEventAt: null,
  error: null,
};

export function useSignalR() {
  const [state, setState] = useState<SignalRState>(initialState);
  const connectionRef = useRef<HubConnection | null>(null);

  const connect = useCallback(async () => {
    const settings = getDashboardSettings();
    const hubUrl = `${settings.apiBaseUrl}/hubs/eventmesh`;

    if (connectionRef.current?.state === HubConnectionState.Connected) {
      return;
    }

    const connection = new HubConnectionBuilder()
      .withUrl(hubUrl, {
        accessTokenFactory: () => settings.apiKey,
        headers: settings.apiKey ? { 'X-Api-Key': settings.apiKey } : undefined,
      })
      .withAutomaticReconnect([0, 2000, 5000, 10000])
      .configureLogging(LogLevel.Information)
      .build();

    connection.on('OverviewUpdated', (overview: OverviewInfo) => {
      setState((prev) => ({
        ...prev,
        overview,
        lastEventAt: new Date().toISOString(),
      }));
    });

    connection.on('MetricsUpdated', (metrics: MetricsSnapshot) => {
      setState((prev) => ({
        ...prev,
        metrics,
        lastEventAt: new Date().toISOString(),
      }));
    });

    connection.onreconnecting(() => {
      setState((prev) => ({
        ...prev,
        connected: false,
        connectionState: HubConnectionState.Reconnecting,
      }));
    });

    connection.onreconnected(() => {
      setState((prev) => ({
        ...prev,
        connected: true,
        connectionState: HubConnectionState.Connected,
        error: null,
      }));
    });

    connection.onclose(() => {
      setState((prev) => ({
        ...prev,
        connected: false,
        connectionState: 'disconnected',
      }));
    });

    try {
      await connection.start();
      await connection.invoke('SubscribeOverview');
      await connection.invoke('SubscribeMetrics');

      connectionRef.current = connection;
      setState((prev) => ({
        ...prev,
        connected: true,
        connectionState: HubConnectionState.Connected,
        error: null,
      }));
    } catch (error) {
      setState((prev) => ({
        ...prev,
        connected: false,
        connectionState: 'disconnected',
        error: error instanceof Error ? error.message : 'Failed to connect to SignalR hub.',
      }));
    }
  }, []);

  useEffect(() => {
    void connect();

    return () => {
      void connectionRef.current?.stop();
      connectionRef.current = null;
    };
  }, [connect]);

  return { ...state, reconnect: connect };
}
