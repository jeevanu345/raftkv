import { useEffect, useRef, useState } from "react";

import { getEventStreamUrl, isDemoMode } from "../lib/api";
import { mockEvents } from "../lib/mock";

import type { RuntimeEvent } from "../types/api";

const MAX_EVENTS = 250;

export function useRaftEvents() {
  const [events, setEvents] = useState<RuntimeEvent[]>(
    isDemoMode() ? [...mockEvents].reverse() : []
  );

  const [connected, setConnected] = useState(
    isDemoMode()
  );

  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (isDemoMode()) {
      return;
    }

    const source = new EventSource(getEventStreamUrl(), {withCredentials:true});

    sourceRef.current = source;

    source.onopen = () => {
      setConnected(true);
    };

    source.onerror = () => {
      setConnected(false);
    };

    source.addEventListener("gap",()=>setEvents([]));
    source.onmessage = (message) => {
      try {
        const event = JSON.parse(
          message.data
        ) as RuntimeEvent;

        if(typeof event.seq!=="number" || typeof event.nodeId!=="number" || typeof event.type!=="string") return;
        setEvents(current=> current.some(e=>e.seq===event.seq)?current:[event,...current].slice(0,MAX_EVENTS));
      } catch {
        // Ignore malformed event.
      }
    };

    return () => {
      source.close();
      sourceRef.current = null;
    };
  }, []);

  function clear() {
    setEvents([]);
  }

  return {
    events,
    connected,
    clear
  };
}
