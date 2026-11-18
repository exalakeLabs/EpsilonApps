import { useEffect, useRef, type RefObject } from 'react';
import { INBOUND_EVENTS, type SigmaEvent } from './events';

let seq = 0;

interface Props {
  src: string;
  title: string;
  iframeRef?: RefObject<HTMLIFrameElement | null>;
  onEvent?: (e: SigmaEvent) => void;
  height?: number | string;
}

/** An iframe that forwards every Sigma SDK event to `onEvent`. */
export function SigmaFrame({ src, title, iframeRef, onEvent, height = '100%' }: Props) {
  const local = useRef<HTMLIFrameElement | null>(null);
  const ref = iframeRef ?? local;
  const handler = useRef(onEvent);
  handler.current = onEvent;

  useEffect(() => {
    const listener = (event: MessageEvent) => {
      const iframe = ref.current;
      if (!iframe || event.source !== iframe.contentWindow) return;
      for (const { name, attach } of INBOUND_EVENTS) {
        attach(event, iframe, (payload) =>
          handler.current?.({ id: ++seq, at: Date.now(), name, payload }),
        );
      }
    };
    window.addEventListener('message', listener);
    return () => window.removeEventListener('message', listener);
  }, [ref]);

  return (
    <iframe
      ref={ref}
      src={src}
      title={title}
      className="sigma-frame"
      style={{ height }}
      allow="clipboard-write; fullscreen"
    />
  );
}
