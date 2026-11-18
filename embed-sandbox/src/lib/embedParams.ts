export interface EmbedParams {
  theme: string;
  menuPosition: 'default' | 'top' | 'bottom' | 'none';
  hideFolderNavigation: boolean;
  hideTooltip: boolean;
  hideViewSelector: boolean;
  responsiveHeight: boolean;
  lng: string;
}

export const DEFAULT_PARAMS: EmbedParams = {
  theme: '',
  menuPosition: 'default',
  hideFolderNavigation: false,
  hideTooltip: false,
  hideViewSelector: false,
  responsiveHeight: false,
  lng: '',
};

/** Appends Sigma embed URL parameters (":name=value") to a base embed URL. */
export function applyParams(
  url: string,
  p: EmbedParams,
  variables: Record<string, string> = {},
): string {
  const q: string[] = [];
  if (p.theme) q.push(`:theme=${encodeURIComponent(p.theme)}`);
  if (p.menuPosition !== 'default') q.push(`:menu_position=${p.menuPosition}`);
  if (p.hideFolderNavigation) q.push(':hide_folder_navigation=true');
  if (p.hideTooltip) q.push(':hide_tooltip=true');
  if (p.hideViewSelector) q.push(':hide_view_selector=true');
  if (p.responsiveHeight) q.push(':responsive_height=true');
  if (p.lng) q.push(`:lng=${encodeURIComponent(p.lng)}`);
  for (const [k, v] of Object.entries(variables)) q.push(`${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
  if (!q.length) return url;
  return url + (url.includes('?') ? '&' : '?') + q.join('&');
}

/** Hides the JWT so URLs can be displayed without leaking a live token. */
export function redact(url: string): string {
  return url.replace(/(:jwt=)[^&]+/, '$1<redacted>');
}
