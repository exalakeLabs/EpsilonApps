import * as sdk from '@sigmacomputing/embed-sdk';

export interface SigmaEvent {
  id: number;
  at: number;
  name: string;
  payload: unknown;
}

type Attach = (
  event: MessageEvent,
  iframe: HTMLIFrameElement,
  cb: (payload: unknown) => void,
) => void;

/** Every inbound (embed -> host) listener the SDK exposes, keyed by its wire event name. */
export const INBOUND_EVENTS: { name: string; attach: Attach }[] = [
  { name: 'workbook:loaded', attach: sdk.workbookLoadedListener },
  { name: 'workbook:error', attach: sdk.workbookErrorListener },
  { name: 'workbook:dataloaded', attach: sdk.workbookDataLoadedListener },
  { name: 'workbook:variables:onchange', attach: sdk.workbookVariableChangeListener },
  { name: 'workbook:variables:current', attach: sdk.workbookVariableCurrentListener },
  { name: 'workbook:sharinglink:update', attach: sdk.workbookLinkSharingUpdateListener },
  { name: 'workbook:table:oncellselect', attach: sdk.workbookTableCellSelectListener },
  { name: 'workbook:pivottable:oncellselect', attach: sdk.workbookPivotTableCellSelectListener },
  { name: 'workbook:chart:onvalueselect', attach: sdk.workbookChartValueSelectListener },
  { name: 'workbook:chart:error', attach: sdk.workbookChartErrorListener },
  { name: 'workbook:selectednodeid:onchange', attach: sdk.workbookSelectedNodeListener },
  { name: 'workbook:fullscreen:onchange', attach: sdk.workbookFullScreenListener },
  { name: 'workbook:pageheight:onchange', attach: sdk.workbookPageHeightListener },
  { name: 'workbook:published', attach: sdk.workbookPublishedListener },
  { name: 'workbook:exploreKey:onchange', attach: sdk.workbookExploreKeyOnChangeListener },
  { name: 'workbook:bookmark:oncreate', attach: sdk.workbookBookmarkCreateListener },
  { name: 'workbook:bookmark:onchange', attach: sdk.workbookBookmarkOnChangeListener },
  { name: 'workbook:bookmark:onupdate', attach: sdk.workbookBookmarkOnUpdateListener },
  { name: 'workbook:bookmark:ondelete', attach: sdk.workbookBookmarkOnDeleteListener },
  { name: 'url:onchange', attach: sdk.urlOnChangeListener },
  { name: 'workbook:id:onchange', attach: sdk.workbookIdOnChangeListener },
  { name: 'action:outbound', attach: sdk.actionOutboundListener },
];

/** Outbound (host -> embed) commands, bound to an iframe. */
export function commands(ref: { current: HTMLIFrameElement | null }) {
  const guard = <A extends unknown[]>(fn: (f: HTMLIFrameElement, ...a: A) => void) =>
    (...a: A) => {
      if (ref.current) fn(ref.current, ...a);
    };
  return {
    listVariables: guard(sdk.workbookVariablesList),
    updateVariables: guard(sdk.workbookVariablesUpdate),
    fullscreen: guard(sdk.workbookFullscreenUpdate),
    selectNode: guard(sdk.workbookSelectedNodeIdUpdate),
    bookmarkCreate: guard(sdk.workbookBookmarkCreate),
    bookmarkUpdate: guard(sdk.workbookBookmarkUpdate),
    bookmarkSelect: guard(sdk.workbookBookmarkSelect),
    bookmarkDelete: guard(sdk.workbookBookmarkDelete),
  };
}
