import { Plugin, PluginEvent, PluginMeta, Properties, RetryError } from '@posthog/plugin-scaffold'
import fetch, { RequestInit, Response } from 'node-fetch'

declare const posthog: {
    api: {
        post: (
            endpoint: string,
            options: {
                data: Record<string, unknown>
            }
        ) => Promise<void>
    }
}

export type FetchBraze = (
    endpoint: string,
    options: Partial<RequestInit>,
    method: string
) => Promise<Record<string, unknown> | null>

type BooleanChoice = 'Yes' | 'No'

type BrazePlugin = Plugin<{
    global: {
        fetchBraze: FetchBraze
    }
    config: {
        brazeEndpoint: 'US-01' | 'US-02' | 'US-03' | 'US-04' | 'US-05' | 'US-06' | 'US-08' | 'EU-01' | 'EU-02'
        apiKey: string
        importCampaigns: BooleanChoice
        importCanvases: BooleanChoice
        importCustomEvents: BooleanChoice
        importFeeds: BooleanChoice
        importKPIs: BooleanChoice
        importSegments: BooleanChoice
        importSessions: BooleanChoice
        eventsToExport: string
        userPropertiesToExport: string
        importUserAttributesInAllEvents: BooleanChoice
    }
}>

// NOTE: type is exported for tests
export type BrazeMeta = PluginMeta<BrazePlugin>

interface PostHogEvent extends Partial<PluginEvent> {
    event: PluginEvent['event']
}

const ONE_HOUR = 1000 * 60 * 60
const ONE_DAY = ONE_HOUR * 24

const ENDPOINTS_MAP = {
    'US-01': 'https://rest.iad-01.braze.com',
    'US-02': 'https://rest.iad-02.braze.com',
    'US-03': 'https://rest.iad-03.braze.com',
    'US-04': 'https://rest.iad-04.braze.com',
    'US-05': 'https://rest.iad-05.braze.com',
    'US-06': 'https://rest.iad-06.braze.com',
    'US-08': 'https://rest.iad-08.braze.com',
    'EU-01': 'https://rest.fra-01.braze.eu',
    'EU-02': 'https://rest.fra-02.braze.eu',
}

export async function setupPlugin({ config, global }: BrazeMeta): Promise<void> {
    const brazeUrl = ENDPOINTS_MAP[config.brazeEndpoint]
    // we define a global fetch function that handles authentication and API errors
    global.fetchBraze = async (endpoint: string, options = {}, method = 'GET') => {
        const headers = {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            Authorization: `Bearer ${config.apiKey}`,
        }

        let response: Response | undefined

        // TEMP: Debugging
        const startTime = Date.now()

        try {
            response = await fetch(`${brazeUrl}${endpoint}`, {
                method,
                headers,
                ...options,
                timeout: 5000,
            })
        } catch (e) {
            console.error(e)
            throw new RetryError('Fetch failed, retrying.')
        } finally {
            // TEMP: Debugging
            console.log(`Fetch took ${(Date.now() - startTime) / 1000} seconds.`)
        }

        if (String(response.status)[0] === '5') {
            throw new RetryError('Service is down, retry later')
        }

        if (String(response.status)[0] !== '2') {
            return null
        }

        const responseJson = await response.json()
        if (responseJson['errors']) {
            const errors = responseJson['errors'] as string[]
            errors.forEach((error) => console.error(error))
        }
        return responseJson
    }
}

export const jobs = {
    trackCampaigns,
    trackCampaign,
    trackCanvases,
    trackCanvas,
    trackCustomEvents,
    trackCustomEvent,
    trackKPIs,
    trackFeeds,
    trackFeed,
    trackSegments,
    trackSegment,
    trackSessions,
}

// the jobs are run once every day, and the different imports are run as separate async jobs
export async function runEveryDay(meta: BrazeMeta): Promise<void> {
    if (meta.config.importCampaigns === 'Yes') {
        await meta.jobs.trackCampaigns({}).runNow()
    }
    if (meta.config.importCanvases === 'Yes') {
        await meta.jobs.trackCanvases({}).runNow()
    }
    if (meta.config.importCustomEvents === 'Yes') {
        await meta.jobs.trackCustomEvents({}).runNow()
    }
    if (meta.config.importKPIs === 'Yes') {
        await meta.jobs.trackKPIs({}).runNow()
    }
    if (meta.config.importFeeds === 'Yes') {
        await meta.jobs.trackFeeds({}).runNow()
    }
    if (meta.config.importSegments === 'Yes') {
        await meta.jobs.trackSegments({}).runNow()
    }
    if (meta.config.importSessions === 'Yes') {
        await meta.jobs.trackSessions({}).runNow()
    }
}

export enum BrazeObject {
    campaigns = 'campaigns',
    canvas = 'canvas',
    events = 'events',
    new_users = 'kpi/new_users',
    active_users = 'kpi/dau',
    monthly_active_users = 'kpi/mau',
    uninstalls = 'kpi/uninstalls',
    feed = 'feed',
    segments = 'segments',
    sessions = 'sessions',
}

export const BRAZE_PAGINATION_BY_OBJECT_TYPE = {
    campaigns: 100,
    canvas: 100,
    events: 250,
    feed: 100,
    segments: 100,
}

type Item = {
    id: string
    name: string
}

// different Braze endpoints allow for pagination, with different pagination limits, defined in BRAZE_PAGINATION_BY_OBJECT_TYPE
// we run a callback to fetch items from the API and re run if we hit the pagination limit
export async function paginateItems(
    brazeObject: BrazeObject,
    paginateAfter = 100,
    callback: (BrazeObject: BrazeObject, page: number, fetchBraze: FetchBraze) => Promise<Item[]>,
    fetchBraze: FetchBraze
): Promise<Item[]> {
    let currentPage = 0
    let items: Item[] = []
    let currentBatch: Item[] = []
    let runCycle = true
    while (runCycle) {
        currentPage += 1
        currentBatch = await callback(brazeObject, currentPage, fetchBraze)
        items = items.concat(...currentBatch)
        runCycle = currentBatch.length === paginateAfter
    }
    return items
}

type ListResponse<T extends BrazeObject> = {
    [key in T]: {
        id: string
        name: string
    }[]
}

// whenever we call a /list endpoint, we are only interested in fetching the ids and names of items in the list
async function getItems<T extends BrazeObject>(brazeObject: T, page: number, fetchBraze: FetchBraze): Promise<Item[]> {
    const response = (await fetchBraze(`/${brazeObject}/list?page=${page}`, {}, 'GET')) as ListResponse<T> | null
    if (response) {
        return response[brazeObject].map((o) => {
            return { id: o.id, name: o.name }
        })
    } else {
        return []
    }
}

interface BrazeObjectDetailsResponse {
    draft: boolean
    last_sent?: string
    last_entry?: string
    end_at?: string
}

// for certain objects, we call the /details endpoint to determine if the object is currently active
export async function isBrazeObjectActive<T extends BrazeObject>(
    brazeObject: T,
    idKey: string,
    id: string,
    fetchBraze: FetchBraze
): Promise<boolean> {
    const response = (await fetchBraze(
        `/${brazeObject}/details?${idKey}=${id}`,
        {},
        'GET'
    )) as BrazeObjectDetailsResponse | null
    if (!response || response.draft) {
        return false
    }
    // we only parse objects which were active in the 24 hours before the last UTC midnight
    const lastActive = response.last_entry || response.last_sent || response.end_at
    if (lastActive) {
        return getLastUTCMidnight().getTime() - new Date(lastActive).getTime() < ONE_DAY
    }
    return true
}

interface DataSeriesResponse<T> {
    data: T
}

// endpoint to fetch data series in the 24 hours before the last UTC midnight
async function getDataSeries<T>(
    brazeObject: BrazeObject,
    query: string,
    fetchBraze: FetchBraze
): Promise<DataSeriesResponse<T>['data'] | null> {
    const response = (await fetchBraze(
        `/${brazeObject}/data_series?${query}&ending_date=${getLastUTCMidnight()}`,
        {},
        'GET'
    )) as DataSeriesResponse<T> | null
    return response ? response.data : null
}

/* CAMPAIGNS */

async function trackCampaigns({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const campaigns = await paginateItems(
        BrazeObject.campaigns,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.campaigns,
        getItems,
        meta.global.fetchBraze
    )
    // for each campaign, we run the export asynchronously
    for (const campaign of campaigns) {
        await meta.jobs.trackCampaign(campaign).runNow()
    }
}

type MessageStats = Record<string, Array<Record<string, string | number> & { variation_name?: string }>>

// FIX ME: typescript forces to have a union here, which means MessageStats could be used under another key
// see Campaign Data Series object schema here: https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_analytics/
export type CampaignDataSeries = Record<string, string | number | MessageStats> & {
    time: string
    messages: MessageStats
}

export function transformCampaignDataSeriesToPostHogEvents(
    dataSeries: CampaignDataSeries[],
    name: string
): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of dataSeries) {
        const properties = Object.keys(item).reduce((result: Record<string, string | number>, currentKey: string) => {
            if (currentKey === 'messages') {
                for (const messageKey of Object.keys(item.messages)) {
                    const currentMessages = item.messages[messageKey]
                    for (const variation of currentMessages) {
                        const variationName = variation['variation_name']
                        const variationKey = variationName ? `${messageKey}:${variationName}` : messageKey
                        for (const subKey of Object.keys(variation)) {
                            if (subKey !== 'variation_name') {
                                result[`${variationKey}:${subKey}`] = variation[subKey]
                            }
                        }
                    }
                }
            } else {
                if (currentKey !== 'time') {
                    //@ts-expect-error type error related to the MessageStats FIX ME above
                    result[currentKey] = item[currentKey]
                }
            }
            return result
        }, {})
        events.push({
            event: `Braze campaign: ${name}`,
            properties,
            timestamp: item.time,
        })
    }
    return events
}

async function trackCampaign(item: Item, meta: BrazeMeta): Promise<void> {
    const isActive = await isBrazeObjectActive(BrazeObject.campaigns, 'campaign_id', item.id, meta.global.fetchBraze)
    if (!isActive) {
        return
    }
    const query = `$campaign_id=${item.id}&length=1`
    const dataSeries = await getDataSeries<CampaignDataSeries[]>(BrazeObject.campaigns, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformCampaignDataSeriesToPostHogEvents(dataSeries, item.name)
    await posthogBatchCapture(events)
}

/* CANVAS */

async function trackCanvases({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const canvases = await paginateItems(
        BrazeObject.campaigns,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.canvas,
        getItems,
        meta.global.fetchBraze
    )
    for (const canvas of canvases) {
        await meta.jobs.trackCanvas(canvas).runNow()
    }
}

// see Canvas Data Series object schema here: https://www.braze.com/docs/api/endpoints/export/canvas/get_canvas_analytics/
export type CanvasDataSeries = {
    name: string
    stats: {
        time: string
        total_stats: Record<string, number>
        variant_stats: Record<string, Record<string, string | number>>
        step_stats: Record<string, Record<string, string | number | MessageStats>> // same type problem as above with the CampaignDataSeries
    }[]
}

const mapAndPrependKeys = (object: Record<string, string | number>, prependKey: string, skipKeys: string[] = []) => {
    const result: Record<string, string | number> = {}
    Object.keys(object).forEach((key) => {
        if (!skipKeys.includes(key)) {
            result[`${prependKey}${key}`] = object[key]
        }
    })
    return result
}

export function transformCanvasDataSeriesToPostHogEvents(dataSeries: CanvasDataSeries, name: string): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const series of dataSeries.stats) {
        const properties = Object.keys(series).reduce((result: Record<string, string | number>, currentKey: string) => {
            switch (currentKey) {
                case 'total_stats':
                    // we remap the keys in the result by prepending `total_stats:`
                    result = { ...result, ...mapAndPrependKeys(series.total_stats, 'total_stats:') }
                    break
                case 'variant_stats':
                    // for each variant, we remap the keys in the result by prepending `variant_stats:` and name of the variant
                    for (const variantKey of Object.keys(series.variant_stats)) {
                        const variant = series.variant_stats[variantKey]
                        result = {
                            ...result,
                            ...mapAndPrependKeys(variant, `variant_stats:${variant.name}:`, ['name']),
                        }
                    }
                    break
                case 'step_stats':
                    // for each step and each variant, we remap the keys in the result by prepending `step_stats:`, the name of the variant, the name of the step
                    for (const stepKey of Object.keys(series.step_stats)) {
                        const step = series.step_stats[stepKey]
                        for (const messageKey of Object.keys(step.messages)) {
                            const currentMessages = (step.messages as MessageStats)[messageKey]
                            // each step contains messages, each message contains multiple variations
                            for (const variation of currentMessages) {
                                const variationName = variation['variation_name']
                                // if a variation_name is provided, we add it to the key
                                const variationKey = variationName ? `${messageKey}:${variationName}` : messageKey
                                result = {
                                    ...result,
                                    ...mapAndPrependKeys(variation, `step_stats:${step.name}:${variationKey}:`, [
                                        'variation_name',
                                    ]),
                                }
                            }
                        }
                        result = {
                            ...result,
                            ...mapAndPrependKeys(step as Record<string, string | number>, `step_stats:${step.name}:`, [
                                'messages',
                                'name',
                            ]),
                        }
                    }
                    break
            }
            return result
        }, {})
        events.push({
            event: `Braze canvas: ${name}`,
            properties,
            timestamp: series.time,
        })
    }
    return events
}

async function trackCanvas(item: Item, meta: BrazeMeta): Promise<void> {
    const isActive = await isBrazeObjectActive(BrazeObject.canvas, 'canvas_id', item.id, meta.global.fetchBraze)
    if (!isActive) {
        return
    }
    const query = `canvas_id=${item.id}&length=1&include_variant_breakdown=true&include_step_breakdown=true`
    const dataSeries = await getDataSeries<CanvasDataSeries>(BrazeObject.canvas, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformCanvasDataSeriesToPostHogEvents(dataSeries, item.name)
    await posthogBatchCapture(events)
}

/* CUSTOM EVENTS */

async function getEvents(_: unknown, page: number, fetchBraze: FetchBraze): Promise<Item[]> {
    const response = (await fetchBraze(`/events/list?page=${page}`, {}, 'GET')) as { events: string[] } | null
    if (response) {
        return response.events.map((e) => {
            return { id: e, name: e }
        })
    } else {
        return []
    }
}

async function trackCustomEvents({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const events = await paginateItems(
        BrazeObject.events,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.events,
        getEvents,
        meta.global.fetchBraze
    )
    for (const event of events) {
        await meta.jobs.trackCustomEvent(event).runNow()
    }
}

type CustomEventDataSeries = {
    time: string
    count: number
}

export function transformCustomEventDataSeriesToPostHogEvents(
    series: CustomEventDataSeries[],
    event: string
): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: `Braze event: ${event}`,
            timestamp: item.time,
            properties: {
                count: item.count,
            },
        })
    }
    return events
}

async function trackCustomEvent(
    {
        event,
    }: {
        event: string
    },
    meta: BrazeMeta
): Promise<void> {
    const query = `event=${event}&length=1&unit=day`
    const dataSeries = await getDataSeries<CustomEventDataSeries[]>(BrazeObject.events, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformCustomEventDataSeriesToPostHogEvents(dataSeries, event)
    await posthogBatchCapture(events)
}

/* KPIs */

type NewUsersDataSeries = {
    time: string
    new_users: number
}

export function transformNewUsersDataSeriesToPostHogEvents(series: NewUsersDataSeries[]): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: 'Braze KPI: Daily New Users',
            timestamp: item.time,
            properties: {
                count: item.new_users,
            },
        })
    }
    return events
}

async function trackDailyNewUsers(meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<NewUsersDataSeries[]>(BrazeObject.new_users, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformNewUsersDataSeriesToPostHogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type ActiveUsersDataSeries = {
    time: string
    dau: number
}

export function transformActiveUsersDataSeriesToPostHogEvents(series: ActiveUsersDataSeries[]): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: 'Braze KPI: Daily Active Users',
            timestamp: item.time,
            properties: {
                count: item.dau,
            },
        })
    }
    return events
}

async function trackDailyActiveUsers(meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<ActiveUsersDataSeries[]>(
        BrazeObject.active_users,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformActiveUsersDataSeriesToPostHogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type MonthlyActiveUsersDataSeries = {
    time: string
    mau: number
}

export function transformMonthlyActiveUsersDataSeriesToPostHogEvents(
    series: MonthlyActiveUsersDataSeries[]
): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: 'Braze KPI: Monthly Active Users',
            timestamp: item.time,
            properties: {
                count: item.mau,
            },
        })
    }
    return events
}

async function trackMonthlyActiveUsers(meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<MonthlyActiveUsersDataSeries[]>(
        BrazeObject.monthly_active_users,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformMonthlyActiveUsersDataSeriesToPostHogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type DailyUninstallsDataSeries = {
    time: string
    uninstalls: number
}

export function transformDailyUninstallsDataSeriesToPostHogEvents(series: DailyUninstallsDataSeries[]): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: 'Braze KPI: Daily Uninstalls',
            timestamp: item.time,
            properties: {
                count: item.uninstalls,
            },
        })
    }
    return events
}

async function trackDailyUninstalls(meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<DailyUninstallsDataSeries[]>(
        BrazeObject.uninstalls,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformDailyUninstallsDataSeriesToPostHogEvents(dataSeries)
    await posthogBatchCapture(events)
}

async function trackKPIs(_: unknown, meta: BrazeMeta): Promise<void> {
    await trackDailyNewUsers(meta)
    await trackDailyActiveUsers(meta)
    await trackMonthlyActiveUsers(meta)
    await trackDailyUninstalls(meta)
}

/* NEWS FEED CARDS */

async function trackFeeds({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const feeds = await paginateItems(
        BrazeObject.feed,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.feed,
        getItems,
        meta.global.fetchBraze
    )
    for (const feed of feeds) {
        await meta.jobs.trackFeed(feed).runNow()
    }
}

export type FeedDataSeries = {
    time: string
    clicks: number
    impressions: number
    unique_clicks: number
    unique_impressions: number
}

export function transformFeedDataSeriesToPostHogEvents(dataSeries: FeedDataSeries[], name: string): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of dataSeries) {
        events.push({
            event: `Braze News Feed Card: ${name}`,
            properties: {
                clicks: item.clicks,
                impressions: item.impressions,
                unique_clicks: item.unique_clicks,
                unique_impressions: item.unique_impressions,
            },
            timestamp: item.time,
        })
    }
    return events
}

async function trackFeed(item: Item, meta: BrazeMeta): Promise<void> {
    const isActive = await isBrazeObjectActive(BrazeObject.feed, 'card_id', item.id, meta.global.fetchBraze)
    if (!isActive) {
        return
    }
    const query = `$card_id=${item.id}&length=1&unit=day`
    const dataSeries = await getDataSeries<FeedDataSeries[]>(BrazeObject.feed, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformFeedDataSeriesToPostHogEvents(dataSeries, item.name)
    await posthogBatchCapture(events)
}

/* SEGMENTS */

async function trackSegments({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const segments = await paginateItems(
        BrazeObject.segments,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.segments,
        getItems,
        meta.global.fetchBraze
    )
    for (const segment of segments) {
        await meta.jobs.trackSegment(segment).runNow()
    }
}

export type SegmentDataSeries = {
    time: string
    size: number
}

export function transformSegmentDataSeriesToPostHogEvents(
    dataSeries: SegmentDataSeries[],
    name: string
): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of dataSeries) {
        events.push({
            event: `Braze Segment: ${name}`,
            properties: {
                count: item.size,
            },
            timestamp: item.time,
        })
    }
    return events
}

async function trackSegment(item: Item, meta: BrazeMeta): Promise<void> {
    const isActive = await isBrazeObjectActive(BrazeObject.segments, 'segment_id', item.id, meta.global.fetchBraze)
    if (!isActive) {
        return
    }
    const query = `$segment_id=${item.id}&length=1`
    const dataSeries = await getDataSeries<SegmentDataSeries[]>(BrazeObject.segments, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformSegmentDataSeriesToPostHogEvents(dataSeries, item.name)
    await posthogBatchCapture(events)
}

type SessionsDataSeries = {
    time: string
    sessions: number
}

/* SESSIONS */

export function transformSessionsDataSeriesToPostHogEvents(series: SessionsDataSeries[]): PostHogEvent[] {
    const events: PostHogEvent[] = []
    for (const item of series) {
        events.push({
            event: 'Braze Sessions',
            timestamp: item.time,
            properties: {
                count: item.sessions,
            },
        })
    }
    return events
}

async function trackSessions(_: unknown, meta: BrazeMeta): Promise<void> {
    const query = 'length=1&unit=day'
    const dataSeries = await getDataSeries<SessionsDataSeries[]>(BrazeObject.sessions, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformSessionsDataSeriesToPostHogEvents(dataSeries)
    await posthogBatchCapture(events)
}

async function posthogBatchCapture(batch: PostHogEvent[]) {
    await posthog.api.post('/capture/', {
        data: {
            batch,
        },
    })
}

export function ISODateString(d: Date): string {
    function pad(n: number) {
        return n < 10 ? '0' + n : n
    }
    return (
        d.getUTCFullYear() +
        '-' +
        pad(d.getUTCMonth() + 1) +
        '-' +
        pad(d.getUTCDate()) +
        'T' +
        pad(d.getUTCHours()) +
        ':' +
        pad(d.getUTCMinutes()) +
        ':' +
        pad(d.getUTCSeconds()) +
        '.' +
        pad(d.getUTCMilliseconds()) +
        'Z'
    )
}

function getLastUTCMidnight() {
    const now = new Date()
    return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0)
}

type BrazeUserAlias = { alias_name: string; alias_label: string }

type BrazeAttribute = {
    external_id?: string
    user_alias?: BrazeUserAlias
    braze_id?: string
    _update_existing_only?: boolean
    push_token_import?: boolean
} & Record<string, unknown>

// NOTE: Reference: https://www.braze.com/docs/api/objects_filters/event_object/
type BrazeEvent = {
    external_id?: string
    user_alias?: BrazeUserAlias
    braze_id?: string
    app_id?: string
    name: string
    time: string // ISO 8601 timestamp
    properties?: Record<string, unknown>
    _update_existing_only?: boolean
}

export const onEvent = async (pluginEvent: PluginEvent, meta: BrazeMeta): Promise<void> => {
    // This App supports pushing events to Braze also, via the `onEvent` hook. It
    // should send any $set attributes to Braze `/users/track` endpoint in the
    // `attributes` param as well as events in the `events` property.
    // To enable this functionality, the user must configure the plugin with the
    // config.eventsToExport and config.userPropertiesToExport config options.
    // exportEvents is a comma separated list of event names to export to Braze.
    //
    // See https://www.braze.com/docs/api/endpoints/user_data/post_user_track/
    // for more info.

    if (!meta.config.eventsToExport && !meta.config.userPropertiesToExport) {
        return
    }

    const { event, $set, properties, timestamp } = pluginEvent

    // If we have $set or properties.$set then attributes should be an array
    // of one object. Otherwise it should be an empty array.
    const userProperties: Properties = $set ?? properties?.$set ?? {}
    const propertiesToExport = meta.config.userPropertiesToExport?.split(',') ?? []
    const filteredProperties = Object.keys(userProperties).reduce((filtered, key) => {
        if (propertiesToExport.includes(key)) {
            filtered[key] = userProperties[key]
        }
        return filtered
    }, {} as Properties)

    const shouldImportAttributes =
        meta.config.importUserAttributesInAllEvents === 'Yes' || meta.config.eventsToExport?.split(',').includes(event)

    const attributes: Array<BrazeAttribute> =
        shouldImportAttributes && Object.keys(filteredProperties).length
            ? [{ ...filteredProperties, external_id: pluginEvent.distinct_id }]
            : []

    // If we have an event name in the exportEvents config option then we
    // should export the event to Braze.
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { $set: _set, ...eventProperties } = properties ?? {}
    const events: Array<BrazeEvent> = meta.config.eventsToExport?.split(',').includes(event)
        ? [
              {
                  properties: eventProperties,
                  external_id: pluginEvent.distinct_id,
                  name: event,
                  time: timestamp ? ISODateString(new Date(timestamp)) : ISODateString(getLastUTCMidnight()),
              },
          ]
        : []

    if (attributes.length || events.length) {
        const response = await meta.global.fetchBraze(
            '/users/track',
            {
                body: JSON.stringify({
                    attributes,
                    events,
                }),
            },
            'POST'
        )

        if (response?.message !== 'success') {
            console.error(`Braze API error response: `, response)
            throw new RetryError('Braze API error onEvent, retrying.')
        }
    }
}
