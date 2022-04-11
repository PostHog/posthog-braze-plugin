import { Plugin, PluginMeta, RetryError } from '@posthog/plugin-scaffold'
import fetch, { RequestInit } from 'node-fetch'

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

type BrazePlugin = Plugin<{
    global: {
        fetchBraze: FetchBraze
    }
    config: {
        brazeUrl: string
        apiKey: string
        importCampaigns: string
        importCanvases: string
        importCustomEvents: string
        importFeeds: string
        importKPIs: string
        importSegments: string
        importSessions: string
    }
}>

type BrazeMeta = PluginMeta<BrazePlugin>

interface PosthogEvent {
    event: string
    properties: Record<string, string | number>
    timestamp: string
}

const ONE_HOUR = 1000 * 60 * 60
const ONE_DAY = ONE_HOUR * 24

export async function setupPlugin({ config, global }: BrazeMeta): Promise<void> {
    const brazeUrl = config.brazeUrl.endsWith('/')
        ? config.brazeUrl.substring(0, config.brazeUrl.length - 1)
        : config.brazeUrl
    // we define a global fetch function that handles authentication and API errors
    global.fetchBraze = async (endpoint: string, options = {}, method = 'GET') => {
        const headers = {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            Authorization: `Bearer ${config.apiKey}`,
        }
        const response = await fetch(`${brazeUrl}${endpoint}`, { method, headers, ...options })
        const responseJson = await response.json()
        if (responseJson.get('errors')) {
            const errors = responseJson['errors'] as string[]
            errors.forEach((error) => console.error(error))
        }
        if (String(response.status)[0] === '2') {
            return responseJson
        } else {
            if (String(response.status)[0] === '5') {
                throw new RetryError('Service is down, retry later')
            }
            return null
        }
    }
}

export const jobs = {
    trackCampaigns,
    trackCampaign,
    trackCanvases,
    trackCanvas,
    trackCustomEvents,
    trackCustomEvent,
    trackDailyNewUsers,
    trackDailyActiveUsers,
    trackMonthlyActiveUsers,
    trackDailyUninstalls,
    trackFeeds,
    trackFeed,
    trackSegments,
    trackSegment,
    trackSessions,
}

// the jobs are run once every day, and the different imports are run as separate async jobs
export async function runEveryDay(meta: BrazeMeta): Promise<void> {
    // this is something plugin devs aren't expected to know, but while we're at it, might be best to trigger these
    // such that they don't all run at the same time. Also, do they all need to be separate?
    // Imagine 1000 people on Cloud use this plugin. When 12PM hits we'll trigger 10*1000 = 10000 jobs to be run immediately. 
    // We should be able to handle these of course, but would lighten up the load if we did like runIn(n*5, 'seconds') where n is the 
    // job number
    
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
        await meta.jobs.trackDailyNewUsers({}).runNow()
        await meta.jobs.trackDailyActiveUsers({}).runNow()
        await meta.jobs.trackMonthlyActiveUsers({}).runNow()
        await meta.jobs.trackDailyUninstalls({}).runNow()
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
    
    // nit
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
    campaigns.forEach(async (campaign) => {
        // this syntax might look weird, but we're not awaiting the job itself, just the confirmation that it was stored successfully
        await meta.jobs.trackCampaign(campaign).runNow()
    })
}

type MessageStats = Record<string, Array<Record<string, string | number> & { variation_name?: string }>>

// FIX ME: typescript forces to have a union here, which means MessageStats could be used under another key
// see Campaign Data Series object schema here: https://www.braze.com/docs/api/endpoints/export/campaigns/get_campaign_analytics/
export type CampaignDataSeries = Record<string, string | number | MessageStats> & {
    time: string
    messages: MessageStats
}

export function transformCampaignDataSeriesToPosthogEvents(
    dataSeries: CampaignDataSeries[],
    name: string
): PosthogEvent[] {
    const events: PosthogEvent[] = []
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
            } else if (currentKey !== 'time') {
                //@ts-expect-error type error related to the MessageStats FIX ME above
                result[currentKey] = item[currentKey]
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
    const events = transformCampaignDataSeriesToPosthogEvents(dataSeries, item.name)
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
    canvases.forEach(async (canvas) => {
        await meta.jobs.trackCanvas(canvas).runNow()
    })
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

export function transformCanvasDataSeriesToPosthogEvents(dataSeries: CanvasDataSeries, name: string): PosthogEvent[] {
    const events: PosthogEvent[] = []
    
    // not reading too deeply at this stage to tell you how/if this can be refactored but this looks like a code smell
    // way too much nesting and very hard to read/reason about
    for (const series of dataSeries.stats) {
        const properties = Object.keys(series).reduce((result: Record<string, string | number>, currentKey: string) => {
            switch (currentKey) {
                case 'total_stats':
                    Object.keys(series.total_stats).forEach((key) => {
                        result[`total_stats:${key}`] = series.total_stats[key]
                    })
                    break
                case 'variant_stats':
                    for (const variantKey of Object.keys(series.variant_stats)) {
                        const variant = series.variant_stats[variantKey]
                        Object.keys(variant).forEach((key) => {
                            if (key !== 'name') {
                                result[`variant_stats:${variant.name}:${key}`] = variant[key]
                            }
                        })
                    }
                    break
                case 'step_stats':
                    for (const stepKey of Object.keys(series.step_stats)) {
                        const step = series.step_stats[stepKey]
                        Object.keys(step).forEach((key) => {
                            if (key !== 'name') {
                                // for example, here we can actually do a if key === messages else if key !== name and remove one level of nesting
                                if (key === 'messages') {
                                    for (const messageKey of Object.keys(step.messages)) {
                                        const currentMessages = (step.messages as MessageStats)[messageKey]
                                        for (const variation of currentMessages) {
                                            const variationName = variation['variation_name']
                                            const variationKey = variationName
                                                ? `${messageKey}:${variationName}`
                                                : messageKey
                                            for (const subKey of Object.keys(variation)) {
                                                if (subKey !== 'variation_name') {
                                                    result[`step_stats:${step.name}:${variationKey}:${subKey}`] =
                                                        variation[subKey]
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    //@ts-expect-error type error related to the MessageStats FIX ME above
                                    result[`step_stats:${step.name}:${key}`] = step[key]
                                }
                            }
                        })
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
    const events = transformCanvasDataSeriesToPosthogEvents(dataSeries, item.name)
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
    events.forEach((event) => {
        meta.jobs.trackCustomEvent(event).runNow()
    })
}

type CustomEventDataSeries = {
    time: string
    count: number
}

export function transformCustomEventDataSeriesToPosthogEvents(
    series: CustomEventDataSeries[],
    event: string
): PosthogEvent[] {
    const events: PosthogEvent[] = []
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
    const events = transformCustomEventDataSeriesToPosthogEvents(dataSeries, event)
    await posthogBatchCapture(events)
}

/* KPIs */

type NewUsersDataSeries = {
    time: string
    new_users: number
}

export function transformNewUsersDataSeriesToPosthogEvents(series: NewUsersDataSeries[]): PosthogEvent[] {
    const events: PosthogEvent[] = []
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

async function trackDailyNewUsers(_: unknown, meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<NewUsersDataSeries[]>(BrazeObject.new_users, query, meta.global.fetchBraze)
    if (!dataSeries) {
        return
    }
    const events = transformNewUsersDataSeriesToPosthogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type ActiveUsersDataSeries = {
    time: string
    dau: number
}

export function transformActiveUsersDataSeriesToPosthogEvents(series: ActiveUsersDataSeries[]): PosthogEvent[] {
    const events: PosthogEvent[] = []
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

async function trackDailyActiveUsers(_: unknown, meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<ActiveUsersDataSeries[]>(
        BrazeObject.active_users,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformActiveUsersDataSeriesToPosthogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type MonthlyActiveUsersDataSeries = {
    time: string
    mau: number
}

export function transformMonthlyActiveUsersDataSeriesToPosthogEvents(
    series: MonthlyActiveUsersDataSeries[]
): PosthogEvent[] {
    const events: PosthogEvent[] = []
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

async function trackMonthlyActiveUsers(_: unknown, meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<MonthlyActiveUsersDataSeries[]>(
        BrazeObject.monthly_active_users,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformMonthlyActiveUsersDataSeriesToPosthogEvents(dataSeries)
    await posthogBatchCapture(events)
}

type DailyUninstallsDataSeries = {
    time: string
    uninstalls: number
}

export function transformDailyUninstallsDataSeriesToPosthogEvents(series: DailyUninstallsDataSeries[]): PosthogEvent[] {
    const events: PosthogEvent[] = []
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

async function trackDailyUninstalls(_: unknown, meta: BrazeMeta): Promise<void> {
    const query = 'length=1'
    const dataSeries = await getDataSeries<DailyUninstallsDataSeries[]>(
        BrazeObject.uninstalls,
        query,
        meta.global.fetchBraze
    )
    if (!dataSeries) {
        return
    }
    const events = transformDailyUninstallsDataSeriesToPosthogEvents(dataSeries)
    await posthogBatchCapture(events)
}

/* NEWS FEED CARDS */

async function trackFeeds({}: Record<string, unknown>, meta: BrazeMeta): Promise<void> {
    const feeds = await paginateItems(
        BrazeObject.feed,
        BRAZE_PAGINATION_BY_OBJECT_TYPE.feed,
        getItems,
        meta.global.fetchBraze
    )
    feeds.forEach(async (feed) => {
        await meta.jobs.trackFeed(feed).runNow()
    })
}

export type FeedDataSeries = {
    time: string
    clicks: number
    impressions: number
    unique_clicks: number
    unique_impressions: number
}

export function transformFeedDataSeriesToPosthogEvents(dataSeries: FeedDataSeries[], name: string): PosthogEvent[] {
    const events: PosthogEvent[] = []
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
    const events = transformFeedDataSeriesToPosthogEvents(dataSeries, item.name)
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
    segments.forEach((segment) => {
        meta.jobs.trackSegment(segment).runNow()
    })
}

export type SegmentDataSeries = {
    time: string
    size: number
}

export function transformSegmentDataSeriesToPosthogEvents(
    dataSeries: SegmentDataSeries[],
    name: string
): PosthogEvent[] {
    const events: PosthogEvent[] = []
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
    const events = transformSegmentDataSeriesToPosthogEvents(dataSeries, item.name)
    await posthogBatchCapture(events)
}

type SessionsDataSeries = {
    time: string
    sessions: number
}

/* SESSIONS */

export function transformSessionsDataSeriesToPosthogEvents(series: SessionsDataSeries[]): PosthogEvent[] {
    const events: PosthogEvent[] = []
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
    const events = transformSessionsDataSeriesToPosthogEvents(dataSeries)
    await posthogBatchCapture(events)
}

async function posthogBatchCapture(batch: PosthogEvent[]) {
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
