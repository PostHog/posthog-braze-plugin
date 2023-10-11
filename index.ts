import { Plugin, PluginEvent, PluginMeta, Properties, RetryError } from '@posthog/plugin-scaffold'
import crypto from 'crypto'
import fetch, { RequestInit, Response } from 'node-fetch'


export type FetchBraze = (
    endpoint: string,
    options: Partial<RequestInit>,
    method: string,
    requestId?: string
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
    global.fetchBraze = async (endpoint, options = {}, method = 'GET', requestId = '') => {
        const headers = {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            Authorization: `Bearer ${config.apiKey}`,
        }

        let response: Response | undefined

        const startTime = Date.now()

        try {
            response = await fetch(`${brazeUrl}${endpoint}`, {
                method,
                headers,
                ...options,
                timeout: 5000,
            })
        } catch (e) {
            console.error(e, endpoint, options.body, requestId)
            throw new RetryError('Fetch failed, retrying.')
        } finally {
            const elapsedTime = (Date.now() - startTime) / 1000
            if (elapsedTime >= 5) {
                console.warn(
                    `🐢 Slow request warning. Fetch took ${elapsedTime} seconds. Request ID: ${requestId}`,
                    endpoint
                )
            }
        }

        if (String(response.status)[0] === '5') {
            throw new RetryError(`Service is down, retry later. Request ID: ${requestId}`)
        }

        let responseJson: Record<string, unknown> | null = null

        try {
            responseJson = await response.json()
        } catch (e) {
            console.error('Error parsing Braze response as JSON: ', e, endpoint, options.body, requestId)
        }

        if (responseJson?.['errors']) {
            console.error('Braze API error (not retried): ', responseJson, endpoint, options.body, requestId)
        }
        return responseJson
    }
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

type BrazeUsersTrackBody = {
    attributes: Array<BrazeAttribute> // NOTE: max length 75
    events: Array<BrazeEvent> // NOTE: max length 75
}

const _generateBrazeRequestBody = (pluginEvent: PluginEvent, meta: BrazeMeta): BrazeUsersTrackBody => {
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

    return {
        attributes,
        events,
    }
}

export const exportEvents = async (pluginEvents: PluginEvent[], meta: BrazeMeta): Promise<void> => {
    if (!pluginEvents.length) {
        console.warn('Received `exportEvents` with no events.')
        return
    }

    // NOTE: We compute a unique ID for this request so we can identify the same request in the logs
    const requestId = crypto.createHash('sha256').update(JSON.stringify(pluginEvents)).digest('hex')
    const startTime = Date.now()

    const brazeRequestBodies = pluginEvents.map((pluginEvent) => _generateBrazeRequestBody(pluginEvent, meta))

    if (
        brazeRequestBodies.length === 0 ||
        brazeRequestBodies.every((body) => body.attributes.length === 0 && body.events.length === 0)
    ) {
        return console.log('No events to export.', requestId)
    }

    const batchSize = 75 // NOTE: https://www.braze.com/docs/api/endpoints/user_data/post_user_track/
    const batchedBodies = brazeRequestBodies.reduce((acc, curr) => {
        const { attributes, events } = curr
        const lastBatch = acc[acc.length - 1]

        if (attributes.length === 0 && events.length === 0) {
            return acc
        }

        if (!lastBatch || lastBatch.attributes.length >= batchSize || lastBatch.events.length >= batchSize) {
            acc.push({ attributes: [...attributes], events: [...events] })
        } else {
            lastBatch.attributes.push(...attributes)
            lastBatch.events.push(...events)
        }

        return acc
    }, [] as BrazeUsersTrackBody[])

    const brazeRequests = batchedBodies.map((body, idx) =>
        meta.global.fetchBraze(
            '/users/track',
            {
                body: JSON.stringify(body),
            },
            'POST',
            `${requestId}-${idx}`
        )
    )

    // NOTE: Send all requests in parallel, error responses already handled and logged by fetchBraze
    await Promise.all(brazeRequests)

    const elapsedTime = (Date.now() - startTime) / 1000

    if (elapsedTime >= 30) {
        console.warn(`🐢🐢 Slow exportEvents warning. Export took ${elapsedTime} seconds.`, requestId)
    }
}
