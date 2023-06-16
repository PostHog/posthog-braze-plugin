// This App supports pushing events to Braze also, via the `onEvent` hook. It
// should send any $set attributes to Braze `/users/track` endpoint in the
// `attributes` param as well as events in the `events` property.
//
// For an $identify event with $set properties the PostHog PluginEvent json
// looks like:
//
// {
//   "event": "$identify",
//   "properties": {
//     "$set": {
//       "email": "test@posthog",
//       "name": "Test User"
//     }
//   }
// }
//
// The Braze `/users/track` endpoint expects a json payload like:
//
// {
//   "attributes": {
//     "email": "test@posthog",
//     "name": "Test User"
//   },
//   "events": []
// }
//
// For an $capture event with properties the PostHog PluginEvent json looks
// like:
//
// {
//   "event": "test event",
//   "properties": {
//     "test property": "test value"
//   }
// }
//
// The Braze `/users/track` endpoint expects a json payload like:
//
// {
//   "attributes": {},
//   "events": [
//     {
//       "name": "test event",
//       "properties": {
//         "test property": "test value"
//       }
//     }
//   ]
// }
//

import { rest } from 'msw'
import { setupServer } from 'msw/node'

import { onEvent, setupPlugin } from './index'

const server = setupServer()

beforeAll(() => server.listen())
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

test('onEvent sends $set attributes and events to Braze', async () => {
    const mockService = jest.fn()

    server.use(
        rest.post('https://rest.iad-01.braze.com/users/track', (req, res, ctx) => {
            const requestBody = req.body as any
            mockService(requestBody)
            return res(ctx.status(200), ctx.json({}))
        })
    )

    // Create a meta object that we can pass into the setupPlugin and onEvent
    const meta = {
        config: {
            brazeUrl: 'https://rest.iad-01.braze.com',
            exportEvents: '$identify',
            exportUserAttributes: true,
        },
        global: {},
    }

    await setupPlugin(meta as any)
    await onEvent(
        {
            event: '$identify',
            properties: {
                $set: {
                    email: 'test@posthog',
                    name: 'Test User',
                },
            },
            distinct_id: 'test',
            ip: '',
            site_url: '',
            team_id: 0,
            now: new Date().toISOString(),
        },
        meta as any
    )

    expect(mockService).toHaveBeenCalledWith({
        attributes: [
            {
                email: 'test@posthog',
                name: 'Test User',
            },
        ],
        events: [
            {
                $set: {
                    email: 'test@posthog',
                    name: 'Test User',
                },
                external_id: 'test',
                name: '$identify',
                time: '2023-06-16T00:00:00.00Z',
            },
        ],
    })
})
