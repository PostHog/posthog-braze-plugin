import { RetryError } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'

import { BrazeMeta, onEvent, setupPlugin } from '..'

jest.mock('node-fetch')

test('Braze network error', async () => {
    // @ts-ignore
    fetch.mockImplementationOnce(() => {
        throw new Error('Network error')
    })
    // Create a meta object that we can pass into the setupPlugin and onEvent
    const meta = {
        config: {
            brazeEndpoint: 'US-08',
            eventsToExport: 'account created',
            userPropertiesToExport: 'email',
            importUserAttributesInAllEvents: 'Yes',
        },
        global: {},
    } as BrazeMeta

    await setupPlugin(meta)

    try {
        await onEvent(
            {
                event: '$identify',
                timestamp: '2023-06-16T00:00:00.00Z',
                properties: {
                    $set: {
                        email: 'test@posthog',
                        name: 'Test User',
                    },
                    is_a_demo_user: true,
                },
                distinct_id: 'test',
                ip: '',
                site_url: '',
                team_id: 0,
                now: new Date().toISOString(),
            },
            meta
        )
    } catch (e) {
        expect(e instanceof RetryError).toBeTruthy()
        // @ts-ignore
        expect(e.message).toEqual('Fetch failed, retrying.')
    }
})
