import {
    BrazeObject,
    CampaignDataSeries,
    CanvasDataSeries,
    FetchBraze,
    isBrazeObjectActive,
    ISODateString,
    paginateItems,
    transformActiveUsersDataSeriesToPostHogEvents,
    transformCampaignDataSeriesToPostHogEvents,
    transformCanvasDataSeriesToPostHogEvents,
    transformCustomEventDataSeriesToPostHogEvents,
    transformDailyUninstallsDataSeriesToPostHogEvents,
    transformFeedDataSeriesToPostHogEvents,
    transformMonthlyActiveUsersDataSeriesToPostHogEvents,
    transformNewUsersDataSeriesToPostHogEvents,
    transformSegmentDataSeriesToPostHogEvents,
    transformSessionsDataSeriesToPostHogEvents,
} from '../index'

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const dummyFetch: (x: Record<string, unknown>) => FetchBraze = (x) => (_a, _b, _c) => Promise.resolve(x)

test('ISODateString', () => {
    expect(ISODateString(new Date(1648458820359))).toEqual('2022-03-28T09:13:40.359Z')
})

test('paginateIds', async () => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const dummyCallback = (_a: unknown, _b: unknown, _c: unknown) => {
        let ids: string[] = []
        let index = 0
        while (index < 2) {
            if (index === 0) {
                ids = ids.concat(Array(100))
            } else {
                ids = ids.concat(Array(50))
            }
            index += 1
        }
        return Promise.resolve(
            ids.map((id) => {
                return { id, name: id }
            })
        )
    }
    const ids = await paginateItems(BrazeObject.campaigns, 100, dummyCallback, dummyFetch({}))
    expect(ids).toHaveLength(150)
})

test('isBrazeObjectActive', async () => {
    // successful
    expect(
        await isBrazeObjectActive(
            BrazeObject.campaigns,
            'campaign_id',
            'foobar',
            dummyFetch({
                draft: false,
                last_sent: ISODateString(new Date()),
            })
        )
    ).toEqual(true)

    // draft object
    expect(
        await isBrazeObjectActive(
            BrazeObject.campaigns,
            'campaign_id',
            'foobar',
            dummyFetch({
                draft: true,
                last_sent: ISODateString(new Date()),
            })
        )
    ).toEqual(false)

    // last_sent two days ago
    expect(
        await isBrazeObjectActive(
            BrazeObject.campaigns,
            'campaign_id',
            'foobar',
            dummyFetch({
                draft: false,
                last_sent: ISODateString(new Date(new Date().getTime() - 1000 * 60 * 60 * 48)),
            })
        )
    ).toEqual(false)

    // last_entry two days ago
    expect(
        await isBrazeObjectActive(
            BrazeObject.campaigns,
            'campaign_id',
            'foobar',
            dummyFetch({
                draft: false,
                last_entry: ISODateString(new Date(new Date().getTime() - 1000 * 60 * 60 * 48)),
            })
        )
    ).toEqual(false)

    // end_at two days ago
    expect(
        await isBrazeObjectActive(
            BrazeObject.campaigns,
            'campaign_id',
            'foobar',
            dummyFetch({
                draft: false,
                end_at: ISODateString(new Date(new Date().getTime() - 1000 * 60 * 60 * 48)),
            })
        )
    ).toEqual(false)
})

test('transformCampaignDataSeriesToPostHogEvents', () => {
    const series: CampaignDataSeries[] = [
        {
            time: '2022-03-28T09:13:40.359Z',
            messages: {
                ios_push: [
                    {
                        variation_name: 'iOS_Push',
                        sent: 1,
                        direct_opens: 1,
                        total_opens: 1,
                        bounces: 1,
                        body_clicks: 1,
                        revenue: 1,
                        unique_recipients: 1,
                        conversions: 1,
                        conversions_by_send_time: 1,
                        conversions1: 1,
                        conversions1_by_send_time: 1,
                        conversions2: 1,
                        conversions2_by_send_time: 1,
                        conversions3: 1,
                        conversions3_by_send_time: 1,
                        'carousel_slide_[NUM]_[TITLE]_click': 1,
                        'notif_button_[NUM]_[TITLE]_click': 1,
                    },
                ],
                android_push: [
                    {
                        sent: 1,
                        direct_opens: 1,
                        total_opens: 1,
                        bounces: 1,
                        body_clicks: 1,
                    },
                ],
                webhook: [
                    {
                        sent: 1,
                        errors: 1,
                    },
                ],
                email: [
                    {
                        sent: 1,
                        opens: 1,
                        unique_opens: 1,
                        clicks: 1,
                        unique_clicks: 1,
                        unsubscribes: 1,
                        bounces: 1,
                        delivered: 1,
                        reported_spam: 1,
                    },
                ],
                sms: [
                    {
                        sent: 1,
                        sent_to_carrier: 1,
                        delivered: 1,
                        rejected: 1,
                        delivery_failed: 1,
                        opt_out: 1,
                        help: 1,
                    },
                ],
                content_cards: [
                    {
                        variation_name: 'Variant 1',
                        variation_api_id: 'foobar',
                        sent: 1,
                        total_impressions: 1,
                        unique_impressions: 1,
                        total_clicks: 1,
                        unique_clicks: 1,
                        total_dismissals: 1,
                        unique_dismissals: 1,
                        revenue: 1,
                        unique_recipients: 1,
                        conversions: 1,
                        conversions_by_send_time: 1,
                        conversions1: 1,
                        conversions1_by_send_time: 1,
                        conversions2: 1,
                        conversions2_by_send_time: 1,
                        conversions3: 1,
                        conversions3_by_send_time: 1,
                    },
                ],
            },
            conversions_by_send_time: 1,
            conversions1_by_send_time: 1,
            conversions2_by_send_time: 1,
            conversions3_by_send_time: 1,
            conversions: 1,
            conversions1: 1,
            conversions2: 1,
            conversions3: 1,
            unique_recipients: 1,
            revenue: 1,
        },
    ]

    expect(transformCampaignDataSeriesToPostHogEvents(series, 'campaign1')).toEqual([
        {
            event: 'Braze campaign: campaign1',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                conversions_by_send_time: 1,
                conversions1_by_send_time: 1,
                conversions2_by_send_time: 1,
                conversions3_by_send_time: 1,
                conversions: 1,
                conversions1: 1,
                conversions2: 1,
                conversions3: 1,
                unique_recipients: 1,
                revenue: 1,
                'ios_push:iOS_Push:sent': 1,
                'ios_push:iOS_Push:direct_opens': 1,
                'ios_push:iOS_Push:total_opens': 1,
                'ios_push:iOS_Push:bounces': 1,
                'ios_push:iOS_Push:body_clicks': 1,
                'ios_push:iOS_Push:revenue': 1,
                'ios_push:iOS_Push:unique_recipients': 1,
                'ios_push:iOS_Push:conversions': 1,
                'ios_push:iOS_Push:conversions_by_send_time': 1,
                'ios_push:iOS_Push:conversions1': 1,
                'ios_push:iOS_Push:conversions1_by_send_time': 1,
                'ios_push:iOS_Push:conversions2': 1,
                'ios_push:iOS_Push:conversions2_by_send_time': 1,
                'ios_push:iOS_Push:conversions3': 1,
                'ios_push:iOS_Push:conversions3_by_send_time': 1,
                'ios_push:iOS_Push:carousel_slide_[NUM]_[TITLE]_click': 1,
                'ios_push:iOS_Push:notif_button_[NUM]_[TITLE]_click': 1,
                'android_push:sent': 1,
                'android_push:direct_opens': 1,
                'android_push:total_opens': 1,
                'android_push:bounces': 1,
                'android_push:body_clicks': 1,
                'webhook:sent': 1,
                'webhook:errors': 1,
                'email:sent': 1,
                'email:opens': 1,
                'email:unique_opens': 1,
                'email:clicks': 1,
                'email:unique_clicks': 1,
                'email:unsubscribes': 1,
                'email:bounces': 1,
                'email:delivered': 1,
                'email:reported_spam': 1,
                'sms:sent': 1,
                'sms:sent_to_carrier': 1,
                'sms:delivered': 1,
                'sms:rejected': 1,
                'sms:delivery_failed': 1,
                'sms:opt_out': 1,
                'sms:help': 1,
                'content_cards:Variant 1:variation_api_id': 'foobar',
                'content_cards:Variant 1:sent': 1,
                'content_cards:Variant 1:total_impressions': 1,
                'content_cards:Variant 1:unique_impressions': 1,
                'content_cards:Variant 1:total_clicks': 1,
                'content_cards:Variant 1:unique_clicks': 1,
                'content_cards:Variant 1:total_dismissals': 1,
                'content_cards:Variant 1:unique_dismissals': 1,
                'content_cards:Variant 1:revenue': 1,
                'content_cards:Variant 1:unique_recipients': 1,
                'content_cards:Variant 1:conversions': 1,
                'content_cards:Variant 1:conversions_by_send_time': 1,
                'content_cards:Variant 1:conversions1': 1,
                'content_cards:Variant 1:conversions1_by_send_time': 1,
                'content_cards:Variant 1:conversions2': 1,
                'content_cards:Variant 1:conversions2_by_send_time': 1,
                'content_cards:Variant 1:conversions3': 1,
                'content_cards:Variant 1:conversions3_by_send_time': 1,
            },
        },
    ])
})

test('transformCanvasDataSeriesToPostHogEvents', () => {
    const series: CanvasDataSeries = {
        name: 'canvas1',
        stats: [
            {
                time: '2022-03-28T09:13:40.359Z',
                total_stats: {
                    revenue: 1,
                    conversions: 1,
                    conversions_by_entry_time: 1,
                    entries: 1,
                },
                variant_stats: {
                    '00000000-0000-0000-0000-0000000000000': {
                        name: 'variant1',
                        revenue: 1,
                        conversions: 1,
                        conversions_by_entry_time: 1,
                        entries: 1,
                    },
                },
                step_stats: {
                    '00000000-0000-0000-0000-0000000000000': {
                        name: 'step1',
                        revenue: 1,
                        conversions: 1,
                        conversions_by_entry_time: 1,
                        messages: {
                            email: [
                                {
                                    sent: 1,
                                    opens: 1,
                                    unique_opens: 1,
                                    clicks: 1,
                                },
                            ],
                        },
                    },
                },
            },
        ],
    }

    expect(transformCanvasDataSeriesToPostHogEvents(series, 'canvas1')).toEqual([
        {
            event: 'Braze canvas: canvas1',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                'total_stats:revenue': 1,
                'total_stats:conversions': 1,
                'total_stats:conversions_by_entry_time': 1,
                'total_stats:entries': 1,
                'variant_stats:variant1:revenue': 1,
                'variant_stats:variant1:conversions': 1,
                'variant_stats:variant1:conversions_by_entry_time': 1,
                'variant_stats:variant1:entries': 1,
                'step_stats:step1:revenue': 1,
                'step_stats:step1:conversions': 1,
                'step_stats:step1:conversions_by_entry_time': 1,
                'step_stats:step1:email:sent': 1,
                'step_stats:step1:email:opens': 1,
                'step_stats:step1:email:unique_opens': 1,
                'step_stats:step1:email:clicks': 1,
            },
        },
    ])
})

test('transformCustomEventDataSeriesToPostHogEvent', () => {
    const event = 'event1'
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            count: 1,
        },
    ]

    expect(transformCustomEventDataSeriesToPostHogEvents(series, event)).toEqual([
        {
            event: 'Braze event: event1',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformNewUsersDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            new_users: 1,
        },
    ]

    expect(transformNewUsersDataSeriesToPostHogEvents(series)).toEqual([
        {
            event: 'Braze KPI: Daily New Users',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformActiveUsersDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            dau: 1,
        },
    ]

    expect(transformActiveUsersDataSeriesToPostHogEvents(series)).toEqual([
        {
            event: 'Braze KPI: Daily Active Users',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformMonthlyActiveUsersDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            mau: 1,
        },
    ]

    expect(transformMonthlyActiveUsersDataSeriesToPostHogEvents(series)).toEqual([
        {
            event: 'Braze KPI: Monthly Active Users',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformMonthlyActiveUsersDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            uninstalls: 1,
        },
    ]

    expect(transformDailyUninstallsDataSeriesToPostHogEvents(series)).toEqual([
        {
            event: 'Braze KPI: Daily Uninstalls',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformFeedDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            clicks: 1,
            impressions: 1,
            unique_clicks: 1,
            unique_impressions: 1,
        },
    ]

    expect(transformFeedDataSeriesToPostHogEvents(series, 'feed1')).toEqual([
        {
            event: 'Braze News Feed Card: feed1',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                clicks: 1,
                impressions: 1,
                unique_clicks: 1,
                unique_impressions: 1,
            },
        },
    ])
})

test('transformSegmentDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            size: 1,
        },
    ]

    expect(transformSegmentDataSeriesToPostHogEvents(series, 'segment1')).toEqual([
        {
            event: 'Braze Segment: segment1',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})

test('transformSegmentDataSeriesToPostHogEvents', () => {
    const series = [
        {
            time: '2022-03-28T09:13:40.359Z',
            sessions: 1,
        },
    ]

    expect(transformSessionsDataSeriesToPostHogEvents(series)).toEqual([
        {
            event: 'Braze Sessions',
            timestamp: '2022-03-28T09:13:40.359Z',
            properties: {
                count: 1,
            },
        },
    ])
})
