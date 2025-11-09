from utils.logging_facility import log
import pandas as pd

def transform_ga4_data(raw_data):
    log.info("Starting GA4 data transformation...")

    pu = raw_data["USER_RAW"]
    e = raw_data["EVENT_RAW"]

    # --- STG_USER ---
    stg_user = (
        e.merge(pu, left_on="user_pseudo_id", right_on="pseudo_user_id", how="inner")
        [[
            "user_id",
            "pseudo_user_id",
            "stream_id",
            "geo.city",
            "geo.continent",
            "geo.country",
            "geo.region",
            "last_updated_date",
            "occurrence_date",
            "is_active_user",
            "user_first_touch_timestamp",
            "user_ltv.currency",
            "user_ltv.revenue"
        ]]
    )

    # --- STG_EVENT ---
    stg_event = e[[
        "event_bundle_sequence_id",
        "event_date",
        "event_dimensions.hostname",
        "event_name",
        "event_previous_timestamp",
        "event_server_timestamp_offset",
        "event_timestamp",
        "event_value_in_usd",
        "user_id",
        "user_pseudo_id"
    ]]

    # --- STG_GEO ---
    stg_geo = (
        pu.merge(e, left_on="pseudo_user_id", right_on="user_pseudo_id", how="inner")
        [[
            "pseudo_user_id",
            "stream_id",
            "geo.city",
            "geo.continent",
            "geo.country",
            "geo.region",
            "geo.metro",
            "geo.sub_continent",
            "last_updated_date",
            "occurrence_date"
        ]]
    )

    # --- STG_DEVICE ---
    stg_device = (
        pu.merge(e, left_on="pseudo_user_id", right_on="user_pseudo_id", how="inner")
        [[
            "device.category",
            "device.mobile_brand_name",
            "device.mobile_model_name",
            "device.operating_system",
            "device.unified_screen_name",
            "device.advertising_id",
            "device.language",
            "device.mobile_marketing_name",
            "device.mobile_os_hardware_model",
            "device.operating_system_version",
            "device.vendor_id",
            "device.browser",
            "device.browser_version",
            "device.is_limited_ad_tracking",
            "device.time_zone_offset_seconds",
            "device.web_info.browser",
            "device.web_info.browser_version",
            "device.web_info.hostname"
        ]]
    )

    # --- STG_TRAFFICSOURCE ---
    stg_traffic = e[[
        "platform",
        "stream_id",
        "traffic_source.medium",
        "traffic_source.name",
        "traffic_source.source",
        "collected_traffic_source.gclid",
        "collected_traffic_source.manual_campaign_id",
        "collected_traffic_source.manual_campaign_name",
        "collected_traffic_source.manual_content",
        "collected_traffic_source.manual_creative_format",
        "collected_traffic_source.manual_marketing_tactic",
        "collected_traffic_source.manual_medium",
        "collected_traffic_source.manual_source",
        "collected_traffic_source.manual_source_platform",
        "collected_traffic_source.manual_term",
        "session_traffic_source_last_click.manual_campaign.campaign_id",
        "session_traffic_source_last_click.manual_campaign.campaign_name",
        "session_traffic_source_last_click.manual_campaign.content",
        "session_traffic_source_last_click.manual_campaign.creative_format",
        "session_traffic_source_last_click.manual_campaign.marketing_tactic",
        "session_traffic_source_last_click.manual_campaign.medium",
        "session_traffic_source_last_click.manual_campaign.source",
        "session_traffic_source_last_click.manual_campaign.source_platform",
        "session_traffic_source_last_click.manual_campaign.term",
        "session_traffic_source_last_click.google_ads_campaign.account_name",
        "session_traffic_source_last_click.google_ads_campaign.ad_group_id",
        "session_traffic_source_last_click.google_ads_campaign.ad_group_name",
        "session_traffic_source_last_click.google_ads_campaign.campaign_id",
        "session_traffic_source_last_click.google_ads_campaign.campaign_name",
        "session_traffic_source_last_click.google_ads_campaign.customer_id",
        "session_traffic_source_last_click.cross_channel_campaign.campaign_id",
        "session_traffic_source_last_click.cross_channel_campaign.campaign_name",
        "session_traffic_source_last_click.cross_channel_campaign.default_channel_group",
        "session_traffic_source_last_click.cross_channel_campaign.medium",
        "session_traffic_source_last_click.cross_channel_campaign.primary_channel_group",
        "session_traffic_source_last_click.cross_channel_campaign.source",
        "session_traffic_source_last_click.cross_channel_campaign.source_platform"
    ]]

    transformed = {
        "STG_USER": stg_user,
        "STG_EVENT": stg_event,
        "STG_GEO": stg_geo,
        "STG_DEVICE": stg_device,
        "STG_TRAFFICSOURCE": stg_traffic
    }

    log.info("Transformation complete. Generated all staging tables.")
    return transformed
