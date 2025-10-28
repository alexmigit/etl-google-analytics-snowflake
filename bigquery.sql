-------------------------------------------------------------------
-- Google BigQuery Sample Queries
------------------------------------------------------------------- 
--GA_RAW.USER_RAW
SELECT
 *
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` 
;

--GA_RAW.EVENT_RAW
SELECT
  *
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018`
;

--GA_STAGING.STG_USER
SELECT
  e.`user_id`,
  pu.`pseudo_user_id`,
  pu.`stream_id`,
  pu.`geo`.`city`,
  pu.`geo`.`continent`,
  pu.`geo`.`country`,
  pu.`geo`.`region`,
  pu.`last_updated_date`,
  pu.`occurrence_date`,
  e.`is_active_user`,
  e.`user_first_touch_timestamp`,
  e.`user_ltv`.`currency`,
  e.`user_ltv`.`revenue`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

--GA_STAGING.STG_EVENT
SELECT
  `event_bundle_sequence_id`,
  `event_date`,
  `event_dimensions`.`hostname`,
  `event_name`,
  `event_previous_timestamp`,
  `event_server_timestamp_offset`,
  `event_timestamp`,
  `event_value_in_usd`,
  `user_id`,
  `user_pseudo_id`
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018` ;

--GA_STAGING.STG_GEO
SELECT
  pu.`pseudo_user_id`,
  pu.`stream_id`,
  pu.`geo`.`city`,
  pu.`geo`.`continent`,
  pu.`geo`.`country`,
  pu.`geo`.`region`,
  e.`geo`.`metro`,
  e.`geo`.`sub_continent`,
  pu.`last_updated_date`,
  pu.`occurrence_date`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

--GA_STAGING.STG_DEVICE
SELECT
  pu.`device`.`category`,
  pu.`device`.`mobile_brand_name`,
  pu.`device`.`mobile_model_name`,
  pu.`device`.`operating_system`,
  pu.`device`.`unified_screen_name`,
  e.`device`.`advertising_id`,
  --e.`device`.`category`,
  e.`device`.`language`,
  --e.`device`.`mobile_brand_name`,
  e.`device`.`mobile_marketing_name`,
  --e.`device`.`mobile_model_name`,
  e.`device`.`mobile_os_hardware_model`,
  --e.`device`.`operating_system`,
  e.`device`.`operating_system_version`,
  e.`device`.`vendor_id`,
  --e.`device`.`browser`,
  --e.`device`.`browser_version`,
  e.`device`.`is_limited_ad_tracking`,
  e.`device`.`time_zone_offset_seconds`,
  e.`device`.`web_info`.`browser`,
  e.`device`.`web_info`.`browser_version`,
  e.`device`.`web_info`.`hostname`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

--GA_STAGING.STG_TRAFFICSOURCE
SELECT
  `platform`,
  `stream_id`,
  `traffic_source`.`medium`,
  `traffic_source`.`name`,
  `traffic_source`.`source`,
  `collected_traffic_source`.`gclid`,
  `collected_traffic_source`.`manual_campaign_id`,
  `collected_traffic_source`.`manual_campaign_name`,
  `collected_traffic_source`.`manual_content`,
  `collected_traffic_source`.`manual_creative_format`,
  `collected_traffic_source`.`manual_marketing_tactic`,
  `collected_traffic_source`.`manual_medium`,
  `collected_traffic_source`.`manual_source`,
  `collected_traffic_source`.`manual_source_platform`,
  `collected_traffic_source`.`manual_term`,
  `session_traffic_source_last_click`.`manual_campaign`.`campaign_id`,
  `session_traffic_source_last_click`.`manual_campaign`.`campaign_name`,
  `session_traffic_source_last_click`.`manual_campaign`.`content`,
  `session_traffic_source_last_click`.`manual_campaign`.`creative_format`,
  `session_traffic_source_last_click`.`manual_campaign`.`marketing_tactic`,
  `session_traffic_source_last_click`.`manual_campaign`.`medium`,
  `session_traffic_source_last_click`.`manual_campaign`.`source`,
  `session_traffic_source_last_click`.`manual_campaign`.`source_platform`,
  `session_traffic_source_last_click`.`manual_campaign`.`term`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`account_name`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`ad_group_id`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`ad_group_name`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`campaign_id`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`campaign_name`,
  `session_traffic_source_last_click`.`google_ads_campaign`.`customer_id`
  `session_traffic_source_last_click`.`cross_channel_campaign`.`campaign_id`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`campaign_name`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`default_channel_group`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`medium`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`primary_channel_group`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`source`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`source_platform`
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018` ;

--MART.DIM_GEO
--MART.DIM_DEVICE
--MART.DIM_USER
--MART.DIM_EVENT
--MART.DIM_TRAFFICSOURCE
--MART.FCT_EVENTS

--ANALYTICS.
