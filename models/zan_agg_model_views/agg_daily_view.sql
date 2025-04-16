{{ config(
    materialized="view",
) }}

with evc as (

SELECT
                    TO_DATE(timestamp_pst) date
                    ,account_id
                    ,campaign_id
                    ,placement_id
                    ,publisher_id
                    ,SUM(CASE WHEN event_type = 'campaign_clicked' AND (do_not_track != TRUE OR do_not_track IS NULL) THEN 1 ELSE 0 END) clicks
                    ,SUM(CASE WHEN event_type = 'campaign_impressed' AND (do_not_track != TRUE OR do_not_track IS NULL) THEN 1 ELSE 0 END) impressions
                    ,SUM(CASE WHEN event_type = 'campaign_recommended' AND (do_not_track != TRUE OR do_not_track IS NULL) THEN 1 ELSE 0 END) recommendations
                    ,SUM(CASE WHEN event_type = 'campaign_converted' THEN 1 ELSE 0 END) conversions
                    ,SUM(CASE WHEN event_type = 'campaign_converted' THEN revenue ELSE 0 END) advertiser_cost
                    ,SUM(CASE WHEN event_type = 'campaign_converted' THEN revenue ELSE 0 END) - SUM(CASE WHEN event_type = 'campaign_converted' THEN revenue * rev_share/1e6 ELSE 0 END) zeeto_revenue
                    ,SUM(CASE WHEN event_type = 'lead_accepted' THEN 1 ELSE 0 END) leads_accepted
                    ,SUM(CASE WHEN event_type = 'lead_rejected' THEN 1 ELSE 0 END) leads_rejected
                    ,(SUM(CASE WHEN event_type = 'lead_accepted' THEN 1 ELSE 0 END) + SUM(CASE WHEN event_type = 'lead_rejected' THEN 1 ELSE 0 END) + SUM(CASE WHEN EVENT_TYPE = 'lead_failed' THEN 1 ELSE 0 END)) delivery_count
            from {{ source("zan_zevent", "event_visit_campaign") }}
  WHERE
         date IS NOT NULL
        AND date < CURRENT_DATE
  GROUP BY ALL

)
select * from evc