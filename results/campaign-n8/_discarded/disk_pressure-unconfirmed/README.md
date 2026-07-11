disk_pressure_broker_runcampaign6.json: `drop_confirmed_utc: null`. The disk-usage drop
was never confirmed within the injection script's polling window, even though the fault
was injected and the pod recovered afterward (`recovered: true`) -- a known timing-
sensitivity pattern in this fault class going back to Weeks 2-3 development
(node-exporter's scrape-interval timing relative to the fill/detect window), not a new
bug. Kept as evidence, excluded from the active dataset. Replaced by
`disk_pressure_broker_runtopup1.json` in the parent `disk_pressure/` directory
(`drop_confirmed_utc` present, confirmed).
