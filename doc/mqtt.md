MQTT topic mapping:

plutus/ - parent topid

plutus/debug - used to capture debug information
plutus/debug/<transcoder-node-id> - transcoder-specific debug messages


plutus/orchestrator/<orchestrator-node-id> - topics for orchestrator messages
plutus/orchestrator/<orchestrator-node-id>/t_connect - capture transcoder register/unregister events
