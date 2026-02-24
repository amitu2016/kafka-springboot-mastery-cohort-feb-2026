## Kafka Connect – Top 100 Interview Questions with Indicative Answers

Below is a **comprehensive and structured list of 100 Kafka Connect interview questions** covering fundamentals, architecture, operations, CDC, failure handling, and real-world design scenarios.

The answers are intentionally **concise (1–2 lines each)** to help you:

* Revise quickly before interviews
* Structure strong, crisp responses
* Expand verbally during discussion

This covers practical knowledge around **Apache Kafka**, **Kafka Connect**, and CDC tools like **Debezium**.

---

# 1. Core Concepts

1. **What is Kafka Connect and why was it introduced?**
   Kafka Connect is a framework to reliably stream data between Kafka and external systems without writing custom integration code.

2. **How is Kafka Connect different from writing a custom Kafka producer/consumer?**
   It handles scaling, fault tolerance, offset management, and retries automatically.

3. **What are source connectors and sink connectors?**
   Source connectors ingest data into Kafka; sink connectors export Kafka data to external systems.

4. **What is a connector, task, and worker?**
   A connector defines integration logic, tasks perform parallel work, and workers run connectors/tasks in a JVM process.

5. **Why does Kafka Connect run as a separate service?**
   To isolate integration workloads from brokers and allow independent scaling.

6. **What delivery guarantees does Kafka Connect provide?**
   By default, it provides at-least-once delivery.

7. **What is a Single Message Transform (SMT)?**
   A lightweight per-record transformation applied during ingestion or egress.

8. **When should you not use SMTs?**
   For complex stateful transformations; use Kafka Streams instead.

9. **What serialization formats are supported?**
   JSON, Avro, Protobuf, String, ByteArray via converters.

10. **How does Kafka Connect manage offsets?**
    Offsets are stored in an internal Kafka topic for durability and recovery.

---

# 2. Architecture & Internal Working

11. **Explain Kafka Connect distributed architecture.**
    Multiple workers coordinate using internal Kafka topics for configs, offsets, and status.

12. **What are internal Kafka Connect topics?**
    Config, offset, and status topics store metadata and processing state.

13. **Difference between standalone and distributed mode?**
    Standalone runs in one process; distributed supports clustering and automatic rebalancing.

14. **How does task rebalancing work?**
    When workers join/leave, tasks are redistributed automatically across workers.

15. **What happens when a worker crashes?**
    Tasks are reassigned to remaining workers and resume from last committed offsets.

16. **Can multiple connectors run on one worker?**
    Yes, a worker can execute multiple connectors and their tasks.

17. **How does Kafka Connect achieve fault tolerance?**
    State is stored in Kafka topics, enabling recovery after failures.

18. **Why does Kafka Connect need Kafka even for source connectors?**
    It uses Kafka to persist offsets and configuration state.

19. **What is tasks.max?**
    It defines the maximum number of parallel tasks for a connector.

20. **Does increasing tasks always increase throughput?**
    Only if the source/sink and partitions allow parallelism.

---

# 3. Scaling & Performance

21. **How does Kafka Connect scale horizontally?**
    By adding more workers in distributed mode.

22. **Increasing workers vs tasks — what’s the difference?**
    Workers add JVM capacity; tasks increase connector-level parallelism.

23. **How does Kafka Connect handle backpressure?**
    Sink connectors pause consumption when downstream systems slow down.

24. **How do you monitor Kafka Connect?**
    Using JMX metrics, REST APIs, and Kafka consumer lag monitoring.

25. **How do you update a connector without downtime?**
    Use the REST API to update configuration dynamically.

26. **What happens if Kafka is temporarily unavailable?**
    Connect retries and resumes once Kafka becomes available.

27. **What happens if the sink database is down?**
    Records are retried; offsets are not committed until successful writes.

28. **How do you tune Kafka Connect for high throughput?**
    Adjust batch sizes, tasks, worker count, and producer/consumer configs.

29. **How do you debug a failing connector?**
    Check worker logs, task status via REST API, and DLQ if configured.

30. **What causes uneven task load?**
    Partition imbalance or source-side bottlenecks.

---

# 4. Data & Schema Handling

31. **What is the role of converters?**
    Converters serialize/deserialize data between Kafka bytes and Connect data format.

32. **Difference between JsonConverter and AvroConverter?**
    JsonConverter can be schemaless; AvroConverter enforces schema with registry.

33. **Can Kafka Connect work without Schema Registry?**
    Yes, but schema evolution and compatibility checks are limited.

34. **How does schema evolution work?**
    New schemas are registered and validated against compatibility rules.

35. **What happens if schema is incompatible?**
    Producer or connector fails depending on compatibility settings.

36. **How are deletes represented in Kafka Connect?**
    Typically as tombstone records with null values.

37. **What are tombstone records used for?**
    They signal deletions, especially in compacted topics.

38. **What is schemaless JSON?**
    JSON without enforced schema, leading to weaker compatibility guarantees.

39. **How does Connect handle null fields?**
    Optional schema fields allow null values.

40. **What is a dead letter queue (DLQ)?**
    A separate Kafka topic where failed records are sent.

---

# 5. Reliability & Delivery Semantics

41. **Does Kafka Connect support exactly-once semantics?**
    Sink connectors can support EOS if underlying Kafka and sink are idempotent.

42. **How does Kafka Connect avoid data loss?**
    Offsets are committed only after successful processing.

43. **Can Kafka Connect produce duplicates?**
    Yes, at-least-once semantics can cause duplicates after retries.

44. **How do you ensure idempotency in sinks?**
    Use primary keys or upsert semantics in databases.

45. **What is errors.tolerance?**
    It defines whether connector fails or skips problematic records.

46. **When would you enable DLQ?**
    When you want pipeline continuity despite bad records.

47. **What happens during task failure?**
    The task is restarted automatically.

48. **How are retries handled?**
    Sink connectors retry based on configured retry policies.

49. **What happens if offset topic is corrupted?**
    Connector may reprocess data from earlier offsets.

50. **How does Connect recover after restart?**
    It reloads configs and resumes from stored offsets.

---

# 6. CDC & Event Streaming

51. **What is Change Data Capture (CDC)?**
    CDC captures database changes and streams them as events.

52. **How does Debezium work with Kafka Connect?**
    Debezium runs as a source connector reading database logs and emitting change events.

53. **What database logs does CDC read?**
    Binlog (MySQL), WAL (Postgres), etc.

54. **How are updates represented in CDC?**
    As before-and-after structured change events.

55. **How are deletes handled in CDC?**
    As delete events followed by optional tombstones.

56. **Why is CDC preferred over polling?**
    It provides near real-time, low-overhead change streaming.

57. **Can CDC impact source DB performance?**
    Yes, heavy log reading can increase DB load.

58. **How do you scale CDC connectors?**
    By increasing tasks if DB supports partitioned log reading.

59. **What are common CDC production issues?**
    Schema changes, large transactions, and log retention limits.

60. **How do you replay CDC data?**
    Reset offsets and reprocess from earlier positions.

---

# 7. Real-Life Design & Use Cases

61. **When would you use Kafka Connect over Kafka Streams?**
    For data integration, not business logic processing.

62. **When should you not use Kafka Connect?**
    For complex transformations or real-time enrichment.

63. **Give a real use case of source connectors.**
    Streaming MySQL changes into Kafka using CDC.

64. **Give a real use case of sink connectors.**
    Sending Kafka events to Elasticsearch or S3.

65. **How would you build MySQL → Kafka → Elasticsearch?**
    Use Debezium source connector and Elasticsearch sink connector.

66. **How would you replicate Kafka to S3?**
    Use an S3 sink connector for batch object storage.

67. **How would you migrate from batch ETL to streaming?**
    Replace periodic jobs with source connectors streaming continuously.

68. **How do you handle GDPR deletes?**
    Propagate delete events and enable log compaction.

69. **How do you replay historical data?**
    Reset consumer offsets or reconfigure connector offsets.

70. **How would you design a high-throughput Connect cluster?**
    Use distributed mode, multiple workers, optimized batching, and partition alignment.

---

# 8. Advanced & Design Trade-Offs

71. **Can Connect replace ETL tools?**
    For simple pipelines yes; for heavy transformations no.

72. **Is Kafka Connect low latency?**
    It is near real-time but optimized for throughput.

73. **How does Connect differ from NiFi?**
    Connect is Kafka-native; NiFi is broader flow-based processing.

74. **What is connector isolation?**
    Running connectors in isolated classloaders to avoid dependency conflicts.

75. **What happens if tasks exceed partition count?**
    Extra tasks remain idle.

76. **How do you prevent connector overload?**
    Limit tasks and tune batching configs.

77. **How do you migrate connectors between clusters?**
    Export configs and redeploy with preserved internal topics.

78. **How do you ensure ordering in sink connectors?**
    Align tasks with partition ordering.

79. **How does Connect handle large messages?**
    Limited by Kafka max message size configuration.

80. **What is incremental snapshot in CDC?**
    A phased initial data load without locking large tables.

---

# 9. Deep Operational Scenarios

81. **A worker crashes repeatedly — what do you check?**
    Logs, memory usage, connector misconfiguration.

82. **Connector stuck in FAILED state — what next?**
    Check task trace via REST and restart or fix config.

83. **Offsets not advancing — why?**
    Sink failures or downstream blocking.

84. **High consumer lag — possible causes?**
    Slow sink, insufficient tasks, or partition skew.

85. **Schema registry unavailable — impact?**
    Serialization/deserialization failures.

86. **DB restarted during CDC — impact?**
    Connector reconnects and resumes from last log position.

87. **Internal topics deleted — impact?**
    Connector loses state and may reprocess data.

88. **Kafka retention too low — risk?**
    Offsets may point to expired data.

89. **How do you achieve zero downtime upgrades?**
    Rolling restart of workers.

90. **How do you test connectors before production?**
    Run in staging with controlled topics and validation.

---

# 10. Expert-Level Questions

91. **How does Connect handle partition assignment internally?**
    Using Kafka consumer group coordination protocol.

92. **What is cooperative rebalancing?**
    A strategy minimizing task movement during rebalance.

93. **Can connectors be stateful?**
    Tasks are generally stateless; state must be externalized.

94. **How does Connect manage memory?**
    Through JVM heap; large batches can cause OOM.

95. **How do you isolate heavy connectors?**
    Deploy separate Connect clusters.

96. **Can you run multiple Connect clusters against same Kafka?**
    Yes, with different group IDs and internal topics.

97. **How does Connect integrate with Kubernetes?**
    Workers run as pods with REST-based management.

98. **How would you design multi-region replication?**
    Use MirrorMaker plus region-specific connectors.

99. **What limits maximum throughput?**
    Partitions, sink capacity, network, and batch configs.

100. **What are the biggest production mistakes with Kafka Connect?**
     Ignoring schema evolution, poor partitioning, and insufficient monitoring.

---

## Conclusion

If you can confidently answer these 100 questions with:

* Trade-offs
* Failure handling clarity
* Real production examples

You’re operating at **strong senior / staff-level understanding** of Kafka Connect within the **Apache Kafka ecosystem**.

If you want next, I can:

* Convert this into a **mock senior interview simulation**
* Or create a **production-ready architecture deep dive story**
* Or generate **real failure case narratives** you can use in interviews