---
title: Recommended practices
---


# Do not reuse S4 events

**S4 events are immutable, however immutability is not currently enforced.**
Make sure you do not reuse incoming events and for instance simply update a field. Instead, create a new event (you may extend the `Event` class and defined a copy constructor) with the new field value.

More information available in this [ticket](https://issues.apache.org/jira/browse/S4-104)